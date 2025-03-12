module node;

import std.stdio;
import core.thread;
import std.datetime;
import std.random;
import std.container.dlist;
import std.json;

import globals;
import communication;

// Base class for nodes
class Node {
    NodeId m_id;
    ICommunicator m_communicator;

    this(NodeId id, ICommunicator communicator){ 
        this.m_id = id;
        this.m_communicator = communicator;
    }

    abstract bool handleRequest();
}

// RAFT Implementation
class RaftNode : Node {
    enum RaftState { Follower, Candidate, Leader }
    private RaftState m_state = RaftState.Follower;
    private int m_currentTerm = -1;
    private NodeId m_votedFor = INVALID_NODE_ID;
    private NodeId m_currentLeader = INVALID_NODE_ID;
    private DList!Message m_entriesQueue;
    private Duration m_electionTimeout;
    private SysTime m_lastHeartbeat;

    this(NodeId id, ICommunicator communicator) {
        super(id, communicator);
        m_entriesQueue = DList!Message();
        _resetElectionTimeout();
        m_lastHeartbeat = Clock.currTime();
    }

    void raftIteration() {
        _probeElectionTimeout();
        // Other periodic tasks
        // ...
    }

    override bool handleRequest() {
        // Handle incoming messages (AppendEntries, RequestVote, etc.)
        Message receivedMsg = {
            dstId: this.m_id
        };

        if (!m_communicator.recv(receivedMsg)) {
            return false;
        }

        switch (receivedMsg.type) {
            case Message.Type.RAFT.AppendEntries:
                // AppendEntries logic
                // ...
                _receiveHeartbeat();
                _addEntry(receivedMsg);
                break;
            case Message.Type.RAFT.AppendEntriesResponse:
                // AppendEntriesResponse logic
                // ...
                break;
            case Message.Type.RAFT.RequestVote:
                _voteRequest(receivedMsg.content["candidateId"].get!NodeId,
                     receivedMsg.content["term"].get!int, 
                     receivedMsg.content["lastLogIndex"].get!uint,
                     receivedMsg.content["lastLogTerm"].get!int);
                break;
            case Message.Type.RAFT.RequestVoteResponse:
                _voteResponse(receivedMsg.content["candidateId"].get!NodeId,
                     receivedMsg.content["term"].get!int,
                     receivedMsg.content["lastLogIndex"].get!uint,
                     receivedMsg.content["lastLogTerm"].get!int,
                     receivedMsg.content["voteGranted"].get!bool);
                break;

            default:
                break;
        }

        return true;
    }

private:

    uint _getMyLastLogIndex() {
        if (m_entriesQueue.empty()) {
            return 0;
        }
        return m_entriesQueue.back().content["logIndex"].get!uint;
    }

    int _getMyLastLogTerm() {
        if (m_entriesQueue.empty()) {
            return 0;
        }
        return m_entriesQueue.back().content["logTerm"].get!int;
    }

    void _startElection() {
        m_state = RaftState.Candidate;
        m_currentTerm++;
        m_votedFor = this.m_id;
        writeln("[", m_id, "] Started election for term ", m_currentTerm);
        Message requestVoteMsg = {
            srcId: this.m_id,
            dstId: 0, // to fill per peer
            type: Message.Type.RAFT.RequestVote,
            content: JSONValue(["candidateId" : this.m_id, "term" : m_currentTerm,
                                "lastLogIndex" : _getMyLastLogIndex(),
                                "lastLogTerm" : _getMyLastLogTerm()])
        };

        foreach (peer; SERVER_IDS) {
            if (peer != this.m_id) {
                requestVoteMsg.dstId = peer;
                m_communicator.send(requestVoteMsg);// TODO : danger! it might not copy as expected and cause havoc
                //requestVoteMsg = requestVoteMsg.dup;
            }
        }
    }

    bool _addEntry(Message msg) {
        if (m_currentLeader == this.m_id && m_state == RaftState.Leader) {
            writeln("[", this.m_id, "] Leader appending entry: ", msg);
            foreach (peer; SERVER_IDS) {
                if (peer != this.m_id) {
                    msg.dstId = peer;
                    m_communicator.send(msg);
                }
            }
        } else {
            m_currentLeader = msg.content["leaderId"].get!NodeId;
            m_currentTerm = msg.content["term"].get!int;
            m_entriesQueue.insertBack(msg);
            msg.srcId = this.m_id;
            msg.dstId = m_currentLeader;
            msg.type = Message.Type.RAFT.AppendEntriesResponse;
            writeln("[", this.m_id, "] Follower appending entry: ", msg);
            m_communicator.send(msg);
        }

        return true;
    }

    bool _voteRequest(NodeId candidateId, int term, uint lastLogIndex, int lastLogTerm) {
        //TODO: use last two fields for log comparison
        bool voteGranted = true;
        if (term < m_currentTerm || m_state != RaftState.Follower) {
            voteGranted = false;
        } else {
            m_votedFor = candidateId;
        }
        
        Message response = {
            srcId: this.m_id,
            dstId: candidateId,
            type: Message.Type.RAFT.RequestVoteResponse,
            content: JSONValue(["candidateId" : candidateId, "term" : term,
                                "lastLogIndex" : lastLogIndex, "lastLogTerm" : lastLogTerm,
                                    "voteGranted" : voteGranted])
        };
        m_communicator.send(response);
        return true;
    }

    bool _voteResponse(NodeId candidateId, int term, uint lastLogIndex, int lastLogTerm, bool voteGranted) {
        static uint votedForMe = 1;
        if (term < m_currentTerm || m_state != RaftState.Candidate || candidateId != this.m_id) {
            return false;
        }

        if (voteGranted) {
            votedForMe++;
            if (votedForMe > RAFT_NODES / 2) {
                m_state = RaftState.Leader;
                votedForMe = 1;// candidate always votes for itself
                m_currentLeader = this.m_id;
                writeln("[", m_id, "] Became leader for term ", m_currentTerm);
                Message response = {
                    srcId: this.m_id,
                    dstId: 0, // to fill per peer
                    type: Message.Type.RAFT.AppendEntries,
                    content: JSONValue(["leaderId" : this.m_id, "term" : m_currentTerm,
                                        "lastLogIndex" : lastLogIndex+1, "lastLogTerm" : m_currentTerm])
                };
                // Announce new leader step-up
                this._addEntry(response);
            }
        }
        return true;
    }

    void _resetElectionTimeout() {
        m_electionTimeout = dur!("seconds")(uniform(1, 3));
    }

    void _probeElectionTimeout() {
        if (Clock.currTime() - m_lastHeartbeat > m_electionTimeout) {
            _startElection();
            _resetElectionTimeout();
        }
    }

    void _receiveHeartbeat() {
        m_lastHeartbeat = Clock.currTime();
        m_state = RaftState.Follower;
    }
}

class ServerNode : RaftNode {
    this(NodeId id, ICommunicator communicator){
        super(id, communicator);
    }

    void run() {
        // Server-specific run logic
        // ...
        Fiber raftFiber = new Fiber({
            SysTime lastTime = Clock.currTime();
            while (true) {
                if (Clock.currTime() - lastTime > 100.msecs){
                    this.raftIteration();
                    lastTime = Clock.currTime();
                }
                Fiber.yield();
            }
        });

        Fiber serverFiber = new Fiber({
            SysTime lastTime = Clock.currTime();
            while (true) {
                if (Clock.currTime() - lastTime > 50.msecs){
                    while (this.handleRequest()) {}
                    lastTime = Clock.currTime();
                }
                Fiber.yield();
            }
        });

        while (true){
            raftFiber.call();
            serverFiber.call();
        }
    }
}

class ClientNode : Node {
    this(NodeId id, ICommunicator communicator){
        super(id, communicator);
    }

    void run() {
        // Client-specific run logic
        // ...
    }

    bool send(string text) {
        // Client-specific send logic
        // ...
        return true;
    }
}
