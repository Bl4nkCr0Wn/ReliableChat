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

        writeln("[", this.m_id, "] Handling message: ", receivedMsg);
        switch (receivedMsg.type) {
            case Message.Type.RaftAppendEntries:
                // AppendEntries logic
                // ...
                _receiveHeartbeat();
                _addEntry(receivedMsg);
                break;
            case Message.Type.RaftAppendEntriesResponse:
                // AppendEntriesResponse logic
                // ...
                break;
            case Message.Type.RaftRequestVote:
                _voteRequest(receivedMsg.content["candidateId"].get!NodeId,
                     receivedMsg.content["term"].get!int, 
                     receivedMsg.content["lastLogIndex"].get!uint,
                     receivedMsg.content["lastLogTerm"].get!int);
                break;
            case Message.Type.RaftRequestVoteResponse:
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
            type: Message.Type.RaftRequestVote,
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
            msg.type = Message.Type.RaftAppendEntriesResponse;
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
            type: Message.Type.RaftRequestVoteResponse,
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
                    type: Message.Type.RaftAppendEntries,
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
        if (this.m_state == RaftState.Follower && Clock.currTime() - m_lastHeartbeat > m_electionTimeout) {
            _resetElectionTimeout();
            _startElection();
        }
    }

    void _receiveHeartbeat() {
        m_lastHeartbeat = Clock.currTime();
        m_state = RaftState.Follower;
    }
}

class LocalServerNode : RaftNode {
    this(NodeId id, ICommunicator communicator){
        super(id, communicator);
    }

    /** 
     * Local server runs as local fiber and handles only up to handful of messages + raft iteration if necessary before yielding
     */
    void run() {
        writeln("[", this.m_id, "] Running");
        SysTime lastRaftTime = Clock.currTime();
        SysTime requestHandleStartTime;
        while (true){
            if (Clock.currTime() - lastRaftTime > 100.msecs){
                this.raftIteration();
                lastRaftTime = Clock.currTime();
            }

            for (int i = uniform(0, 5); i < 5 && this.handleRequest(); i++) {}
            Fiber.yield();
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
