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

    protected abstract bool handleRequest();
}

// RAFT Implementation
class RaftNode : Node {
    enum RaftState { Follower, Candidate, Leader }
    enum Duration HEARTBEAT_DURATION = 1000.msecs;
    enum uint MIN_ELECTION_TIMEOUT = 2;//seconds
    enum uint MAX_ELECTION_TIMEOUT = 8;//seconds

    private NodeId[] m_peers;
    private RaftState m_state = RaftState.Follower;
    private int m_currentTerm = -1;
    private NodeId m_votedFor = INVALID_NODE_ID;
    private NodeId m_currentLeader = INVALID_NODE_ID;
    private DList!Message m_entriesQueue;
    private Duration m_electionTimeout;
    private SysTime m_lastHeartbeat;

    this(NodeId id, NodeId[] peers, ICommunicator communicator) {
        super(id, communicator);
        m_peers = peers;
        m_entriesQueue = DList!Message();
        _resetElectionTimeout();
        m_lastHeartbeat = Clock.currTime();
    }

    void raftIteration() {
        _probeElectionTimeout();

        if (m_state == RaftState.Leader) {
            // send heartbeat
            if (Clock.currTime() - m_lastHeartbeat > HEARTBEAT_DURATION) {
                _sendHeartbeat();
            }
        }
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
            case Message.Type.ClientRequest:
                _handleClientRequest(receivedMsg);
                break;

            case Message.Type.ClientResponse:
                // Client response means leader commited entry
                _handleClientResponse(receivedMsg);
                break;

            case Message.Type.RaftAppendEntries:
                _receiveHeartbeat();
                _addEntry(receivedMsg);
                break;

            case Message.Type.RaftAppendEntriesResponse:
                _addEntryResponse(receivedMsg);
                break;

            case Message.Type.RaftRequestVote:
                _voteRequest(receivedMsg.content["candidateId"].get!NodeId,
                     receivedMsg.content["logIndex"].get!uint,
                     receivedMsg.content["logTerm"].get!int);
                break;

            case Message.Type.RaftRequestVoteResponse:
                _voteResponse(receivedMsg.content["candidateId"].get!NodeId,
                     receivedMsg.content["logTerm"].get!int,
                     receivedMsg.content["voteGranted"].get!bool);
                break;

            default:
                assert(false, "Unknown message type");
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
            content: JSONValue(["candidateId" : this.m_id,
                                "logIndex" : _getMyLastLogIndex()+1,
                                "logTerm" : m_currentTerm])
        };

        foreach (peer; m_peers) {
            if (peer != this.m_id) {
                requestVoteMsg.dstId = peer;
                m_communicator.send(requestVoteMsg);
            }
        }
    }

    void _sendHeartbeat() {
        // Send heartbeat to followers
        Message heartbeat = {
            srcId: this.m_id,
            dstId: 0, // to fill per peer
            type: Message.Type.RaftAppendEntries,
            content: JSONValue(["leaderId" : JSONValue(this.m_id),
                                "logIndex" : JSONValue(_getMyLastLogIndex+1),
                                "logTerm" : JSONValue(m_currentTerm),
                                "content" : JSONValue("heartbeat")])
        };
        this._addEntry(heartbeat);
        m_lastHeartbeat = Clock.currTime();
    }

    bool _handleClientRequest(Message msg) {
        if (m_currentLeader == INVALID_NODE_ID) {
            // No leader, client should retry later
            writeln("[", this.m_id, "] No leader, client should retry later");
            return false;
        } else if (m_currentLeader != this.m_id) {
            // Forward request to leader
            msg.dstId = m_currentLeader;
            return m_communicator.send(msg);
        }

        msg.type = Message.Type.RaftAppendEntries;
        msg.content = JSONValue(["logIndex" : JSONValue(_getMyLastLogIndex()+1),
                                "logTerm" : JSONValue(m_currentTerm),
                                "leaderId" : JSONValue(this.m_id),
                                "clientId" : JSONValue(msg.srcId),
                                "content" : JSONValue(msg.content)]);
        msg.srcId = this.m_id;
        msg.dstId = 0; // to fill per peer
        _addEntry(msg);
        return true;
    }

    bool _handleClientResponse(Message msg) {
        // Client response means leader commited entry
        m_currentLeader = msg.content["leaderId"].get!NodeId;
        m_currentTerm = msg.content["logTerm"].get!int;
        m_entriesQueue.insertBack(msg);
        return true;
    }

    bool _addEntry(Message msg) {
        if (m_currentLeader == this.m_id && m_state == RaftState.Leader) {
            writeln("[", this.m_id, "] Leader appending entry: ", msg);
            foreach (peer; m_peers) {
                if (peer != this.m_id) {
                    msg.dstId = peer;
                    m_communicator.send(msg);
                }
            }
        } else if (m_votedFor == msg.srcId || m_currentLeader == msg.srcId) {
            if (msg.content["content"].get!string == "heartbeat") {
                m_currentLeader = msg.content["leaderId"].get!NodeId;
                m_currentTerm = msg.content["logTerm"].get!int;
                return true;
            }
            msg.dstId = msg.srcId;
            msg.srcId = this.m_id;
            msg.type = Message.Type.RaftAppendEntriesResponse;
            msg.content["origMessageId"] = msg.messageId;
            writeln("[", this.m_id, "] Follower responding entry: ", msg);
            m_communicator.send(msg);
        }

        return true;
    }

    bool _addEntryResponse(Message msg) {
        static uint[int] messageAckCount;
        messageAckCount[msg.content["origMessageId"].get!int]++;
        // this is strict equality to avoid responding several times to the same message
        if (messageAckCount[msg.content["origMessageId"].get!int] == (RAFT_NODES / 2)) {
            if (msg.content["content"].get!string == "heartbeat") {
                return true;
            }

            // Entry is committed
            this.m_entriesQueue.insertBack(msg);
            // else this is client request and should receive response (and servers should commit)
            Message commitMsg = {
                srcId: this.m_id,
                dstId: 0, // to fill per peer
                type: Message.Type.ClientResponse,
                content: JSONValue(["logIndex" : JSONValue(_getMyLastLogIndex),
                                    "logTerm" : JSONValue(m_currentTerm),
                                    "leaderId" : JSONValue(this.m_id),
                                    "content" : JSONValue(msg.content["content"])])
            };
            foreach (peer; m_peers) {
                if (peer != this.m_id) {
                    commitMsg.dstId = peer;
                    m_communicator.send(commitMsg);
                }
            }

            Message clientMsg = {
                type: Message.Type.ClientResponse,
                srcId: this.m_id,
                dstId: msg.content["clientId"].get!NodeId,
                content: JSONValue(msg.content["content"]),
            };
            return m_communicator.send(clientMsg);
        }
        
        return true;
    }

    bool _voteRequest(NodeId candidateId, uint logIndex, int logTerm) {
        bool voteGranted = true;
        if (logTerm < m_currentTerm || logIndex < _getMyLastLogTerm() || m_state != RaftState.Follower) {
            voteGranted = false;
        } else {
            m_votedFor = candidateId;
        }
        
        Message response = {
            srcId: this.m_id,
            dstId: candidateId,
            type: Message.Type.RaftRequestVoteResponse,
            content: JSONValue(["candidateId" : JSONValue(candidateId),
                                "logIndex" : JSONValue(logIndex), 
                                "logTerm" : JSONValue(logTerm),
                                "voteGranted" : JSONValue(voteGranted)])
        };
        m_communicator.send(response);
        return true;
    }

    bool _voteResponse(NodeId candidateId, int logTerm, bool voteGranted) {
        static uint votedForMe = 1;
        if (logTerm < m_currentTerm || m_state != RaftState.Candidate || candidateId != this.m_id) {
            return false;
        }

        if (voteGranted) {
            votedForMe++;
            if (votedForMe > RAFT_NODES / 2) {
                m_state = RaftState.Leader;
                votedForMe = 1;// candidate always votes for itself
                m_currentLeader = this.m_id;
                writeln("[", m_id, "] Became leader for term ", m_currentTerm);
                // Announce new leader step-up
                this._sendHeartbeat();
            }
        }
        return true;
    }

    void _resetElectionTimeout() {
        m_electionTimeout = dur!("seconds")(uniform(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT));
    }

    void _probeElectionTimeout() {
        if (this.m_state != RaftState.Leader && Clock.currTime() - m_lastHeartbeat > m_electionTimeout) {
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
    this(NodeId id, NodeId[] peers, ICommunicator communicator){
        super(id, peers, communicator);
    }

    /** 
     * Local server runs as local fiber and handles only up to handful of messages + raft iteration if necessary before yielding
     */
    void run() {
        writeln("[", this.m_id, "] Running");
        SysTime lastRaftTime = Clock.currTime();
        while (true){
            if (Clock.currTime() - lastRaftTime > 100.msecs){
                this.raftIteration();
                lastRaftTime = Clock.currTime();
            }

            for (int i = uniform(0, 5); i < 5 && this.handleRequest(); i++) {}
            Fiber.yield();
        }
    }

    void runRaftIteration() {
        this.raftIteration();
    }

    bool runHandleMessageOnce(){
        return this.handleRequest();
    }
}

class ClientNode : Node {
    this(NodeId id, ICommunicator communicator){
        super(id, communicator);
    }

    void run() {
        // Client-specific run logic
        // ...
        assert(false, "Not implemented");
    }

    bool send(string text) {
        Message msg = {
            type: Message.Type.ClientRequest,
            srcId: this.m_id,
            dstId: 1,//For now clients always send to server ID 1
            content: JSONValue(text),
        };
        return this.m_communicator.send(msg);
    }

    string recv(){
        Message msg = {
            dstId: this.m_id,
        };

        if (this.m_communicator.recv(msg) && msg.type == Message.Type.ClientResponse){
            writeln("Client message commited: ", msg);
            return msg.content.get!string;
        }
        return null;
    }

    override protected bool handleRequest(){
        return true;
    }
}
