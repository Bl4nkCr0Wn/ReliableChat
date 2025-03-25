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
    private DList!Message m_commitedEntriesQueue;
    private DList!Message m_notCommitedEntriesQueue;
    private Duration m_electionTimeout;
    private SysTime m_lastHeartbeat;

    this(NodeId id, NodeId[] peers, ICommunicator communicator) {
        super(id, communicator);
        m_peers = peers;
        m_commitedEntriesQueue = DList!Message();
        m_notCommitedEntriesQueue = DList!Message();
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

    bool _handleRequest(Message receivedMsg) {
        switch (receivedMsg.type) {
            case Message.Type.ClientRequest:
                _handleClientRequest(receivedMsg);
                break;

            case Message.Type.RaftAppendEntries:
                _receiveHeartbeat();
                _addEntry(receivedMsg);
                break;

            case Message.Type.RaftAppendEntriesResponse:
                _addEntryResponse(receivedMsg);
                break;

            case Message.Type.RaftRequestVote:
                _voteRequest(receivedMsg);
                break;

            case Message.Type.RaftRequestVoteResponse:
                _voteResponse(receivedMsg);
                break;

            default:
                assert(false, "Unknown message type");
                break;
        }
        return true;
    }

    override bool handleRequest() {
        // Handle incoming messages (AppendEntries, RequestVote, etc.)
        Message receivedMsg = {
            dstId: this.m_id
        };

        if (!m_communicator.recv(receivedMsg)) {
            return false;
        }

        return _handleRequest(receivedMsg);
    }

private:

    uint _getMyLastLogIndex() {
        if (!m_notCommitedEntriesQueue.empty()) {
            return m_notCommitedEntriesQueue.back().logIndex;

        } else if (!m_commitedEntriesQueue.empty()) {
            return m_commitedEntriesQueue.back().logIndex;

        } else {
            return 0;
        }
    }

    int _getMyLastLogTerm() {
        if (!m_notCommitedEntriesQueue.empty()) {
            return m_notCommitedEntriesQueue.back().logTerm;

        } else if (!m_commitedEntriesQueue.empty()) {
            return m_commitedEntriesQueue.back().logTerm;
            
        } else {
            return 0;
        }
    }

    void _startElection() {
        m_state = RaftState.Candidate;
        m_currentTerm++;
        m_votedFor = this.m_id;
        writeln("[", m_id, "] Started election for term ", m_currentTerm);
        Message requestVoteMsg = {
            type: Message.Type.RaftRequestVote,
            srcId: this.m_id,
            dstId: 0, // to fill per peer
            logIndex: _getMyLastLogIndex()+1,
            logTerm: m_currentTerm,
            content: JSONValue("")
        };

        foreach (peer; m_peers) {
            if (peer != this.m_id) {
                requestVoteMsg.dstId = peer;
                m_communicator.send(requestVoteMsg);
            }
        }

        writeln("[", m_id, "] Clearing uncommited entries for new term.");
        m_notCommitedEntriesQueue.clear();
    }

    void _sendHeartbeat() {
        // Send heartbeat to followers
        Message heartbeat = {
            type: Message.Type.RaftAppendEntries,
            srcId: this.m_id,
            dstId: 0, // to fill per peer
            logIndex: _getMyLastLogIndex()+1,
            logTerm: m_currentTerm,
            content: JSONValue(["subtype" : JSONValue("heartbeat")])
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
            writeln("[", this.m_id, "] Currently not leader, forwarding request to leader", m_currentLeader);
            msg.dstId = m_currentLeader;
            return m_communicator.send(msg);
        }

        // Leader handles client request
        msg.type = Message.Type.RaftAppendEntries;
        msg.logIndex = _getMyLastLogIndex()+1;
        msg.logTerm = m_currentTerm;
        msg.content = JSONValue([
            "subtype" : JSONValue("clientRequest"),
            "clientId" : JSONValue(msg.srcId),
            "content" : JSONValue(msg.content)]);
        msg.srcId = this.m_id;
        msg.dstId = 0; // to fill per peer
        _addEntry(msg);
        return true;
    }

    bool _addEntry(Message msg) {
        const Message copyForSave = msg;

        if (m_currentLeader == this.m_id && m_state == RaftState.Leader) {
            writeln("[", this.m_id, "] Leader scattering entry: ", msg);
            foreach (peer; m_peers) {
                if (peer != this.m_id) {
                    msg.dstId = peer;
                    m_communicator.send(msg);
                }
            }
            writeln("[", this.m_id, "] Adding message to NOT yet commited entries: ", msg);
            m_notCommitedEntriesQueue.insertBack(copyForSave);
        } else if (m_votedFor == msg.srcId || m_currentLeader == msg.srcId) {
            
            if (msg.content["subtype"].get!string == "heartbeat") {
                m_currentLeader = msg.srcId;
                m_currentTerm = msg.logTerm;

                while (!m_notCommitedEntriesQueue.empty()) {
                    if (m_notCommitedEntriesQueue.front.logTerm == msg.logTerm) {
                        if (m_notCommitedEntriesQueue.front.logIndex < msg.logIndex) {
                            m_commitedEntriesQueue.insertBack(m_notCommitedEntriesQueue.front);
                            m_notCommitedEntriesQueue.removeFront();
                        } else {
                            break;
                        }
                    } else {
                        m_notCommitedEntriesQueue.removeFront();
                    }
                }
            } 
            // else if (msg.content["subtype"].get!string == "clientRequest") {
            msg.dstId = msg.srcId;
            msg.srcId = this.m_id;
            msg.type = Message.Type.RaftAppendEntriesResponse;
            msg.content["origMessageId"] = msg.messageId;
            writeln("[", this.m_id, "] Follower responding entry: ", msg);
            m_communicator.send(msg);
            // }

            writeln("[", this.m_id, "] Follower adding message to NOT yet commited entries: ", msg);
            m_notCommitedEntriesQueue.insertBack(copyForSave);
        }

        return true;
    }

    bool _handleVerifiedEntry(int verifiedMessageId) {
        // Entry is committed
        for (auto it = m_notCommitedEntriesQueue[]; !it.empty; it.popFront()) {
            if (it.front.messageId == verifiedMessageId) {
                m_commitedEntriesQueue.insertBack(it.front);
                m_notCommitedEntriesQueue.popFirstOf(it);
                break;
            }
        }
        Message commitedEntry = m_commitedEntriesQueue.back;
        if (commitedEntry.content["subtype"].get!string == "clientRequest") {
            // this is client request and should receive response (and servers committed)
            Message clientMsg = {
                type: Message.Type.ClientResponse,
                srcId: this.m_id,
                dstId: commitedEntry.content["clientId"].get!NodeId,
                logIndex: commitedEntry.logIndex,
                logTerm: commitedEntry.logTerm,
                content: JSONValue(commitedEntry.content["content"]),
            };
            return m_communicator.send(clientMsg);
        }
        return true;
    }

    bool _addEntryResponse(Message msg) {

        static uint[int] messageAckCount;
        messageAckCount[msg.content["origMessageId"].get!int]++;
        // this is strict equality to avoid responding several times to the same message
        if (messageAckCount[msg.content["origMessageId"].get!int] == (RAFT_NODES / 2)) {
            _handleVerifiedEntry(msg.content["origMessageId"].get!int);
        }
        
        return true;
    }

    bool _voteRequest(Message msg) {
        bool voteGranted = true;
        if (msg.logTerm < m_currentTerm || msg.logIndex < _getMyLastLogIndex() || m_state != RaftState.Follower) {
            voteGranted = false;
        } else {
            m_votedFor = msg.srcId;
            m_state = RaftState.Follower;
        }
        
        msg.type = Message.Type.RaftRequestVoteResponse;
        msg.dstId = msg.srcId;
        msg.srcId = this.m_id;
        msg.content = JSONValue(["voteGranted" : JSONValue(voteGranted)]);
        writeln("[", this.m_id, "] Voting for ", m_votedFor, " in term ", msg.logTerm);
        return m_communicator.send(msg);
    }

    bool _voteResponse(Message msg) {
        static uint votedForMe = 1;
        if (msg.logTerm < m_currentTerm || m_state != RaftState.Candidate) {
            return false;
        }

        if (msg.content["voteGranted"].get!bool) {
            votedForMe++;
            // Note that starting condition m_state != Candidate protects from double execution
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

class RaftMixedPBFTNode : RaftNode {

    this(NodeId id, NodeId[] peers, ICommunicator communicator){
        super(id, peers, communicator);
    }

    override protected bool handleRequest(){
        Message receivedMsg = {
            dstId: this.m_id
        };

        if (!m_communicator.recv(receivedMsg)) {
            return false;
        }

        switch (receivedMsg.type) {
            case Message.Type.RaftAppendEntries:
                // treat as pre-prepare message
                _sendPrepare(receivedMsg);
                break;

            case Message.Type.PbftPrepare:
                // upon receiving enough validated prepare send commit reponse to all
                _handlePrepare(receivedMsg);
                break;

            case Message.Type.PbftCommit:
                // upon receiving enough validated commits send clientResponse reponse to client
                _handleCommit(receivedMsg);
                break;

            case Message.Type.RaftAppendEntriesResponse:
                // not supported in PBFT mode
                break;

            default:
                _handleRequest(receivedMsg);
                break;
        }
        
        return true;
    }

    bool _sendPrepare(Message appendMsg) {
        m_notCommitedEntriesQueue.insertBack(appendMsg);
        Message prepareMsg = {
            type: Message.Type.PbftPrepare,
            srcId: this.m_id,
            dstId: 0, //fill per peer
            logIndex: appendMsg.logIndex,
            logTerm: appendMsg.logTerm,
            content: JSONValue(["origMessageId" : JSONValue(appendMsg.messageId)]),
        };
        
        foreach (NodeId peer; m_peers) {
            if (peer != m_id) {
                prepareMsg.dstId = peer;
                m_communicator.send(prepareMsg);
            }
        }
        return true;
    }

    void _handlePrepare(Message prepareMsg) {
        // count from how many peers received prepare for message ID
        static uint[int] m_prepareMessagesCount;
        foreach (Message notCommitedMsg; m_notCommitedEntriesQueue) {
            // For this program verifying the message ID compared to what received from AppendEntry is verification
            if (notCommitedMsg.messageId == prepareMsg.content["origMessageId"].get!int) {
                m_prepareMessagesCount[notCommitedMsg.messageId]++;

                if (m_prepareMessagesCount[notCommitedMsg.messageId] == (PBFT_NODES - (PBFT_NODES/3))) {
                    _sendCommit(prepareMsg);
                    return;
                }
            }
        }
    }

    bool _sendCommit(Message prepareMsg) {
        Message commitMsg = {
            type: Message.Type.PbftCommit,
            srcId: this.m_id,
            dstId: 0, //fill per peer
            logIndex: prepareMsg.logIndex,
            logTerm: prepareMsg.logTerm,
            content: JSONValue(["origMessageId" : prepareMsg.content["origMessageId"]]),
        };
        
        foreach (NodeId peer; m_peers) {
            if (peer != m_id) {
                commitMsg.dstId = peer;
                m_communicator.send(commitMsg);
            }
        }
        return true;
    }

    void _handleCommit(Message commitMsg) {
        // count from how many peers received commit for message ID
        static uint[int] m_commitMessagesCount;
        foreach (Message notCommitedMsg; m_notCommitedEntriesQueue) {
            // For this program verifying the message ID compared to what received from AppendEntry is verification
            if (notCommitedMsg.messageId == commitMsg.content["origMessageId"].get!int) {
                m_commitMessagesCount[notCommitedMsg.messageId]++;

                if (m_commitMessagesCount[notCommitedMsg.messageId] == (PBFT_NODES - (PBFT_NODES/3))) {
                    _handleVerifiedEntry(notCommitedMsg.messageId);
                    _receiveHeartbeat();
                    Message lastCommitedMsg = m_commitedEntriesQueue.back;
                    if (m_votedFor == lastCommitedMsg.srcId || m_currentLeader == lastCommitedMsg.srcId) {
                        if (lastCommitedMsg.content["subtype"].get!string == "heartbeat") {
                            m_currentLeader = lastCommitedMsg.srcId;
                            m_currentTerm = lastCommitedMsg.logTerm;
                        }
                    }
                    return;
                }
            }
        }
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
