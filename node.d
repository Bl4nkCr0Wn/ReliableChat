module node;

import std.stdio;
import core.thread;
import std.datetime;
import std.random;
import std.container.dlist;

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

    abstract bool handleRequest(Message msg);
}

// RAFT Implementation
class RaftNode : Node {
    enum RaftState { Follower, Candidate, Leader }
    private RaftState m_state = RaftState.Follower;
    private uint m_currentTerm = 0;
    private NodeId m_votedFor = 0;
    private NodeId m_currentLeader = INVALID_LEADER_ID;
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

    override bool handleRequest(Message msg) {
        // Handle incoming messages (AppendEntries, RequestVote, etc.)
        // ...
        return true;
    }

private:
    void _startElection() {
        m_state = RaftState.Candidate;
        m_currentTerm++;
        m_votedFor = this.m_id;
        writeln("[", m_id, "] Started election for term ", m_currentTerm);
        // Send RequestVote RPCs to other nodes
        // ...
    }

    auto _addEntry(Message msg) {
        // if (m_entriesQueue.length == MESSAGE_LOG_SIZE) {
        //     m_entriesQueue.removeFront();
        // }
        m_entriesQueue.insertBack(msg);
        return true;
    }

    auto _vote() {
        // Implement voting logic
        // ...
    }

    void _resetElectionTimeout() {
        m_electionTimeout = dur!("seconds")(uniform(10, 30));
    }

    void _probeElectionTimeout() {
        if (Clock.currTime() - m_lastHeartbeat > m_electionTimeout) {
            _startElection();
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
        while (true){
            this.raftIteration();
            Thread.sleep(100.msecs);
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
