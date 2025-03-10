module node;

import std.stdio;
import globals;
import communication;
import std.datetime;
import std.random;

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
    import std.container.array: Array;
    private Array!Message m_entriesQueue;
    private Duration m_electionTimeout;
    private SysTime m_lastHeartbeat;

    this(NodeId id, ICommunicator communicator) {
        super(id, communicator);
        m_entriesQueue = new Array!Message(MESSAGE_LOG_SIZE);
        resetElectionTimeout();
        m_lastHeartbeat = Clock.currTime();
    }

    void raftIteration() {
        _probeElectionTimeout();
        // Other periodic tasks
        // ...
    }

    bool handleRequest(Message msg) {
        // Handle incoming messages (AppendEntries, RequestVote, etc.)
        // ...
        return true;
    }

private:
    void startElection() {
        m_state = RaftState.Candidate;
        m_currentTerm++;
        m_votedFor = this.m_id;
        writeln("[", m_id, "] Started election for term ", m_currentTerm);
        // Send RequestVote RPCs to other nodes
        // ...
    }

    auto addEntry(Message msg) {
        if (m_entriesQueue.length == MESSAGE_LOG_SIZE) {
            m_entriesQueue.popFront();
            m_entriesQueue.insertBack(msg);
        } else {
            m_entriesQueue.insertBack(msg);
        }

        return true;
    }

    auto vote() {
        // Implement voting logic
        // ...
    }

    void _resetElectionTimeout() {
        m_electionTimeout = dur!("seconds")(uniform(10, 30));
    }

    void _probeElectionTimeout() {
        if (Clock.currTime() - m_lastHeartbeat > m_electionTimeout) {
            startElection();
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
        while (True){
            this.raftIteration();
            sleep(1.milliseconds);
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
