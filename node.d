module node;

import std.stdio;
import globals;
import communication;


// Base class for nodes
class Node(CommunicatorT : ICommunicator){
    NodeId m_id;
    CommunicatorT m_communicator;

    this(NodeId id) {
        this.m_id = id;
	}

    abstract bool handleRequest(Message msg);
}

// RAFT Implementation
class RaftNode(CommunicatorT : ICommunicator) : Node!CommunicatorT {
    enum RaftState { Follower, Candidate, Leader }
    private RaftState m_state = RaftState.Follower;
    private uint m_currentTerm = 0;
    private NodeId m_votedFor = 0;
    private NodeId m_currentLeader = INVALID_LEADER_ID;
    private Message[MESSAGE_LOG_SIZE] m_entries;

    this(NodeId id) {
        super(id);
	}

    void startElection() {
        state = State.Candidate;
        currentTerm++;
        votedFor = this.m_id;
        writeln("[", id, "] Started election for term ", currentTerm);
    }

    auto addEntry() {}
    auto vote() {}

    bool handleRequest() {

	}

}

class ServerNode(CommunicatorT : ICommunicator) : RaftNode!CommunicatorT {
    this(NodeId id){
        super(id);
	}

    void run() {

	}
}

class ClientNode(CommunicatorT : ICommunicator) : Node!CommunicatorT {
    this(NodeId id){
        super(id);
	}

    void run() {

	}

    bool send(string text) {

	}
}
