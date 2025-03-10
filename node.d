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
                break;
            case Message.Type.RAFT.AppendEntriesResponse:
                // AppendEntriesResponse logic
                // ...
                break;
            case Message.Type.RAFT.RequestVote:
                // RequestVote logic
                // ...
                break;
            case Message.Type.RAFT.RequestVoteResponse:
                // RequestVoteResponse logic
                // ...
                break;

            default:
                break;
        }

        return true;
    }

private:
    void _startElection() {
        m_state = RaftState.Candidate;
        m_currentTerm++;
        m_votedFor = this.m_id;
        writeln("[", m_id, "] Started election for term ", m_currentTerm);
        Message requestVoteMsg = {
            srcId: this.m_id,
            dstId: 0, // to fill per peer
            type: Message.Type.RAFT.RequestVote,
            content: JSONValue(["candidateId" : this.m_id, "term" : m_currentTerm])
        };

        foreach (peer; SERVER_IDS) {
            if (peer != this.m_id) {
                requestVoteMsg.dstId = peer;
                m_communicator.send(requestVoteMsg);// TODO : danger! it might not copy as expected and cause havoc
                //requestVoteMsg = requestVoteMsg.dup;
            }
        }        
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
