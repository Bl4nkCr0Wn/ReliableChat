module globals;

import std.json;

// Constants for RAFT and PBFT
enum int RAFT_TIMEOUT = 1500;  // in ms
enum int RAFT_NODES = 5;        // Minimum 5 nodes for RAFT 2 crash failures
enum int PBFT_NODES = 4;       // Minimum 4 nodes for PBFT

// import std.algorithm.comparison: max;
// enum uint SERVER_NODES_AMOUNT = max(RAFT_NODES, PBFT_NODES);
enum uint MESSAGE_LOG_SIZE = 64; // How much of a history the system keeps

alias NodeId = int;
enum NodeId INVALID_NODE_ID = -1;

struct Message {
    enum Type : byte {
        RaftAppendEntries = 1, 
        RaftAppendEntriesResponse,
        RaftRequestVote, 
        RaftRequestVoteResponse,
        
        PbftPrePrepare = 10,
        PbftPrepare,
        PbftCommit,
        PbftReply,
    }
    
    int messageId = -1;
    Type type;
    NodeId srcId = INVALID_NODE_ID;
    NodeId dstId = INVALID_NODE_ID;
    JSONValue content;

    // Custom toString overload
    void toString(scope void delegate(const(char)[]) sink) const
    {
        import std.format : formattedWrite;
        formattedWrite(sink, "[MessageId %d | type %s | src %d | dst %d ] %s", messageId, type, srcId, dstId, content);
    }
}