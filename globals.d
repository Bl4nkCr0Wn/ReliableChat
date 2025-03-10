module globals;

import std.json;

// Constants for RAFT and PBFT
enum int RAFT_TIMEOUT = 1500;  // in ms
enum int RAFT_NODES = 5;        // Minimum 5 nodes for RAFT 2 crash failures
enum int PBFT_NODES = 4;       // Minimum 4 nodes for PBFT

import std.algorithm.comparison: max;
enum uint SERVER_NODES_AMOUNT = max(RAFT_NODES, PBFT_NODES);
enum uint MESSAGE_LOG_SIZE = 64; // How much of a history the system keeps

alias NodeId = int;
enum NodeId INVALID_NODE_ID = -1;

struct Message {
    struct Type {
        enum RAFT : _MessageType { 
            AppendEntries = 1, 
            AppendEntriesResponse,
            RequestVote, 
            RequestVoteResponse
        }

        enum PBFT : _MessageType {
            PrePrepare = 10,
            Prepare,
            Commit,
            Reply
        }
    } alias _MessageType = byte;
    
    int messageId = -1;
    NodeId srcId = INVALID_NODE_ID;
    NodeId dstId = INVALID_NODE_ID;
    _MessageType type;
    JSONValue content;
}

const NodeId[2] CLIENT_IDS = [6, 7];
const NodeId[SERVER_NODES_AMOUNT] SERVER_IDS = [1, 2, 3, 4, 5];