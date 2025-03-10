module globals;

// Constants for RAFT and PBFT
enum int RAFT_TIMEOUT = 1500;  // in ms
enum int RAFT_NODES = 5;        // Minimum 5 nodes for RAFT 2 crash failures
enum int PBFT_NODES = 4;       // Minimum 4 nodes for PBFT

import std.algorithm.comparison: max;
enum uint SERVER_NODES_AMOUNT = max(RAFT_NODES, PBFT_NODES);
enum uint MESSAGE_LOG_SIZE = 64; // How much of a history the system keeps

alias NodeId = int;
enum NodeId INVALID_LEADER_ID = -1;

struct Message {
    enum MessageType {Content, RAFT, PBFT};
    int messageId;
    NodeId srcId;
    NodeId dstId;
    MessageType type;
    string content;
}
