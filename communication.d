module communication;

import std.stdio;
import std.container.dlist;

import globals;

interface ICommunicator {
	// Transfering the message given by fields
    bool send(Message msg);

	// Receiving message filtered by given fields
    bool recv(ref Message o_msg);
}

class SharedMemoryCommunicator : ICommunicator {
    DList!Message[NodeId] m_memoryMap;

    this(const NodeId[] peers) {
        foreach (peer; peers) {
            m_memoryMap[peer] = DList!Message();
        }
    }

    bool send(Message msg) {
        static int messageUniqueId = 0;
        msg.messageId = messageUniqueId++;
        writeln("[", msg.srcId, "] Sending message: ", msg);
        m_memoryMap[msg.dstId].insertBack(msg);
        return true;
    }

	// Note: requires supplying message with dstId of interest (caller nodeId)
    bool recv(ref Message o_msg) {
		if (m_memoryMap[o_msg.dstId].empty()) 
			return false;
		
		o_msg = m_memoryMap[o_msg.dstId].front;
		writeln("[", o_msg.dstId, "] Receiving message: ", o_msg);
        m_memoryMap[o_msg.dstId].removeFront();
		return true;
    }
}

// PBFTCommunicator : SharedMemoryCommunicator
//// PBFT Implementation
//class PBFTCommunicator : ICommunicator {
//    int[] replicas;
//    this(int[] peers) {
//        replicas = peers;
//    }
//
//    void sendPrepare(string message) {
//        writeln("[", id, "] Sending prepare: ", message);
//    }
//}
//
//unittest {
//    class Test1Communicator : PBFTCommunicator {
//        // and then we override send/receive 
//        // using static class queue to sync between all participants
//    }
//}
// TestCommunicator : PBFTCommunicator