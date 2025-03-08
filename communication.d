module communication;

import globals;

interface ICommunicator {
	bool send(Message msg);
	Message recv();
}

// SharedMemoryCommunicator : ICommunicator

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