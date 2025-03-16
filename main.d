import std.stdio;
import core.thread;

import node;
import communication;
import globals;


// Unittest
unittest {
    class RaftTesterCommunicator : SharedMemoryCommunicator {
        Message[int] recvMessageMap;

        this(const NodeId[] peers){
            super(peers);
        }

        override bool recv(ref Message msg) {
            bool res = super.recv(msg);
            Message recvMsgCopy = msg;
            recvMessageMap[msg.messageId] = recvMsgCopy;
            return res;
        }

        override bool send(Message msg) {
            return super.send(msg);
        }
    }

    void testMain() {
        RaftTesterCommunicator communicator = new RaftTesterCommunicator(SERVER_IDS ~ CLIENT_IDS);
        LocalServerNode[SERVER_NODES_AMOUNT] servers;
        for (uint i = 0; i < SERVER_NODES_AMOUNT; i++){
            servers[i] = new LocalServerNode(SERVER_IDS[i], communicator);
        }
        
        // ClientNode[2] clients = [new ClientNode(CLIENT_IDS[0], communicator),
        //                          new ClientNode(CLIENT_IDS[1], communicator)];
        Fiber[SERVER_NODES_AMOUNT] serverFibers;
        serverFibers[0] = new Fiber({ servers[0].run(); });
        serverFibers[1] = new Fiber({ servers[1].run(); });
        serverFibers[2] = new Fiber({ servers[2].run(); });
        serverFibers[3] = new Fiber({ servers[3].run(); });
        serverFibers[4] = new Fiber({ servers[4].run(); });

        import std.random;
        int[] runOrder = [0, 1, 2, 3, 4];
        while (true){
            runOrder.randomShuffle();
            foreach (i; runOrder){
                serverFibers[i].call();
            }
        }

        assert(1 == 2, "Test failed");
        // ChatServer[RAFT_NODES] servers;
        // for (uint i = 0; i < servers.length; i++){
        //     servers[i] = new ChatServer(i);
        // }

        // ChatClient[] clients = [new ChatClient(6), new ChatClient(7)];
        
        // auto node = new RaftNode("Node1");
        // assert(node.state == RaftNode.State.Follower);
        // node.startElection();
        // assert(node.state == RaftNode.State.Candidate);

        // auto pbftNode = new PBFTNode("PBFT1", ["PBFT2", "PBFT3", "PBFT4"]);
        // pbftNode.sendPrepare("Test Message");
    }

    testMain();
}

void main(){
    writeln("Hello, World!");
}