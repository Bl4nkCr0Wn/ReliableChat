import std.stdio;
import core.thread;

import node;
import communication;
import globals;

// // Simple Chat Server
// class ChatServer {
// 	//ServerNode!ConsoleCommunicator node;
//     TcpSocket listener;
//     this(uint port) {
//         listener = new TcpSocket();
//         listener.bind(new InternetAddress("127.0.0.1", port));
//         listener.listen(10);
//     }
//     void start() {
//         while (true) {
//             auto client = listener.accept();
//             auto message = client.receive(1024);
//             writeln("Received: ", message);
//             client.send("Message received");
//         }
//     }
// }

// // Simple Chat Client
// class ChatClient {

//     TcpSocket socket;
//     this(string host, uint port) {
//         socket = new TcpSocket();
//         socket.connect(new InternetAddress(host, port));
//     }
//     void sendMessage(string msg) {
//         socket.send(msg);
//         writeln("Server response: ", socket.receive(1024));
//     }
// }

// Unittest
unittest {
    SharedMemoryCommunicator communicator = new SharedMemoryCommunicator(SERVER_IDS ~ CLIENT_IDS);
    ServerNode[SERVER_NODES_AMOUNT] servers;
    for (uint i = 0; i < SERVER_NODES_AMOUNT; i++){
        servers[i] = new ServerNode(SERVER_IDS[i], communicator);
    }

    // ClientNode[2] clients = [new ClientNode(CLIENT_IDS[0], communicator),
    //                          new ClientNode(CLIENT_IDS[1], communicator)];
    writeln("Servers: ", servers);
    Thread[SERVER_NODES_AMOUNT] serverThreads;
    foreach (i, server; servers) {
        writeln("Starting server ", i);
        serverThreads[i] = new Thread({
            server.run();
        });
        serverThreads[i].start();
    }

    foreach (thread; serverThreads) {
        thread.join();
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

void main(){
    writeln("Hello, World!");
}