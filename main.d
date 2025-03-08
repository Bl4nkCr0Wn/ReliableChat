import std.stdio;
import std.socket;
import std.concurrency;
import std.json;
import std.algorithm;
import std.random;

import node;
import communication;

// Simple Chat Server
class ChatServer {
	//ServerNode!ConsoleCommunicator node;
    TcpSocket listener;
    this(uint port) {
        listener = new TcpSocket();
        listener.bind(new InternetAddress("127.0.0.1", port));
        listener.listen(10);
    }
    void start() {
        while (true) {
            auto client = listener.accept();
            auto message = client.receive(1024);
            writeln("Received: ", message);
            client.send("Message received");
        }
    }
}

// Simple Chat Client
class ChatClient {

    TcpSocket socket;
    this(string host, uint port) {
        socket = new TcpSocket();
        socket.connect(new InternetAddress(host, port));
    }
    void sendMessage(string msg) {
        socket.send(msg);
        writeln("Server response: ", socket.receive(1024));
    }
}

// Unittest
unittest {
    ChatServer[RAFT_NODES] servers;
    for (uint i = 0; i < servers.length; i++){
        servers[i] = new ChatServer(i);
	}

    ChatClient[] clients = [new ChatClient(6), new ChatClient(7)];
    
	auto node = new RaftNode("Node1");
    assert(node.state == RaftNode.State.Follower);
    node.startElection();
    assert(node.state == RaftNode.State.Candidate);

    auto pbftNode = new PBFTNode("PBFT1", ["PBFT2", "PBFT3", "PBFT4"]);
    pbftNode.sendPrepare("Test Message");
}
