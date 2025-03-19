import std.stdio;
import core.thread;
import std.conv;

import node;
import communication;
import globals;


// Unittest
unittest {
    class RaftTesterCommunicator : SharedMemoryCommunicator {
        private Message[int] m_recvMessageMap;
        private immutable NodeId[] m_servers;
        private immutable NodeId[] m_clients;
        private int m_searchStartIndex;

        this(immutable NodeId[] servers, immutable NodeId[] clients){
            super(servers ~ clients);
            m_servers = servers;
            m_clients = clients;
            m_searchStartIndex = 0;
        }

        override bool recv(ref Message msg) {
            bool res = super.recv(msg);
            Message recvMsgCopy = msg;
            m_recvMessageMap[msg.messageId] = recvMsgCopy;
            return res;
        }

        override bool send(Message msg) {
            return super.send(msg);
        }

        bool checkForLeaderStepUp(NodeId expectedLeader) {
            
            if (!_searchRequestVote(expectedLeader)){
                writeln("ERROR: No request vote found");
                return false;
            }
            
            if (_countRequestVotesGranted(expectedLeader) < m_servers.length/2){
                writeln("ERROR: Not enough votes granted");
                return false;
            }
            
            if (!_searchHeartbeatSent(expectedLeader)){
                writeln("ERROR: No heartbeat sent by leader");
                return false;
            }
            
            return true;
        }

        private bool _searchRequestVote(NodeId candidateId) {
            for (; m_searchStartIndex < m_recvMessageMap.length; m_searchStartIndex++){
                if (m_recvMessageMap[m_searchStartIndex].type == Message.Type.RaftRequestVote &&
                    m_recvMessageMap[m_searchStartIndex].srcId == candidateId){
                    return true;
                }
            }

            return false;
        }

        private int _countRequestVotesGranted(NodeId candidateId) {
            int count = 0;
            for (; m_searchStartIndex < m_recvMessageMap.length && (count < m_servers.length/2); m_searchStartIndex++){
                if (m_recvMessageMap[m_searchStartIndex].type == Message.Type.RaftRequestVoteResponse &&
                    m_recvMessageMap[m_searchStartIndex].dstId == candidateId &&
                    m_recvMessageMap[m_searchStartIndex].content["voteGranted"].get!bool){
                    count++;
                }
            }

            return count;
        }

        private bool _searchHeartbeatSent(NodeId leaderId) {
            for (; m_searchStartIndex < m_recvMessageMap.length; m_searchStartIndex++){
                if (m_recvMessageMap[m_searchStartIndex].type == Message.Type.RaftAppendEntries &&
                    m_recvMessageMap[m_searchStartIndex].srcId == leaderId &&
                    m_recvMessageMap[m_searchStartIndex].content["leaderId"].get!NodeId == leaderId &&
                    m_recvMessageMap[m_searchStartIndex].content["content"].get!string == "heartbeat"){
                    return true;
                }
            }

            return false;
        }
    }

    // basic scenario enables 5 servers and 2 client on local machine by using
    // servers[i] and clients[i]
    mixin template raftBasicScenarioSetup(const uint serversAmount, const uint clientsAmount) {
        enum NodeId[serversAmount] SERVER_IDS = [1, 2, 3, 4, 5];
        enum NodeId[clientsAmount] CLIENT_IDS = [6, 7];
        
        RaftTesterCommunicator tester = new RaftTesterCommunicator(SERVER_IDS, CLIENT_IDS);
        
        LocalServerNode[serversAmount] servers = [
            new LocalServerNode(SERVER_IDS[0], SERVER_IDS, tester),
            new LocalServerNode(SERVER_IDS[1], SERVER_IDS, tester),
            new LocalServerNode(SERVER_IDS[2], SERVER_IDS, tester),
            new LocalServerNode(SERVER_IDS[3], SERVER_IDS, tester),
            new LocalServerNode(SERVER_IDS[4], SERVER_IDS, tester)];
        
        ClientNode[clientsAmount] clients = [
            new ClientNode(CLIENT_IDS[0], tester),
            new ClientNode(CLIENT_IDS[1], tester)];
    }

    void _raftLeaderStepUp(S, C)(ref S servers, ref C clients) {
        import core.thread;
        Thread.sleep(RaftNode.MAX_ELECTION_TIMEOUT.seconds);

        // make sure expectedLeader starts election
        servers[0].runRaftIteration();
        // handle vote requests
        foreach (i; 1..servers.length){
            servers[i].runHandleMessageOnce();
        }
        // handle vote responses
        foreach (i; 1..servers.length){
            servers[0].runHandleMessageOnce();
        }
        // recv heartbeats from leader
        foreach (i; 1..servers.length){
            servers[i].runHandleMessageOnce();
            servers[i].runRaftIteration();
        }
    }

    void test_happy_raftLeaderStepUp() {
        mixin raftBasicScenarioSetup!(RAFT_NODES, 2);
        enum expectedLeader = SERVER_IDS[0];
        
        _raftLeaderStepUp(servers, clients);

        assert(tester.checkForLeaderStepUp(expectedLeader), "Leader step up failed");
    }

    void testMain() {
        test_happy_raftLeaderStepUp();

        // mixin raftBasicScenarioSetup!(RAFT_NODES, 2);

        // Fiber[RAFT_NODES] serverFibers;
        // serverFibers[0] = new Fiber({ servers[0].run(); });
        // serverFibers[1] = new Fiber({ servers[1].run(); });
        // serverFibers[2] = new Fiber({ servers[2].run(); });
        // serverFibers[3] = new Fiber({ servers[3].run(); });
        // serverFibers[4] = new Fiber({ servers[4].run(); });

        // import std.random;
        // int[] runOrder = [0, 1, 2, 3, 4];
        // while (true){
        //     runOrder.randomShuffle();
        //     foreach (i; runOrder){
        //         serverFibers[i].call();
        //     }
        // }

        // assert(1 == 2, "Test failed");
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