import std.stdio;
import core.thread;
import std.conv;

import node;
import communication;
import globals;


// Unittest
unittest {
    class RaftTesterCommunicator : SharedMemoryCommunicator {
        private Message[] m_messages;
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
            m_messages ~= recvMsgCopy;
            return res;
        }

        override bool send(Message msg) {
            m_messages ~= msg;
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
            for (; m_searchStartIndex < m_messages.length; m_searchStartIndex++){
                if (m_messages[m_searchStartIndex].type == Message.Type.RaftRequestVote &&
                    m_messages[m_searchStartIndex].srcId == candidateId){
                    return true;
                }
            }

            return false;
        }

        private int _countRequestVotesGranted(NodeId candidateId) {
            int count = 0;
            for (; m_searchStartIndex < m_messages.length && (count < m_servers.length/2); m_searchStartIndex++){
                if (m_messages[m_searchStartIndex].type == Message.Type.RaftRequestVoteResponse &&
                    m_messages[m_searchStartIndex].dstId == candidateId &&
                    m_messages[m_searchStartIndex].content["voteGranted"].get!bool){
                    count++;
                }
            }

            return count;
        }

        private bool _searchHeartbeatSent(NodeId leaderId) {
            for (; m_searchStartIndex < m_messages.length; m_searchStartIndex++){
                if (m_messages[m_searchStartIndex].type == Message.Type.RaftAppendEntries &&
                    m_messages[m_searchStartIndex].srcId == leaderId &&
                    m_messages[m_searchStartIndex].content["subtype"].get!string == "heartbeat"){
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

    void _raftLeaderStepUp(S, C)(ref S servers, ref C clients, NodeId wantedLeaderIdx) {
        import core.thread;
        Thread.sleep(RaftNode.MAX_ELECTION_TIMEOUT.seconds);

        // make sure expectedLeader starts election
        servers[wantedLeaderIdx].runRaftIteration();
        // handle vote requests
        foreach (i; 0..servers.length){
            if (i != wantedLeaderIdx){
                servers[i].runHandleMessageOnce();
            }
        }
        // handle vote responses
        foreach (i; 1..servers.length){
            servers[wantedLeaderIdx].runHandleMessageOnce();
        }
        // recv heartbeats from leader
        foreach (i; 0..servers.length){
            if (i != wantedLeaderIdx){
                servers[i].runHandleMessageOnce();
                servers[i].runRaftIteration();
            }
        }
    }

    void _handleAllOngoingMessages(S)(ref S servers) {
        bool run = true;
        while (run) {
            run = false;
            foreach (server; servers){
                run |= server.runHandleMessageOnce();
            }
        }
    }

    void test_happy_raftLeaderStepUp() {
        mixin raftBasicScenarioSetup!(RAFT_NODES, 2);
        enum expectedLeaderIdx = 0;
        
        _raftLeaderStepUp(servers, clients, expectedLeaderIdx);

        assert(tester.checkForLeaderStepUp(SERVER_IDS[expectedLeaderIdx]), "Leader step up failed");
    }

    void test_leaderCrash_newStepUp() {
        mixin raftBasicScenarioSetup!(RAFT_NODES, 2);
        enum firstExpectedLeaderIdx = 0;
        enum secondExpectedLeaderIdx = 1;
        
        _raftLeaderStepUp(servers, clients, firstExpectedLeaderIdx);
        assert(tester.checkForLeaderStepUp(SERVER_IDS[firstExpectedLeaderIdx]), "First leader step up failed");

        // crash leader
        auto serversNoLeader = servers[0 .. firstExpectedLeaderIdx] ~ servers[firstExpectedLeaderIdx + 1 .. $];
        _raftLeaderStepUp(serversNoLeader, clients, secondExpectedLeaderIdx-1);// -1 because of removed server
        assert(tester.checkForLeaderStepUp(SERVER_IDS[secondExpectedLeaderIdx]), "Second leader step up failed");
    }

    void test_noQuorom_noLeaderStepUp(){
        mixin raftBasicScenarioSetup!(RAFT_NODES, 2);
        enum firstExpectedLeaderIdx = 0;
        enum secondExpectedLeaderIdx = 1;
        
        _raftLeaderStepUp(servers, clients, firstExpectedLeaderIdx);
        assert(tester.checkForLeaderStepUp(SERVER_IDS[firstExpectedLeaderIdx]), "First leader step up failed");

        // crash 3 servers
        auto serversNoQuorom = servers[firstExpectedLeaderIdx + 1 .. firstExpectedLeaderIdx + 3];
        _raftLeaderStepUp(serversNoQuorom, clients, secondExpectedLeaderIdx-1);// -1 because of removed server
        assert(false == tester.checkForLeaderStepUp(SERVER_IDS[secondExpectedLeaderIdx]), "Second leader step up occured UNEXPECTEDLY");
    }

    void test_happy_appendEntry() {
        mixin raftBasicScenarioSetup!(RAFT_NODES, 2);
        enum expectedLeaderIdx = 0;
        
        _raftLeaderStepUp(servers, clients, expectedLeaderIdx);
        assert(tester.checkForLeaderStepUp(SERVER_IDS[expectedLeaderIdx]), "Leader step up failed");
        _handleAllOngoingMessages(servers);

        enum text = "Hello, World!";
        clients[0].send(text);

        // leader will get request, distribute to followers which will send response
        // leader will get responses and send commit
        _handleAllOngoingMessages(servers);

        assert(clients[0].recv() == text, "Client did not receive appropriate response");
    }

    void test_noQuorom_appendEntry() {
        mixin raftBasicScenarioSetup!(RAFT_NODES, 2);
        enum expectedLeaderIdx = 0;
        
        _raftLeaderStepUp(servers, clients, expectedLeaderIdx);
        assert(tester.checkForLeaderStepUp(SERVER_IDS[expectedLeaderIdx]), "Leader step up failed");
        _handleAllOngoingMessages(servers);

        enum text = "Hello, World!";
        clients[0].send(text);

        // leader will get request, distribute to NOT ENOUGH followers which will send response
        foreach (server; servers[expectedLeaderIdx .. expectedLeaderIdx + 2]){
            server.runHandleMessageOnce();
        }

        // leader will get responses and send commit
        foreach (i; 0..servers.length){
            servers[expectedLeaderIdx].runHandleMessageOnce();
        }

        assert(clients[0].recv() == null, "Client received response when shouldn't have.");
    }

    void test_differentLeader_appendEntry() {
        mixin raftBasicScenarioSetup!(RAFT_NODES, 2);
        enum expectedLeaderIdx = 1;
        
        _raftLeaderStepUp(servers, clients, expectedLeaderIdx);
        assert(tester.checkForLeaderStepUp(SERVER_IDS[expectedLeaderIdx]), "Leader step up failed");
        _handleAllOngoingMessages(servers);

        enum text = "Hello, World!";
        clients[0].send(text);// sends to server ID[0] as default which isn't leader

        // leader will get request, distribute to followers which will send response
        // then leader will get responses and send commit
        _handleAllOngoingMessages(servers);

        assert(clients[0].recv() == text, "Client did not receive appropriate response");
    }

    void testMain() {
        // leadership tests
        writeln("\033[32m test_happy_raftLeaderStepUp\033[0m");
        test_happy_raftLeaderStepUp();
        writeln("\033[32m test_leaderCrash_newStepUp\033[0m");
        test_leaderCrash_newStepUp();
        writeln("\033[32m test_noQuorom_noLeaderStepUp\033[0m");
        test_noQuorom_noLeaderStepUp();

        // entries tests
        writeln("\033[32m test_happy_appendEntry\033[0m");
        test_happy_appendEntry();
        writeln("\033[32m test_noQuorom_appendEntry\033[0m");
        test_noQuorom_appendEntry();
        writeln("\033[32m test_differentLeader_appendEntry\033[0m");
        test_differentLeader_appendEntry();

        // PBFT testss 

        // Byzantine tests

        
    }

    testMain();
}

void main(){
    writeln("Hello, World!");
    writeln("Note: the program is meant to run tests only for now. The adjustment
    to run as actual chat servers require different communication setup ONLY.");
    // mixin raftBasicScenarioSetup!(RAFT_NODES, 2);

    // Fiber[RAFT_NODES] serverFibers;
    // serverFibers[0] = new Fiber({ servers[0].run(); });
    // serverFibers[1] = new Fiber({ servers[1].run(); });
    // serverFibers[2] = new Fiber({ servers[2].run(); });
    // serverFibers[3] = new Fiber({ servers[3].run(); });
    // serverFibers[4] = new Fiber({ servers[4].run(); });

    // // run servers in random order to simulate asynchronous behavior
    // import std.random;
    // int[] runOrder = [0, 1, 2, 3, 4];
    // while (true){
    //     runOrder.randomShuffle();
    //     foreach (i; runOrder){
    //         serverFibers[i].call();
    //     }
    // }
    // // Also need to run clients...
}