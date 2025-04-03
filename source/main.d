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
        private bool[NodeId] m_blocked;

        this(immutable NodeId[] servers, immutable NodeId[] clients){
            super(servers ~ clients);
            m_servers = servers;
            m_clients = clients;
            m_searchStartIndex = 0;
            foreach (server; servers){
                m_blocked[server] = false;
            }
        }

        override bool recv(ref Message msg) {
            bool res = super.recv(msg);
            Message recvMsgCopy = msg;
            m_messages ~= recvMsgCopy;
            return res;
        }

        override bool send(Message msg) {
            if (msg.dstId in m_blocked && m_blocked[msg.dstId]){
                return true;
            }
            m_messages ~= msg;
            return super.send(msg);
        }

        void block(NodeId target) {
            m_blocked[target] = true;
        }

        void unblock(NodeId target) {
            m_blocked[target] = false;
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
    mixin template raftBasicScenarioSetup(S, C, const uint serversAmount, const uint clientsAmount) {
        enum NodeId[serversAmount] SERVER_IDS = [1, 2, 3, 4, 5];
        enum NodeId[clientsAmount] CLIENT_IDS = [6, 7];
        
        RaftTesterCommunicator tester = new RaftTesterCommunicator(SERVER_IDS, CLIENT_IDS);
        
        LocalServerNode!S [serversAmount] servers = [
            new LocalServerNode!S(SERVER_IDS[0], SERVER_IDS, tester),
            new LocalServerNode!S(SERVER_IDS[1], SERVER_IDS, tester),
            new LocalServerNode!S(SERVER_IDS[2], SERVER_IDS, tester),
            new LocalServerNode!S(SERVER_IDS[3], SERVER_IDS, tester),
            new LocalServerNode!S(SERVER_IDS[4], SERVER_IDS, tester)];
        
        C[clientsAmount] clients = [
            new C(CLIENT_IDS[0], tester),
            new C(CLIENT_IDS[1], tester)];
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
        _handleAllOngoingMessages(servers);
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

    void test_happy_raftLeaderStepUp(S, C)() {
        mixin raftBasicScenarioSetup!(S, C, RAFT_NODES, 2);
        enum expectedLeaderIdx = 0;
        
        _raftLeaderStepUp(servers, clients, expectedLeaderIdx);

        assert(tester.checkForLeaderStepUp(SERVER_IDS[expectedLeaderIdx]), "Leader step up failed");
    }

    void test_leaderCrash_newStepUp(S, C)() {
        mixin raftBasicScenarioSetup!(S, C, RAFT_NODES, 2);
        enum firstExpectedLeaderIdx = 0;
        enum secondExpectedLeaderIdx = 1;
        
        _raftLeaderStepUp(servers, clients, firstExpectedLeaderIdx);
        assert(tester.checkForLeaderStepUp(SERVER_IDS[firstExpectedLeaderIdx]), "First leader step up failed");

        // crash leader
        auto serversNoLeader = servers[0 .. firstExpectedLeaderIdx] ~ servers[firstExpectedLeaderIdx + 1 .. $];
        _raftLeaderStepUp(serversNoLeader, clients, secondExpectedLeaderIdx-1);// -1 because of removed server
        assert(tester.checkForLeaderStepUp(SERVER_IDS[secondExpectedLeaderIdx]), "Second leader step up failed");
    }

    void test_noQuorom_noLeaderStepUp(S, C)(){
        mixin raftBasicScenarioSetup!(S, C, RAFT_NODES, 2);
        enum firstExpectedLeaderIdx = 0;
        enum secondExpectedLeaderIdx = 1;
        
        _raftLeaderStepUp(servers, clients, firstExpectedLeaderIdx);
        assert(tester.checkForLeaderStepUp(SERVER_IDS[firstExpectedLeaderIdx]), "First leader step up failed");

        // crash 3 servers
        auto serversNoQuorom = servers[firstExpectedLeaderIdx + 1 .. firstExpectedLeaderIdx + 3];
        _raftLeaderStepUp(serversNoQuorom, clients, secondExpectedLeaderIdx-1);// -1 because of removed server
        assert(false == tester.checkForLeaderStepUp(SERVER_IDS[secondExpectedLeaderIdx]), "Second leader step up occured UNEXPECTEDLY");
    }

    void test_happy_appendEntry(S, C)() {
        mixin raftBasicScenarioSetup!(S, C, RAFT_NODES, 2);
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

    void test_noQuorom_appendEntry(S, C)() {
        mixin raftBasicScenarioSetup!(S, C, RAFT_NODES, 2);
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

    void test_differentLeader_appendEntry(S, C)() {
        mixin raftBasicScenarioSetup!(S, C, RAFT_NODES, 2);
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

    void raftTestMain() {
        // leadership tests
        writeln("\033[32m test_happy_raftLeaderStepUp\033[0m");
        test_happy_raftLeaderStepUp!(RaftNode, RaftClientNode)();
        writeln("\033[32m test_leaderCrash_newStepUp\033[0m");
        test_leaderCrash_newStepUp!(RaftNode, RaftClientNode)();
        writeln("\033[32m test_noQuorom_noLeaderStepUp\033[0m");
        test_noQuorom_noLeaderStepUp!(RaftNode, RaftClientNode)();

        // entries tests
        writeln("\033[32m test_happy_appendEntry\033[0m");
        test_happy_appendEntry!(RaftNode, RaftClientNode)();
        writeln("\033[32m test_noQuorom_appendEntry\033[0m");
        test_noQuorom_appendEntry!(RaftNode, RaftClientNode)();
        writeln("\033[32m test_differentLeader_appendEntry\033[0m");
        test_differentLeader_appendEntry!(RaftNode, RaftClientNode)();
    }

    void test_splitBrainLeader_appendEntry(S, C)() {
        enum NodeId[PBFT_NODES] SERVER_IDS = [1, 2, 3, 4];
        enum NodeId[] CLIENT_IDS = [6, 7];
        
        RaftTesterCommunicator tester = new RaftTesterCommunicator(SERVER_IDS, CLIENT_IDS);
        
        LocalServerNode!S [PBFT_NODES] servers = [
            new LocalServerNode!S(SERVER_IDS[0], SERVER_IDS, tester),
            new LocalServerNode!S(SERVER_IDS[1], SERVER_IDS, tester),
            new LocalServerNode!S(SERVER_IDS[2], SERVER_IDS, tester),
            new LocalServerNode!S(SERVER_IDS[3], SERVER_IDS, tester)];
        
        // LocalServerNode!S splitServerPartOne = new LocalServerNode!S(SERVER_IDS[0], SERVER_IDS[0..2], tester);
        // LocalServerNode!S splitServerPartTwo = new LocalServerNode!S(SERVER_IDS[0], SERVER_IDS[2..4], tester);

        C[] clients = [
            new C(CLIENT_IDS[0], tester),
            new C(CLIENT_IDS[1], tester)];


        enum expectedLeaderIdx = 0;
        
        _raftLeaderStepUp(servers, clients, expectedLeaderIdx);
        assert(tester.checkForLeaderStepUp(SERVER_IDS[expectedLeaderIdx]), "Leader step up failed");
        _handleAllOngoingMessages(servers);

        enum text = "Hello, World!";
        clients[0].send(text);// sends to server ID[0] as default which isn't leader

        // split one will try "Hello World"
        tester.block(SERVER_IDS[2]);
        tester.block(SERVER_IDS[3]);
        servers[0].runHandleMessageOnce();

        // split two will try "Bye Bye"
        auto trailCopy = tester.m_memoryMap[SERVER_IDS[1]].back.uniqueIdTrail;
        tester.block(SERVER_IDS[1]);
        tester.unblock(SERVER_IDS[2]);
        tester.unblock(SERVER_IDS[3]);
        clients[0].send("Bye Bye!");
        servers[0].runHandleMessageOnce();
        tester.m_memoryMap[SERVER_IDS[2]].back.uniqueIdTrail = trailCopy;
        tester.m_memoryMap[SERVER_IDS[3]].back.uniqueIdTrail = trailCopy;

        tester.unblock(SERVER_IDS[1]);
        // leader will get request, distribute to followers which will send response
        // then leader will get responses and send commit
        _handleAllOngoingMessages(servers);

        assert(clients[0].recv() == null, "Client received response while leader acts BYZANTINE");
    }

    void pbftTestMain() {
        // leadership tests
        writeln("\033[32m test_happy_raftLeaderStepUp\033[0m");
        test_happy_raftLeaderStepUp!(RaftMixedPBFTNode, PbftClientNode)();
        writeln("\033[32m test_leaderCrash_newStepUp\033[0m");
        test_leaderCrash_newStepUp!(RaftMixedPBFTNode, PbftClientNode)();
        writeln("\033[32m test_noQuorom_noLeaderStepUp\033[0m");
        test_noQuorom_noLeaderStepUp!(RaftMixedPBFTNode, PbftClientNode)();

        // entries tests
        writeln("\033[32m test_happy_appendEntry\033[0m");
        test_happy_appendEntry!(RaftMixedPBFTNode, PbftClientNode)();
        writeln("\033[32m test_noQuorom_appendEntry\033[0m");
        test_noQuorom_appendEntry!(RaftMixedPBFTNode, PbftClientNode)();
        writeln("\033[32m test_differentLeader_appendEntry\033[0m");
        test_differentLeader_appendEntry!(RaftMixedPBFTNode, PbftClientNode)();

        // Byzantine leader
        writeln("\033[32m test_splitBrainLeader_appendEntry\033[0m");
        test_splitBrainLeader_appendEntry!(RaftMixedPBFTNode, PbftClientNode)();
    }

    raftTestMain();
    pbftTestMain();
}

void main(){
    writeln("Hello, World!");
    writeln("Note: the program is meant to run tests only for now. The adjustment
    to run as actual chat servers require different communication setup ONLY.");
}
