#ifndef raft_h
#define raft_h

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <stdarg.h>
#include <vector>
#include <iostream>

#include "rpc.h"
#include "raft_storage.h"
#include "raft_protocol.h"
#include "raft_state_machine.h"

template<typename state_machine, typename command>
class raft {

static_assert(std::is_base_of<raft_state_machine, state_machine>(), "state_machine must inherit from raft_state_machine");
static_assert(std::is_base_of<raft_command, command>(), "command must inherit from raft_command");


friend class thread_pool;

#define RAFT_LOG(fmt, args...) \
    do { \
        auto now = \
        std::chrono::duration_cast<std::chrono::milliseconds>(\
            std::chrono::system_clock::now().time_since_epoch()\
        ).count();\
        printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, ##args); \
    } while(0);

public:
    raft(
        rpcs* rpc_server,
        std::vector<rpcc*> rpc_clients,
        int idx, 
        raft_storage<command>* storage,
        state_machine* state    
    );
    ~raft();

    // start the raft node.
    // Please make sure all of the rpc request handlers have been registered before this method.
    void start();

    // stop the raft node. 
    // Please make sure all of the background threads are joined in this method.
    // Notice: you should check whether is server should be stopped by calling is_stopped(). 
    //         Once it returns true, you should break all of your long-running loops in the background threads.
    void stop();

    // send a new command to the raft nodes.
    // This method returns true if this raft node is the leader that successfully appends the log.
    // If this node is not the leader, returns false. 
    bool new_command(command cmd, int &term, int &index);

    // returns whether this node is the leader, you should also set the current term;
    bool is_leader(int &term);

    // save a snapshot of the state machine and compact the log.
    bool save_snapshot();

private:
    std::mutex mtx;                     // A big lock to protect the whole data structure
    ThrPool* thread_pool;
    raft_storage<command>* storage;              // To persist the raft log
    state_machine* state;  // The state machine that applies the raft log, e.g. a kv store

    rpcs* rpc_server;               // RPC server to recieve and handle the RPC requests
    std::vector<rpcc*> rpc_clients; // RPC clients of all raft nodes including this node
    int my_id;                     // The index of this node in rpc_clients, start from 0

    std::atomic_bool stopped;

    enum raft_role {
        follower,
        candidate,
        leader
    };
    raft_role role;
    int current_term;

    std::thread* background_election;
    std::thread* background_ping;
    std::thread* background_commit;
    std::thread* background_apply;

    // Your code here:
    int votedFor = -1;              //candidateId that received vote in current term
    int commitIndex = 0;            //index of highest log entry known to becommitted
    int lastApplied = 0;            //index of highest log entry applied to state machine
    int votedMe = 0;
    unsigned long timeout = 0;
    unsigned long last_received_RPC_time;
    std::vector<log_entry<command>> logs;

    //Volatile state on leaders
    std::vector<int> nextIndex;          //for each server, index of the next log entry to send 
                                         //to that server (initialized to leader last log index + 1)

    std::vector<int> matchIndex;         //for each server, index of highest log entry known to be 
                                         //replicated on server(initialized to 0, increases monotonically)

private:
    // RPC handlers
    int request_vote(request_vote_args arg, request_vote_reply& reply);

    int append_entries(append_entries_args<command> arg, append_entries_reply& reply);

    int install_snapshot(install_snapshot_args arg, install_snapshot_reply& reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);
    void handle_request_vote_reply(int target, const request_vote_args& arg, const request_vote_reply& reply);

    void send_append_entries(int target, append_entries_args<command> arg);
    void handle_append_entries_reply(int target, const append_entries_args<command>& arg, const append_entries_reply& reply);

    void send_install_snapshot(int target, install_snapshot_args arg);
    void handle_install_snapshot_reply(int target, const install_snapshot_args& arg, const install_snapshot_reply& reply);

    unsigned long get_current_time();

private:
    bool is_stopped();
    int num_nodes() {return rpc_clients.size();}

    // background workers    
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    // Your code here:
    void start_election();

};

template<typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs* server, std::vector<rpcc*> clients, int idx, raft_storage<command> *storage, state_machine *state) :
    storage(storage),
    state(state),   
    rpc_server(server),
    rpc_clients(clients),
    my_id(idx),
    stopped(false),
    role(follower),
    current_term(0),
    background_election(nullptr),
    background_ping(nullptr),
    background_commit(nullptr),
    background_apply(nullptr)
{
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    // Your code here: 
    // Do the initialization
    votedFor = -1;
    commitIndex = 0;            
    lastApplied = 0;            
    votedMe = 0;
    last_received_RPC_time = get_current_time();
    srand((unsigned)time(NULL));

    std::string dir = std::string("raft_temp/raft_storage_") + std::to_string(my_id);
    RAFT_LOG("dir  %s",dir.c_str());
    storage = new raft_storage<command>(dir);
    // storage->append_votedFor(votedFor);
    // storage->append_term(current_term);
    RAFT_LOG("finish fir");
}

template<typename state_machine, typename command>
raft<state_machine, command>::~raft() {
    RAFT_LOG("~raft");
    // storage->~raft_storage();
    if (background_ping) {
        delete background_ping;
    }
    if (background_election) {
        delete background_election;
    }
    if (background_commit) {
        delete background_commit;
    }
    if (background_apply) {
        delete background_apply;
    }
    delete thread_pool;    
}

/******************************************************************

                        Public Interfaces

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::stop() {
    RAFT_LOG("stop");
    stopped.store(true);
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped() {
    return stopped.load();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term) {
    term = current_term;
    if(role == leader){
        RAFT_LOG("is a leader!");
    }
    else{
        RAFT_LOG("not a leader!")
    }
    return role == leader;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::start() {
    // Your code here:
    // srand((unsigned)time(NULL));
    RAFT_LOG("start");
    logs.clear();
    log_entry<command> l;
    l.term = 0;
    l.index = 0;
    logs.push_back(l);

    std::vector<log_entry<command>> vec;
    storage->find_meta(current_term, votedFor);
    storage->append_meta(current_term, votedFor);
    storage->find_log(vec);
    if(vec.size() != 0){
        logs.insert(logs.end(), vec.begin(), vec.end());
    }
    RAFT_LOG("I recover with log size %d", (int)(logs.size()));
    int total = num_nodes();
    int next = logs[logs.size() - 1].index + 1;
    for(int i = 0; i < total; i++){
        nextIndex.push_back(next);
        matchIndex.push_back(0);
    }
    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
    // Your code here:
    mtx.lock();
    if(role != raft_role::leader){
        mtx.unlock();
        return false;
    }
    // if(logs.size() == 1){
    //     index = logs.size();
    //     term = current_term;
    //     RAFT_LOG("now add new command with index is %d!", index);
    //     log_entry<command> log;
    //     log.commandLog = cmd;
    //     log.index = index;
    //     log.term = current_term;
    //     logs.push_back(log);
    // }
    // else{
    //     if(cmd.value != (logs[logs.size()-1].commandLog.value)){
    //         index = logs.size();
    //         term = current_term;
    //         RAFT_LOG("now add new command with index is %d!", index);
    //         log_entry<command> log;
    //         log.commandLog = cmd;
    //         log.index = index;
    //         log.term = current_term;
    //         logs.push_back(log);
    //     }
    //     else{
    //         if(commitIndex < (int)(logs.size() - 1)){
    //             index = logs[logs.size()-1].index;
    //             term = current_term;
    //             if(current_term > logs[logs.size()-1].term){
    //                 logs.pop_back();
    //                 RAFT_LOG("after pop, now add new command with index is %d!", index);
    //                 log_entry<command> log;
    //                 log.commandLog = cmd;
    //                 log.index = index;
    //                 log.term = current_term;
    //                 logs.push_back(log);
    //             }
    //             // term = logs[logs.size()-1].term;
    //             // index = logs[logs.size()-1].index;
    //             else{
    //                 RAFT_LOG("not add command because it has this as old leader!");
    //             }
    //             int total = num_nodes();
    //             for(int i = 0; i < total; i++){
    //                 if(i == my_id){
    //                     continue;
    //                 }
    //                 if(nextIndex[i] > 1){
    //                     nextIndex[i] = nextIndex[i] - 1;
    //                     RAFT_LOG("change the nextIndex[%d] into %d", i, nextIndex[i])
    //                 }
    //             }
    //         }
    //         else{
    //             index = logs.size();
    //             term = current_term;
    //             RAFT_LOG("although same, now add new command with index is %d!", index);
    //             log_entry<command> log;
    //             log.commandLog = cmd;
    //             log.index = index;
    //             log.term = current_term;
    //             logs.push_back(log);
    //         }
    //     }
    // }
    index = logs.size();
    term = current_term;
    RAFT_LOG("now add new command with index is %d!", index);
    log_entry<command> log;
    log.commandLog = cmd;
    log.index = index;
    log.term = current_term;
    logs.push_back(log);
    RAFT_LOG("my total logs are %d", (int)(logs.size()));

            // index = logs.size();
            // term = current_term;
            // if(logs.size() > 1){
            //     if(logs[index - 1].commandLog.value == cmd.value){
            //         RAFT_LOG("OK to add same like old");
            //     }
            // }
            // RAFT_LOG("now add new command with index is %d!", index);
            // log_entry<command> log;
            // log.commandLog = cmd;
            // log.index = index;
            // log.term = current_term;
            // logs.push_back(log);
    
    matchIndex[my_id] = index;
    std::vector<log_entry<command>> vec;
    vec.insert(vec.end(), logs.begin() + 1, logs.end());
    storage->append_log(vec);
    mtx.unlock();

    return true;
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
    // Your code here:
    return true;
}



/******************************************************************

                         RPC Related

*******************************************************************/
template<typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply& reply) {
    // std::unique_lock<std::mutex> lck(mtx);
    // lck.lock();
    mtx.lock();
    reply.voteGranted = false;
    reply.term = args.term;
    bool isChangedTerm = false;
    bool isChangedVote = false;

    if(args.term == current_term){
        int logSize = logs.size();
        if(votedFor == -1 || votedFor == args.candidateId){
            if(logSize == 1 || args.lastLogTerm > logs[logSize - 1].term
                    || (args.lastLogTerm == logs[logSize - 1].term && args.lastLogIndex >= logs[logSize - 1].index)){
                    // RAFT_LOG("now I vote for %d", args.candidateId);
                    if(logSize == 1){
                        RAFT_LOG("I vote because logSize == 1");
                    }
                    if(votedFor == args.candidateId){
                        RAFT_LOG("I vote because votedFor == args.candidateId");
                    }
                    if(args.lastLogTerm == logs[logSize - 1].term && args.lastLogIndex >= logs[logSize - 1].index){
                        RAFT_LOG("args.lastLogTerm == logs[logSize - 1].term && args.lastLogIndex >= logs[logSize - 1].index");
                    }
                    votedFor = args.candidateId;
                    // role = raft_role::follower;
                    // storage->append_votedFor(votedFor);
                    reply.voteGranted = true;
                    isChangedVote = true;
            }
        }
        else{
            RAFT_LOG("I'm not here, because votedFor = %d   and the args.term is %d", votedFor, args.term);
        }
    }
    else{
        if(args.term > current_term){
            // last_received_RPC_time = get_current_time();
            current_term = args.term;
            isChangedTerm = true;
            role = raft_role::follower;
            // int range = 200 / num_nodes();
            // srand((unsigned)time(NULL));
            // timeout = 300 + my_id * range + rand() % (range + 1);
            timeout = 300 + rand() % 201;
            int logSize = logs.size();
            if(logSize == 1 || args.lastLogTerm > logs[logSize - 1].term
                    || (args.lastLogTerm == logs[logSize - 1].term && args.lastLogIndex >= logs[logSize - 1].index)){
                        RAFT_LOG("I vote because args.term > current_term")
                    if(logSize == 1){
                        RAFT_LOG("I vote because logSize == 1");
                    }
                    if(args.lastLogTerm > logs[logSize - 1].term){
                        RAFT_LOG("I vote because args.lastLogTerm %d > logs[logSize - 1].term %d", args.lastLogTerm, logs[logSize - 1].term);
                    }
                    if(args.lastLogTerm == logs[logSize - 1].term && args.lastLogIndex >= logs[logSize - 1].index){
                        RAFT_LOG("args.lastLogTerm == logs[logSize - 1].term && args.lastLogIndex >= logs[logSize - 1].index");
                    }
                    RAFT_LOG("now I vote for %d", args.candidateId);
                    votedFor = args.candidateId;
                    role = raft_role::follower;
                    // int range = 200 / num_nodes();
                    // srand((unsigned)time(NULL));
                    // timeout = 300 + my_id * range + rand() % (range + 1);
                    timeout = 300 + rand() % 201;
                    // storage->append_votedFor(votedFor);
                    reply.voteGranted = true;
                    isChangedVote = true;

                    if(role == raft_role::follower){
                        last_received_RPC_time = get_current_time();
                    }
            }
        }
    }
    if(isChangedTerm || isChangedVote){
        storage->append_meta(current_term, votedFor);
        int t = 0, v = 0;
        storage->find_meta(t, v);
        RAFT_LOG("now we try to find meta with %d and %d", t, v);
    }
    
    mtx.unlock();

    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args& arg, const request_vote_reply& reply) {
    // Your code here:
    // std::unique_lock<std::mutex> lck(mtx);
    // lck.lock();
    mtx.lock();
    bool isChangedTerm = false;
    bool isChangedVote = false;
    int total = num_nodes();
    RAFT_LOG("follow %d vote for %d", target, reply.voteGranted);
    if(reply.term > current_term){
        current_term = reply.term;
        role = raft_role::follower;
        // int range = 200 / num_nodes();
        // srand((unsigned)time(NULL));
        // timeout = 300 + my_id * range + rand() % (range + 1);
        timeout = 300 + rand() % 201;
        RAFT_LOG("I am no longer leader!");
        isChangedTerm = true;
        // storage->append_term(current_term);
    }
    if(role == raft_role::candidate){
        if(reply.voteGranted == true){
            votedMe++;
            if(votedMe >= (total + 1) / 2){
                role = leader;
                RAFT_LOG("      I am leader!!   and %d followers vote me!", votedMe);
                RAFT_LOG(" total is %d", total);
                votedMe = 0;
                votedFor = -1;
                isChangedVote = true;

                int next = logs[logs.size() - 1].index + 1;
                for(int i = 0; i < total; i++){
                    nextIndex[i] = next;
                    matchIndex[i] = 0;
                }
                matchIndex[my_id] = logs[logs.size() - 1].index;
            }
        }
    }
    // last_received_RPC_time = get_current_time();
    if(isChangedVote || isChangedTerm){
        storage->append_meta(current_term, votedFor);
    }
    mtx.unlock();
    // lck.unlock();

    return;
}


template<typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply& reply) {
    // Your code here:
    // std::unique_lock<std::mutex> lck(mtx);
    // lck.lock();
    mtx.lock();
    RAFT_LOG("get append ask  from leader  %d!  and i am a  %d", arg.leaderId, role);
    reply.success = true;
    reply.term = current_term;
    bool isChangedTerm = false;
    bool isChangedVote = false;

    if(arg.term < current_term){
        reply.success = false;
        RAFT_LOG("arg.term is  %d   but  my term is  %d", arg.term, current_term);
    }
    else{
        // storage->append_term(current_term);
        if(arg.term > current_term){
            if(role == raft_role::leader){
                RAFT_LOG("I am no longer leader!");
            }
            role = raft_role::follower;
            // int range = 200 / num_nodes();
            // srand((unsigned)time(NULL));
            // timeout = 300 + my_id * range + rand() % (range + 1);
            timeout = 300 + rand() % 201;
            current_term = arg.term;
            isChangedTerm = true;
            storage->append_meta(current_term, votedFor);
        }
        
        int size = logs.size();
        if(arg.leaderCommit > commitIndex){
            if(logs[size - 1].term == current_term){
                commitIndex = std::min(arg.leaderCommit, size - 1);
                RAFT_LOG("follower  change commit index into  %d", commitIndex);
            }
            else{
                RAFT_LOG("logs[size - 1].term is %d", logs[size - 1].term);
            }
        }
        if(arg.isHeartBeat){
            RAFT_LOG("I know  %d leader is alive!", arg.leaderId);
            if(role == raft_role::candidate){
                role = raft_role::follower;
                // int range = 200 / num_nodes();
                // srand((unsigned)time(NULL));
                // timeout = 300 + my_id * range + rand() % (range + 1);
                timeout = 300 + rand() % 201;
            }
            if(role == raft_role::leader){
                RAFT_LOG("%d  told me, I am no longer leader!", arg.leaderId);
                role = raft_role::follower;
                // int range = 200 / num_nodes();
                // srand((unsigned)time(NULL));
                // timeout = 300 + my_id * range + rand() % (range + 1);
                timeout = 300 + rand() % 201;
            }
            if(votedFor != -1){
                votedFor = -1;
                isChangedVote = true;
            }
            // storage->append_votedFor(votedFor);
        }
        else{
            
            RAFT_LOG("%d  told me to appendddddddddddddddddddddddddddddddddddddddddddd", arg.leaderId);

            // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
            if(size - 1 < arg.prevLogIndex || (arg.prevLogIndex > 0 && logs[arg.prevLogIndex].term != arg.prevLogTerm)){
                RAFT_LOG("arg.prevLogIndex is  %d", arg.prevLogIndex);
                reply.success = false;
            }
            else{
                //handle the inconsistence
                if(arg.prevLogIndex + 1 <= 0){
                    logs.erase(logs.begin(), logs.end());
                    std::vector<class log_entry<command>>(logs).swap(logs);
                }
                else{
                    if(arg.prevLogIndex + 1 < (int)(logs.size())){
                        logs.erase(logs.begin() + arg.prevLogIndex + 1, logs.end());
                        std::vector<class log_entry<command>>(logs).swap(logs);
                    }
                }
                RAFT_LOG("the arg.prevLogIndex is %d", arg.prevLogIndex);
                RAFT_LOG("logs size is %d", (int)(logs.size()));
                for(long unsigned int i = 0; i < arg.entries.size(); i++){
                    logs.push_back(arg.entries[i]);
                }
                RAFT_LOG("old size is %d, and now modify into %d", size, (int)logs.size());
                size = logs.size();
                for(int i = 0; i < size; i++){
                    std::cout << "value is " << logs[i].commandLog.value << "  term is  " << logs[i].term << std::endl;
                }

                std::vector<log_entry<command>> vec;
                vec.insert(vec.end(), logs.begin() + 1, logs.end());
                storage->append_log(vec);
            }
        }
    }
    last_received_RPC_time = get_current_time();
    if(isChangedVote || isChangedTerm){
        storage->append_meta(current_term, votedFor);
    }
    RAFT_LOG("and now my last_received_RPC_time is  %d", (int)last_received_RPC_time);
    mtx.unlock();
    // lck.unlock();

    return 0;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int target, const append_entries_args<command>& arg, const append_entries_reply& reply) {
    // Your code here:
    // std::unique_lock<std::mutex> lck(mtx);
    // lck.lock();
    mtx.lock();
    if(role != raft_role::leader){
        mtx.unlock();
        return;
    }
    RAFT_LOG("I got reply for append from %d  with reply term   %d!", target, reply.term);
    if(reply.term > current_term){
        if(role == raft_role::leader){
            RAFT_LOG("I am no longer leader!");
        }
        current_term = reply.term;
        // storage->append_term(current_term);
        role = raft_role::follower;
        // int range = 200 / num_nodes();
        // srand((unsigned)time(NULL));
        // timeout = 300 + my_id * range + rand() % (range + 1);
        timeout = 300 + rand() % 201;
        RAFT_LOG("I am no longer leader!");
        RAFT_LOG("failed for term");
        storage->append_meta(current_term, votedFor);
    }
    else{
        // if(role == raft_role::leader){
            if(!arg.isHeartBeat){
                RAFT_LOG("appen not failed for term");
                if(!reply.success){
                    append_entries_args<command> newArg;
                    RAFT_LOG("%d told me not OK!", target);
                    int tmp = nextIndex[target];
                    nextIndex[target] = tmp - 1;
                    
                    // int index = nextIndex[target] - 1;
                    // I dont't know whether it's OK now 
                    // if(index < 0){
                    //     role = raft_role::follower;
                    //     mtx.unlock();
                    //     return;
                    // }
                    // RAFT_LOG("we add log with index  %d  for %d", index, target);
                    // newArg.entries.insert(newArg.entries.begin(), logs.begin() + index, logs.end());
                    // newArg.prevLogIndex = index;
                    // newArg.prevLogTerm = logs[index].term;
                    // newArg.isHeartBeat = false;
                    // newArg.leaderCommit = arg.leaderCommit;
                    // newArg.leaderId = arg.leaderId;
                    // newArg.term = arg.term;

                    // thread_pool->addObjJob(this, &raft::send_append_entries, target, newArg);
                }
                else{
                    RAFT_LOG("%d   told me successpppppppppppppppppppppppppppppp", target);
                    int last = arg.entries[arg.entries.size() - 1].index;
                    nextIndex[target] = last + 1;
                    matchIndex[target] = last;

                }
            }
            else{
                RAFT_LOG("but a heartbeat");
            }
        // }
    }
    // last_received_RPC_time = get_current_time();

    mtx.unlock();

    // lck.unlock();

    return;
}


template<typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply& reply) {
    // Your code here:
    return 0;
}


template<typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int target, const install_snapshot_args& arg, const install_snapshot_reply& reply) {
    // Your code here:
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg) {
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0) {
        handle_request_vote_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg) {
    append_entries_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0) {
        handle_append_entries_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args arg) {
    install_snapshot_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0) {
        handle_install_snapshot_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
unsigned long raft<state_machine, command>::get_current_time()
{
    unsigned long now = std::chrono::duration_cast<std::chrono::milliseconds>
            (std::chrono::system_clock::now().time_since_epoch()).count();
    return now;
}

/******************************************************************

                        Background Workers

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::start_election(){
    RAFT_LOG("start election");
    current_term++;
    // storage->append_term(current_term);
    votedFor = my_id;
    votedMe = 1;
    // storage->append_votedFor(votedFor);
    storage->append_meta(current_term, votedFor);
    role = raft_role::candidate;

    request_vote_args args;
    args.candidateId = my_id;
    args.term = current_term;
    int logSize = logs.size();
    if(logSize == 1){
        args.lastLogIndex = 0;
        args.lastLogTerm = 0;
    }
    else{
        args.lastLogIndex = logSize - 1;
        args.lastLogTerm = logs[logSize - 1].term;
    }
    int total = num_nodes();
    for(int target = 0; target < total; target++){
        if(target == my_id){
            continue;
        }
        // RAFT_LOG("now vote sent to  %d", target);
        thread_pool->addObjJob(this, &raft::send_request_vote, target, args);
    }
    
    storage->append_meta(current_term, votedFor);
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_election() {
    // Check the liveness of the leader.
    // Work for followers and candidates.

    // Hints: You should record the time you received the last RPC.
    //        And in this function, you can compare the current time with it.
    //        For example:
    //        if (current_time - last_received_RPC_time > timeout) start_election();
    //        Actually, the timeout should be different between the follower (e.g. 300-500ms) and the candidate (e.g. 1s).
    // int range = 200 / num_nodes();
    // srand((unsigned)time(NULL));
    // timeout = 300 + my_id * range + rand() % (range + 1);
    timeout = 300 + rand() % 201;
    role = raft_role::follower;
    
    while (true) {
        if (is_stopped()) return;
        // Your code here:
        if(role != raft_role::leader){
            unsigned long current_time = get_current_time();
            if (current_time  - last_received_RPC_time > timeout){
                RAFT_LOG("my current_time is %d, but RPC_time is %d, with timeout is %d", (int)current_time, (int)last_received_RPC_time, (int)timeout);
                if(role == raft_role::follower){
                    // srand((unsigned)time(NULL));
                    timeout = 1000;
                    votedFor = -1;
                    // storage->append_votedFor(votedFor);
                    start_election();
                }
                else{
                    role = raft_role::follower;
                    votedFor = -1;
                    // storage->append_votedFor(votedFor);
                    // int range = 200 / num_nodes();
                    // srand((unsigned)time(NULL));
                    // timeout = 300 + my_id * range + rand() % (range + 1);
                    timeout = 300 + rand() % 201;
                    RAFT_LOG("set timeout as %d", (int)timeout);
                    storage->append_meta(current_term, votedFor);
                }
            }               
        }
        // current_time += 10;            //before or after sleep?
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }    
    

    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit() {
    // Send logs/snapshots to the follower.
    // Only work for the leader.

    // Hints: You should check the leader's last log index and the follower's next log index.        
    
    while (true) {
        if (is_stopped()) return;
        // Your code here:
        if(role == raft_role::leader){
            //find the majority number
            int total = num_nodes();
            std::vector<int> tmp;
            for(int i = 0; i < (int)total; i++){
                tmp.push_back(matchIndex[i]);
                RAFT_LOG("%d  is matched with   %d", i, matchIndex[i]);
            }
            sort(tmp.begin(), tmp.end());
            int N = tmp[total / 2];
            RAFT_LOG("get the majority for log %d", N);
            if(N > commitIndex && logs[N].term == current_term){
                commitIndex = N;
            }
            append_entries_args<command> args;
            args.isHeartBeat = false;
            args.leaderCommit = commitIndex;
            args.leaderId = my_id;
            args.term = current_term;
            int lastIndex = logs.size() - 1;
            // int total = num_nodes();
            for(int target = 0; target < total; target++){
                if(my_id == target){
                    continue;
                }
                RAFT_LOG("%d  and %d   for %d", lastIndex, nextIndex[target], target);
                if(lastIndex >= nextIndex[target]){
                    int index = nextIndex[target] - 1;
                    args.prevLogIndex = index;
                    args.prevLogTerm = logs[index].term;
                    args.entries.erase(args.entries.begin(), args.entries.end());
                    std::vector<class log_entry<command>>().swap(args.entries);
                    args.entries.insert(args.entries.end(), logs.begin() + index + 1, logs.end());
                    int size = logs.size();
                    for(int i = 0; i < size; i++){
                        std::cout << i << "   pppp  " << std::endl;
                        std::cout << i << "   is  my log   " <<  logs[i].commandLog.value << std::endl;
                    }

                    RAFT_LOG("now commit sent to  %d with args.leaderCommit %d", target, args.leaderCommit);
                    thread_pool->addObjJob(this, &raft::send_append_entries, target, args);
                }
                // else{
                //     if(isChangeCommit){
                //         int index = nextIndex[target] - 1;
                //         args.prevLogIndex = index;
                //         args.prevLogTerm = logs[index].term;
                //         args.entries.erase(args.entries.begin(), args.entries.end());
                //         args.entries.insert(args.entries.end(), logs.begin() + index + 1, logs.end());
                //         RAFT_LOG("now commit sent to  %d to update commitIndex", target);
                //         thread_pool->addObjJob(this, &raft::send_append_entries, target, args);  
                //     }
                // }
            }
        }
        
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }    
    
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply() {
    // Apply committed logs the state machine
    // Work for all the nodes.

    // Hints: You should check the commit index and the apply index.
    //        Update the apply index and apply the log if commit_index > apply_index

    
    while (true) {
        if (is_stopped()) return;
        // Your code here:
        this->mtx.lock();
        if(commitIndex > lastApplied){
            for(int i = lastApplied + 1; i <= commitIndex; i++){
                RAFT_LOG("the committed command is at %d", i);
                printf("key is %d\n", logs[i].commandLog.key);
                printf("value is %d\n", logs[i].commandLog.value);
                printf("tp is %d\n", int(logs[i].commandLog.cmd_tp));
                try{
                    state->apply_log(logs[i].commandLog);
                    printf("finishe apply\n");
                }
                catch (std::exception e){
                    std::cout << e.what() << std::endl; 
                }
                printf("finishe try\n");
            }
            lastApplied = commitIndex;
            RAFT_LOG("change the lastApplied into  %d", lastApplied);
        }
        this->mtx.unlock();
        
        
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }    
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {
    // Send empty append_entries RPC to the followers.

    // Only work for the leader.
    
    while (true) {
        if (is_stopped()) return;
        // Your code here:
        if(role == raft_role::leader){
            RAFT_LOG("leader living!!!!!!!");
            int total = num_nodes();
            int logSize = logs.size();
            append_entries_args<command> args;
            args.term = current_term;
            args.leaderId = my_id;
            args.leaderCommit = commitIndex;
            std::vector<log_entry<command>> entries;
            args.entries = entries;
            args.isHeartBeat = true;
            if(logSize == 1){
                args.prevLogIndex = 0;
                args.prevLogTerm = 0;
            }
            else{
                args.prevLogIndex = logSize - 1;
                args.prevLogTerm = logs[logSize - 1].term;
            }

            for(int target = 0; target < total; target++){
                if(target == my_id){
                    continue;
                }
                RAFT_LOG("now ping to %d", target);
                thread_pool->addObjJob(this, &raft::send_append_entries, target, args);
            }
        }

        
        std::this_thread::sleep_for(std::chrono::milliseconds(150)); // Change the timeout here!
    }    
    return;
}


/******************************************************************

                        Other functions

*******************************************************************/



#endif // raft_h