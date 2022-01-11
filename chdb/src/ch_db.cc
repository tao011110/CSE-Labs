#include "ch_db.h"

int view_server::execute(unsigned int query_key, unsigned int proc, const chdb_protocol::operation_var &var, int &r) {
    // TODO: Your code here
    int base_port = this->node->port();
    int shard_offset = this->dispatch(query_key, shard_num());
    chdb_command::command_type tp;
    if(proc == chdb_protocol::Put){
        tp = chdb_command::CMD_PUT;
    }
    else{
        if(proc == chdb_protocol::Get){
            tp = chdb_command::CMD_GET;
        }
        else{
            assert("wrong command_type!!");
        }
    }

    int key = var.key;
    int value = var.value;
    int tx_id = var.tx_id;

    chdb_command ch_command(tp, key, value, tx_id);
    command_logs.push_back(ch_command);
    int leader = raft_group->check_exact_one_leader();
    int term, index;
    raft_group->nodes[leader]->new_command(ch_command, term, index);

    return this->node->template call(base_port + shard_offset, proc, var, r);
}

int view_server::execute_prepare(unsigned int query_key, unsigned int proc, const chdb_protocol::prepare_var &var, int &r) {
    int base_port = this->node->port();
    int shard_offset = this->dispatch(query_key, shard_num());

    return this->node->template call(base_port + shard_offset, proc, var, r);
}

int view_server::execute_rollback(unsigned int query_key, unsigned int proc, const chdb_protocol::rollback_var &var, int &r) {
    int base_port = this->node->port();
    int shard_offset = this->dispatch(query_key, shard_num());

    return this->node->template call(base_port + shard_offset, proc, var, r);
}

int view_server::execute_check(unsigned int query_key, unsigned int proc, const chdb_protocol::check_prepare_state_var &var, int &r) {
    int base_port = this->node->port();
    int shard_offset = this->dispatch(query_key, shard_num());

    return this->node->template call(base_port + shard_offset, proc, var, r);
}

int view_server::execute_commit(unsigned int query_key, unsigned int proc, const chdb_protocol::commit_var &var, int &r) {
    int base_port = this->node->port();
    int shard_offset = this->dispatch(query_key, shard_num());
    // for(; next_commit < command_logs.size(); next_commit++){
        
    // }

    return this->node->template call(base_port + shard_offset, proc, var, r);
}

view_server::~view_server() {
#if RAFT_GROUP
    delete this->raft_group;
#endif
    delete this->node;

}