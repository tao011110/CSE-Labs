#include "shard_client.h"


int shard_client::put(chdb_protocol::operation_var var, int &r) {
    // TODO: Your code here
    int tx_id = var.tx_id;
    int key = var.key;
    int value = var.value;
    current_tx_id = var.tx_id;
    r = 1;
    printf("shard %d put in key %d\n", shard_id, key);

    value_entry entry;
    entry.value = value;
    store[primary_replica][key] = entry;

    if(!this->active){
        printf("shard %d is not active\n", shard_id);
        r = 0;
    }

    return 0;
}

int shard_client::get(chdb_protocol::operation_var var, int &r) {
    // TODO: Your code here
    int tx_id = var.tx_id;
    int key = var.key;
    int value = var.value;
    current_tx_id = var.tx_id;

    std::map<int, value_entry> pri_map = get_store();
    r = pri_map[key].value;

    // if(!local_store.count(key)){
    //     printf("shard %d get key %d\n", shard_id, key);
    //     // for(int i = 0; i < (int)store.size(); i++){
    //     //     for(auto it = store[i].begin(); it != store[i].end(); it++){
    //     //         if(it->first == key){
    //     //             value_entry entry = it ->second;
    //     //             r = entry.value;
    //     //             break;
    //     //         }
    //     //     }
    //     // }
    //     std::map<int, value_entry> pri_map = get_store();
    //     r = pri_map[key].value;
    // }
    // else{
    //     printf("shard %d local get key %d\n", shard_id, key);
    //     r = local_store[key].value;
    // }
    printf("%d get the value is %d\n", current_tx_id, r);

    return 0;
}

int shard_client::commit(chdb_protocol::commit_var var, int &r) {
    // TODO: Your code here
    if(!this->active){
        r = -1;
        return 0;
    }
    r = var.tx_id;

    for(int i = 0; i < replica_num; i++){
        if(i == primary_replica){
            continue;
        }
        store[i] = store[primary_replica];
    }
   
    return 0;
}

int shard_client::rollback(chdb_protocol::rollback_var var, int &r) {
    // TODO: Your code here
    // if(!this->active){
    //     r = -1;
    //     return 0;
    // }
    for(int i = 0; i < replica_num; i++){
        if(i != primary_replica){
            store[primary_replica] = store[i];
            break;
        }
    }
    r = var.tx_id;

    return 0;
}

int shard_client::check_prepare_state(chdb_protocol::check_prepare_state_var var, int &r) {
    // TODO: Your code here
    printf("shard %d this->prepared is %d\n", shard_id, this->prepared);
    // if(!this->active){
    //     this->prepared =
    //     return 0;
    // }
    r = this->prepared;

    return 0;
}

int shard_client::prepare(chdb_protocol::prepare_var var, int &r) {
    // TODO: Your code here
    printf("shard %d active is %d\n", shard_id, this->active);
    if(!this->active){
        this->prepared = false;
        return 0;
    }
    this->prepared = true;

    return 0;
}

shard_client::~shard_client() {
    delete node;
}