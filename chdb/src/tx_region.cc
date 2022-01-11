#include "tx_region.h"

void tx_region::restart(){
    printf("do restart\n");
    bool flag = true;
    while(flag) {
        flag = false;
        db->new_mtx.lock();
        std::set<int> tmp;
        for(auto &cm : commands_vec) {
            if(tmp.find(cm.key) != tmp.end() || db->locks[cm.key].mtx->try_lock()) {
                db->locks[cm.key].tx_id = this->tx_id;
                tmp.insert(this->tx_id);
            }
            else if(db->locks[cm.key].tx_id > this->tx_id) {
                db->new_mtx.unlock();
                db->locks[cm.key].mtx->lock();
                db->new_mtx.lock();
                db->locks[cm.key].tx_id = this->tx_id;
                tmp.insert(this->tx_id);
            }
            else{
                flag = true;
                for(auto i : tmp) {
                    db->locks[i].tx_id = -1;
                    db->locks[i].mtx->unlock();
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                break;
            }
        }
        db->new_mtx.unlock();
    }

    for(auto &cm : commands_vec) {
        if(cm.tp == 1) {
            chdb_protocol::operation_var op_var;
            op_var.tx_id = tx_id;
            op_var.key = cm.key;
            op_var.value = cm.val;
            int r;
            this->db->vserver->execute(cm.key, chdb_protocol::Put, op_var, r);
            put_kv.push_back(std::make_pair(cm.key, cm.val));
            if(!r){
                prepared = false;
            }
        }
    }
}

bool tx_region::try_put(const int key, const int val){
    db->new_mtx.lock();
    chdb_protocol::operation_var op_var;
    op_var.tx_id = tx_id;
    op_var.key = key;
    op_var.value = val;
    int r = 0;

    bool ret;
    if(db->locks[key].mtx->try_lock() || db->locks[key].tx_id == tx_id) {
        db->locks[key].tx_id = tx_id;
        this->db->vserver->execute(key, chdb_protocol::Put, op_var, r);
        put_kv.push_back(std::make_pair(key, val));
        if(!r){
            prepared = false;
        }
        ret = true;
    }
    else {
        if(db->locks[key].tx_id <= tx_id) {
            ret = false;
        }
        else{
            db->new_mtx.unlock();
            db->locks[key].mtx->lock();
            db->locks[key].tx_id = tx_id;
            db->new_mtx.lock();
            this->db->vserver->execute(key, chdb_protocol::Put, op_var, r);
            put_kv.push_back(std::make_pair(key, val));
            if(!r){
                prepared = false;
            }
            ret = true;
        }
    }
    if(ret){
        commands_vec.push_back(commands(1, key, val));
    }

    db->new_mtx.unlock();
    return ret;
}

bool tx_region::try_get(const int key, int &r) {
    db->new_mtx.lock();
    chdb_protocol::operation_var op_var;
    op_var.tx_id = tx_id;
    op_var.key = key;
    op_var.value = 0;
    
    bool ret;
    if(db->locks[key].mtx->try_lock() || db->locks[key].tx_id == tx_id) {
        db->locks[key].tx_id = tx_id;
        this->db->vserver->execute(key, chdb_protocol::Get, op_var, r);
        ret = true;
    }
    else {
        if(db->locks[key].tx_id <= tx_id) {
            ret = false;
        }
        else{
            db->new_mtx.unlock();
            db->locks[key].mtx->lock();
            db->locks[key].tx_id = tx_id;
            db->new_mtx.lock();
            this->db->vserver->execute(key, chdb_protocol::Get, op_var, r);
            ret = true;
        }
    }
    if(ret){
        commands_vec.push_back(commands(2, key, 0));
    }

    db->new_mtx.unlock();
    return ret;
}


int tx_region::put(const int key, const int val) {
    // TODO: Your code here

    #if BIG_LOCK
    chdb_protocol::operation_var op_var;
    op_var.tx_id = tx_id;
    op_var.key = key;
    op_var.value = val;
    int r;
    put_kv.push_back(std::make_pair(key, val));

    if(this->db->vserver->execute(key, chdb_protocol::Put, op_var, r) == 0){
        if(r != -1){
            chdb_protocol::prepare_var pre_var;
            pre_var.tx_id = tx_id;
            this->db->vserver->execute_prepare(key, chdb_protocol::Prepare, pre_var, r);
        }
    }
    else {
        // RPC fails
    }

    #else
    {
        std::unique_lock<std::mutex> lock(db->new_mtx);
        if(db->locks.find(key) == db->locks.end()){
            db->locks[key] = chdb::key_lock(key);
        }
    }
    bool succ = try_put(key, val);
    while(!succ) {
        tx_abort();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        restart();
        succ = try_put(key, val);
    }
    return 1;

            
    #endif

    return 0;
}



int tx_region::get(const int key) {
    // TODO: Your code here
    #if BIG_LOCK
    chdb_protocol::operation_var op_var;
    op_var.tx_id = tx_id;
    op_var.key = key;
    op_var.value = 0;
    int r = 0;
    if(this->db->vserver->execute(key, chdb_protocol::Get, op_var, r) == 0){
        // if(r != -1){
        //     chdb_protocol::prepare_var pre_var;
        //     pre_var.tx_id = db->next_tx_id();
        //     this->db->vserver->execute_prepare(key, chdb_protocol::Prepare, pre_var, r);
        // }
    }
    else {
        // RPC fails
    }

    return r;

    #else
    {
        std::unique_lock<std::mutex> lock(db->new_mtx);
        if(db->locks.find(key) == db->locks.end()){
            db->locks[key] = chdb::key_lock(key);
        }
    }
    int r = 0;
    bool succ = try_get(key, r);
    while(!succ) {
        tx_abort();
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        restart();
        succ = try_get(key, r);
    }
    return r;

    #endif
}

int tx_region::tx_can_commit() {
    // TODO: Your code here
    #if BIG_LOCK
    for(auto kv : put_kv){
        chdb_protocol::check_prepare_state_var ch_pre_var;
        ch_pre_var.tx_id = tx_id;
        int r = 0;
        this->db->vserver->execute_check(kv.first, chdb_protocol::CheckPrepareState, ch_pre_var, r);
        printf("%d\n", r);
        if(r == false){
            return chdb_protocol::prepare_not_ok;
        }
    }

    return chdb_protocol::prepare_ok;

    #else
    return prepared;
    
    #endif
}

int tx_region::tx_begin() {
    // TODO: Your code here
    printf("tx[%d] begin\n", tx_id);
    return 0;
}

int tx_region::tx_commit() {
    // TODO: Your code here
    printf("tx[%d] commit\n", tx_id);
    for(auto kv : put_kv){
        chdb_protocol::commit_var cm_var;
        cm_var.tx_id = tx_id;
        int r = 0;
        this->db->vserver->execute_commit(kv.first, chdb_protocol::Commit, cm_var, r);
    }

    #if BIG_LOCK
    #else

    db->new_mtx.lock();
    std::set<int> tmp;
    for(auto command : commands_vec) {
        if(tmp.find(command.key) != tmp.end()){
            continue;
        }
        tmp.insert(command.key);
        db->locks[command.key].tx_id = -1;
        db->locks[command.key].mtx->unlock();
    }
    db->new_mtx.unlock();

    #endif

    return 0;
}

int tx_region::tx_abort() {
    // TODO: Your code here
    printf("tx[%d] abort\n", tx_id);
    for(auto kv : put_kv){
        chdb_protocol::rollback_var abort_var;
        abort_var.tx_id = tx_id;
        int r = 0;
        this->db->vserver->execute_rollback(kv.first, chdb_protocol::Rollback, abort_var, r);
    }

    #if BIG_LOCK
    #else
    db->new_mtx.lock();
    std::set<int> tmp;
    for(auto command : commands_vec) {
        if(tmp.find(command.key) != tmp.end()){
            continue;
        }
        tmp.insert(command.key);
        db->locks[command.key].tx_id = -1;
        db->locks[command.key].mtx->unlock();
    }
    db->new_mtx.unlock();

    #endif

    return 0;
}
