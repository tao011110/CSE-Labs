#include "chdb_state_machine.h"

chdb_command::chdb_command() : chdb_command(CMD_NONE, 0, 0, 0) {
    // TODO: Your code here
}

chdb_command::chdb_command(command_type tp, const int &key, const int &value, const int &tx_id)
        : cmd_tp(tp), key(key), value(value), tx_id(tx_id), res(std::make_shared<result>()) {
    // TODO: Your code here
    res->start = std::chrono::system_clock::now();
    res->key = key;

}

chdb_command::chdb_command(const chdb_command &cmd) :
        cmd_tp(cmd.cmd_tp), key(cmd.key), value(cmd.value), tx_id(cmd.tx_id), res(cmd.res) {
    // TODO: Your code here

}


void chdb_command::serialize(char *buf, int size) const {
    // TODO: Your code here
    std::cout << "do  serialize  " << std::endl;
    std::string tmp = std::to_string(cmd_tp) + "-" + std::to_string(key) + "-" + std::to_string(value);
    std::cout << "serialize is " << tmp << std::endl;
    strcpy(buf, tmp.c_str());

    return;
}

void chdb_command::deserialize(const char *buf, int size) {
    // TODO: Your code here
    int count = 0;
    std::string tmp = "";
    std::cout << ":deserialize" << std::endl;
    for(int i = 0; i < 128; i++){
        if(buf[i] == '-'){
            if(count == 0){
                int tp = std::atoi(tmp.c_str());
                cmd_tp = (command_type)tp;
                tmp = "";
                std::cout << "get cmd_tp is " << cmd_tp << std::endl;
            }
            if(count == 1){
                key = std::atoi(tmp.c_str());
                tmp = "";
                std::cout << "get key is " << key << std::endl;
            }
            count++;
            continue;
        }
        if(buf[i] == '\0'){
            value = std::atoi(tmp.c_str());
            std::cout << "get value is " << value << std::endl;
            break;
        }
        tmp += buf[i];
    }

    return;
}

marshall &operator<<(marshall &m, const chdb_command &cmd) {
    // TODO: Your code here
    int tp = 0;
    std::cout << "we in cmd.cmd_tp:" << cmd.cmd_tp << std::endl;
    switch (cmd.cmd_tp)
    {
    case chdb_command::command_type::CMD_NONE:
        tp = 0;
        break;
    
    case chdb_command::command_type::CMD_GET:
        tp = 1;
        printf("get!\n");
        break;

    case chdb_command::command_type::CMD_PUT:
        tp = 2;
        printf("put!\n");
        break;

    default:
        break;
    }
    printf("tp is %d\n", tp);
    m << tp;
    printf("cmd.key is %d\n", cmd.key);
    m << cmd.key;
    printf("cmd.value is %d\n", cmd.value);
    m << cmd.value;
    printf("cmd.tx_id is %d\n", cmd.tx_id);
    m << cmd.tx_id;

    return m;
}

unmarshall &operator>>(unmarshall &u, chdb_command &cmd) {
    // TODO: Your code here
    int tp = 0;
    u >> tp;
    std::cout << "tp is " << tp << std::endl;
    switch (tp)
    {
    case 0:
        std::cout << "CMD_NONE" << std::endl;
        cmd.cmd_tp = chdb_command::command_type::CMD_NONE;
        break;

    case 1:
        std::cout << "CMD_GET" << std::endl;
        cmd.cmd_tp = chdb_command::command_type::CMD_GET;
        break;

    case 2:
        std::cout << "CMD_PUT" << std::endl;
        cmd.cmd_tp = chdb_command::command_type::CMD_PUT;
        break;
    
    default:
        break;
    }
    std::cout << "cmd_tp:  " << cmd.cmd_tp << std::endl;
    printf("cmd.key is %d\n", cmd.key);
    u >> cmd.key;
    printf("cmd.value is %d\n", cmd.value);
    u >> cmd.value;
    printf("cmd.tx_id is %d\n", cmd.tx_id);
    u >> cmd.tx_id;

    return u;
}

void chdb_state_machine::apply_log(raft_command &cmd) {
    // TODO: Your code here
    try{
        std::cout << "do apply log" << std::endl;
        if(&cmd == nullptr){
            return;
        }
        chdb_command chdb_cmd;

        // if(typeid(cmd) == typeid(chdb_command)){
            printf("it is now!\n");
            chdb_cmd.key = ((chdb_command &)cmd).key;
            chdb_cmd.value = ((chdb_command &)cmd).value;
            chdb_cmd.cmd_tp = ((chdb_command &)cmd).cmd_tp;
            printf("key is %d\n", chdb_cmd.key);
            printf("value is %d\n", chdb_cmd.value);
            printf("tp is %d\n", int(chdb_cmd.cmd_tp));
        // }
        // else{
        //     printf("why not now!\n");
        //     chdb_cmd = dynamic_cast<chdb_command&>(cmd);
        // }
        // chdb_command chdb_cmd;

        // chdb_command &chdb_cmd = dynamic_cast<chdb_command&>(cmd);
        std::cout << "cmd_tp:  " << chdb_cmd.cmd_tp << std::endl;
        chdb_cmd.res->done = true;
        std::cout << "res done true  " << std::endl;
        std::unique_lock<std::mutex> lock(chdb_cmd.res->mtx);
        switch (chdb_cmd.cmd_tp)
        {
            case chdb_command::command_type::CMD_NONE:{
                std::cout << "just none " << std::endl;
                chdb_cmd.res->key = chdb_cmd.key;
                chdb_cmd.res->value = chdb_cmd.value;
                // chdb_cmd.res->succ = true;
                break;
            }

            case chdb_command::command_type::CMD_GET:{
                std::cout << "apply get" << std::endl;
                std::cout << "we apply get " << chdb_cmd.key << std::endl;
                chdb_cmd.res->key = chdb_cmd.key;
                chdb_cmd.res->value = chdb_cmd.value;
                int size = kv.size();
                std::cout << "size  " << size << std::endl;
                // bool succ = false;
                for(int i = 0; i < size; i++){
                    if(kv[i].first == chdb_cmd.key){
                        // if(kv[i].second == ""){
                        //     break;
                        // }
                        // succ = true;
                        std::cout << "we get it !" << std::endl;
                        chdb_cmd.res->value = kv[i].second;
                        break;
                    }
                    else{
                        std::cout << "chdb[i].first  " <<  kv[i].first << std::endl;
                    }
                }
                std::cout << "we get " << chdb_cmd.res->value << std::endl;
                // chdb_cmd.res->succ = succ;
                break;
            }

            case chdb_command::command_type::CMD_PUT:{
                std::cout << "apply put" << std::endl;
                std::cout << "we apply put " << chdb_cmd.key << std::endl;
                chdb_cmd.res->key = chdb_cmd.key;
                chdb_cmd.res->value = chdb_cmd.value;
                int size = kv.size();
                // bool succ = true;
                int i = 0;
                for(i = 0; i < size; i++){
                    if(kv[i].first == chdb_cmd.key){
                        // succ = false;
                        chdb_cmd.res->value = kv[i].second;
                        std::pair<int, int> p;
                        p.first = chdb_cmd.key;
                        p.second = chdb_cmd.value;
                        kv[i] = p;
                        break;
                    }
                }
                if(i == size){
                    std::pair<int, int> p;
                    p.first = chdb_cmd.key;
                    p.second = chdb_cmd.value;
                    kv.push_back(p);
                }
                std::cout << "we put " << chdb_cmd.res->value << std::endl;
                // chdb_cmd.res->succ = succ;
                break;
            }

            default:
                break;
        }
        chdb_cmd.res->cv.notify_all();
        std::cout << "are you ok now" << std::endl;
    }
    catch(std::exception e){
        std::cout << e.what() << std::endl; 
    }
}