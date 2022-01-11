#include "raft_state_machine.h"


kv_command::kv_command() : kv_command(CMD_NONE, "", "") { }

kv_command::kv_command(command_type tp, const std::string &key, const std::string &value) : 
    cmd_tp(tp), key(key), value(value), res(std::make_shared<result>())
{
    res->start = std::chrono::system_clock::now();
    res->key = key;
}

kv_command::kv_command(const kv_command &cmd) :
    cmd_tp(cmd.cmd_tp), key(cmd.key), value(cmd.value), res(cmd.res) {}

kv_command::~kv_command() { }

int kv_command::size() const {
    // Your code here:
    return -1;
}


void kv_command::serialize(char* buf, int size) const {
    // Your code here:
    std::string tmp = std::to_string(cmd_tp) + "-" + key + "-" + value;
    strcpy(buf, tmp.c_str());
    std::cout << "serialize is " << tmp << std::endl;

    return;
}

void kv_command::deserialize(const char* buf, int size) {
    // Your code here:
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
                key = tmp;
                tmp = "";
                std::cout << "get key is " << key << std::endl;
            }
            count++;
            continue;
        }
        if(buf[i] == '\0'){
            value = tmp;
            std::cout << "get value is " << value << std::endl;
            break;
        }
        tmp += buf[i];
    }

    return;
}

marshall& operator<<(marshall &m, const kv_command& cmd) {
    // Your code here:
    int tp = 0;
    std::cout << "we in cmd.cmd_tp:" << cmd.cmd_tp << std::endl;
    switch (cmd.cmd_tp)
    {
    case kv_command::command_type::CMD_NONE:
        tp = 0;
        break;
    
    case kv_command::command_type::CMD_GET:
        tp = 1;
        break;

    case kv_command::command_type::CMD_PUT:
        tp = 2;
        break;

    case kv_command::command_type::CMD_DEL:
        tp = 3;
        break;

    default:
        break;
    }
    m << tp;
    m << cmd.key;
    m << cmd.value;

    return m;
}

unmarshall& operator>>(unmarshall &u, kv_command& cmd) {
    // Your code here:
    int tp = 0;
    std::cout << "cmd_tp:  " << cmd.cmd_tp << std::endl;
    u >> tp;
    switch (tp)
    {
    case 0:
        cmd.cmd_tp = kv_command::command_type::CMD_NONE;
        break;

    case 1:
        cmd.cmd_tp = kv_command::command_type::CMD_GET;
        break;

    case 2:
        cmd.cmd_tp = kv_command::command_type::CMD_PUT;
        break;

    case 3:
        cmd.cmd_tp = kv_command::command_type::CMD_DEL;
        break;
    
    default:
        break;
    }
    u >> cmd.key;
    std::cout << "cmd.key:  " << cmd.key << std::endl;
    u >> cmd.value;
    std::cout << "cmd.value:  " << cmd.value << std::endl;

    return u;
}

kv_state_machine::~kv_state_machine() {

}

void kv_state_machine::apply_log(raft_command &cmd) {
    kv_command &kv_cmd = dynamic_cast<kv_command&>(cmd);
    std::unique_lock<std::mutex> lock(kv_cmd.res->mtx);
    // Your code here:
    kv_cmd.res->done = true;
    switch (kv_cmd.cmd_tp)
    {
        case kv_command::command_type::CMD_NONE:{
            std::cout << "just none " << std::endl;
            kv_cmd.res->key = kv_cmd.key;
            kv_cmd.res->value = kv_cmd.value;
            kv_cmd.res->succ = true;
            break;
        }

        case kv_command::command_type::CMD_GET:{
            std::cout << "we apply get " << kv_cmd.key << std::endl;
            kv_cmd.res->key = kv_cmd.key;
            kv_cmd.res->value = kv_cmd.value;
            int size = kv.size();
            std::cout << "size  " << size << std::endl;
            bool succ = false;
            for(int i = 0; i < size; i++){
                if(kv[i].first == kv_cmd.key){
                    if(kv[i].second == ""){
                        break;
                    }
                    succ = true;
                    std::cout << "we get it !" << std::endl;
                    kv_cmd.res->value = kv[i].second;
                    break;
                }
                else{
                    std::cout << "kv[i].first  " <<  kv[i].first << std::endl;
                }
            }
            std::cout << "we get " << kv_cmd.res->value << std::endl;
            kv_cmd.res->succ = succ;
            break;
        }

        case kv_command::command_type::CMD_PUT:{
            std::cout << "we apply put " << kv_cmd.key << std::endl;
            kv_cmd.res->key = kv_cmd.key;
            kv_cmd.res->value = kv_cmd.value;
            int size = kv.size();
            bool succ = true;
            int i = 0;
            for(i = 0; i < size; i++){
                if(kv[i].first == kv_cmd.key){
                    succ = false;
                    kv_cmd.res->value = kv[i].second;
                    std::pair<std::string, std::string> p;
                    p.first = kv_cmd.key;
                    p.second = kv_cmd.value;
                    kv[i] = p;
                    break;
                }
            }
            if(i == size){
                std::pair<std::string, std::string> p;
                p.first = kv_cmd.key;
                p.second = kv_cmd.value;
                kv.push_back(p);
            }
            std::cout << "we put " << kv_cmd.res->value << std::endl;
            kv_cmd.res->succ = succ;
            break;
        }

        case kv_command::command_type::CMD_DEL:{
            std::cout << "we apply del " << kv_cmd.key << std::endl;
            kv_cmd.res->key = kv_cmd.key;
            kv_cmd.res->value = kv_cmd.value;
            int size = kv.size();
            bool succ = false;
            for(int i = 0; i < size; i++){
                if(kv[i].first == kv_cmd.key){
                    succ = true;
                    kv_cmd.res->value = kv[i].second;
                    std::pair<std::string, std::string> p;
                    p.first = kv_cmd.key;
                    p.second = kv_cmd.value;
                    kv[i] = p;
                    break;
                }
            }
            kv_cmd.res->succ = succ;
            break;
        }

        default:
            break;
    }
    kv_cmd.res->cv.notify_all();
    std::cout << "are you ok" << std::endl;
    return;
}

std::vector<char> kv_state_machine::snapshot() {
    // Your code here:
    return std::vector<char>();
}

void kv_state_machine::apply_snapshot(const std::vector<char>& snapshot) {
    // Your code here:
    return;    
}
