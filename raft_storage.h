#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <mutex>
#include <fstream>
#include <sstream>

template<typename command>
class raft_storage {
public:
    raft_storage(const std::string& file_dir);
    ~raft_storage();
    // Your code here
    void append_log(std::vector<log_entry<command>> vec);
    void append_meta(int term, int votedFor);
    void find_meta(int &term, int &votedFor);
    void find_log(std::vector<log_entry<command>> &vec);
    void append_snapshot(std::vector<char> snapshot, int term, int index);
    void find_snapshot(std::vector<char> &snapshot);
    void find_snapshot_meta(int &term, int &index);

private:
    std::mutex mtx;
    struct log_meta_format{
        int term;
        int index;
    };
    struct meta_format{
        int term;
        int votedFor;
    };
    std::string dir_path;

    // int log_offset = 0;
};

template<typename command>
raft_storage<command>::raft_storage(const std::string& dir){
    // Your code here
    dir_path = dir;

}

template<typename command>
raft_storage<command>::~raft_storage() {
   // Your code here
   std::cout << "~raft_storage" << std::endl;
//    std::string filename = dir_path + std::string("/metadata.txt");
//    std::fstream meta_file(filename, std::fstream::out | std::ios_base::trunc);
//    filename = dir_path + std::string("/log.txt");
//    std::fstream log_file(filename, std::fstream::out | std::ios_base::trunc);
}

template<typename command>
void raft_storage<command>::append_meta(int term, int votedFor){
    mtx.lock();
    std::string filename = dir_path + std::string("/metadata.txt");
    std::cout << "metadata filename is " << filename << std::endl;
    std::ofstream file(filename, std::ios::app|std::ios::binary);
    meta_format meta;
    meta.term = term;
    meta.votedFor = votedFor;
    std::string buf;
    buf.append((char*)(&meta), sizeof(meta_format));
    file << buf;
    std::cout << "write in " << term << "  and  " << votedFor << std::endl;
    file.close();
    mtx.unlock();
}
    
template<typename command>
void raft_storage<command>::find_meta(int &term, int &votedFor){
    mtx.lock();
    std::cout << "to find metadata  "  << std::endl;
    std::string filename = dir_path + std::string("/metadata.txt");
    std::fstream file(filename, std::ios::in|std::ios::binary);
    int offset_init = sizeof(meta_format);
    file.seekg(0, std::ios::end);
    if(file.tellg() == 0){
        std::cout << "size 0"<<std::endl;
        term = 0;
        votedFor = -1;
        file.close();
        mtx.unlock();
    }
    else{
        std::cout << "meta tellg() is " << file.tellg() << std::endl;
        file.seekg(0 - offset_init, std::ios::end);
        std::cout << "meta tellg() is " << file.tellg() << std::endl;
        std::cout << offset_init << std::endl;
        char *tmp = new char[offset_init];
        memset(tmp, '\0', offset_init);
        file.read(tmp, offset_init);
        std::cout << "meta tellg() is " << file.tellg() << std::endl;
        meta_format meta;
        memcpy((void*)(&meta), tmp, offset_init);
        std::cout <<"meta.term  " <<  meta.term << std::endl;
        std::cout <<"meta.votedFor  " <<  meta.votedFor << std::endl;
        term = meta.term;
        votedFor = meta.votedFor;
        delete[] tmp;
        file.close();
        mtx.unlock();
    }
}

template<typename command>
void raft_storage<command>::append_log(std::vector<log_entry<command>> vec){
    mtx.lock();
    std::cout << dir_path << "do write log " << std::endl;
    int size = vec.size();
    if(size == 0){
        return;
    }
    std::string filename = dir_path + std::string("/log.txt");
    std::ofstream file(filename, std::ios::app|std::ios::binary);
    
    for(int i = 0; i < size; i++){
        log_meta_format log;
        log.term = vec[i].term;
        log.index = vec[i].index;
        std::cout << "we this log term is " << log.term << " and log index is " << log.index << std::endl;
        char *tmp = new char[128];
        memset(tmp, '\0', 128);
        vec[i].commandLog.serialize(tmp, vec[i].commandLog.size());
        std::cout << "we serialize the  " << vec[i].commandLog.value << std::endl;
        std::cout << "after serialize :  " << tmp << std::endl;
        std::string buf;
        buf.append((char*)(&log), sizeof(log_meta_format));
        buf.append((char*)tmp, 128);
        file << buf;
        
        std::cout << "after this the file size is " << file.tellp() << std::endl;
        delete[] tmp;
    }
    std::string size_buf;
    size_buf.append((char*)(&size), sizeof(int));
    file << size_buf;
    std::cout << "we want to in size  " << size << std::endl;
    std::cout << "after append the file size is " << file.tellp() << std::endl;
    file.close();
    mtx.unlock();
}

template<typename command>
void raft_storage<command>::find_log(std::vector<log_entry<command>> &vec){
    mtx.lock();
    std::string filename = dir_path + std::string("/log.txt");
    std::fstream file(filename, std::ios::in|std::ios::binary);
    file.seekg(0, std::ios::end);
    std::cout << dir_path << " do read log " << std::endl;
    if(file.tellg() == 0){
        std::cout << "log file size 0"<<std::endl;
    }
    else{
        std::cout << "file size is " << file.tellg() << std::endl;
        int offset_init = sizeof(int);
        std::cout << offset_init << std::endl;
        file.seekg(0 - offset_init, std::ios::end);
        int total = 0;
        file.read((char*)&total, sizeof(int));
        std::cout << "we getsize is " << total << std::endl;

        int log_size = sizeof(log_meta_format);
        int offset = total * (log_size + 128) + (int)(sizeof(int));
        file.seekg(0 - offset, std::ios::end);
        char *tmp = new char[total * (log_size + 128)];
        memset(tmp, '\0', total * (log_size + 128));
        file.read(tmp, total * (log_size + 128));
        for(int i = 0; i < total; i++){
            log_meta_format log_f;
            memcpy((void*)(&log_f), tmp + i * (log_size + 128), log_size);
            log_entry<command> log_e;
            log_e.term = log_f.term;
            log_e.index = log_f.index;
            char detail[128];
            memcpy((char *)detail, tmp + i * (log_size + 128) + log_size, 128);
            std::cout << " detail " << detail << std::endl;
            log_e.commandLog.deserialize(detail, log_e.commandLog.size());
            std::cout << "log term is " << log_e.term << " and index is " << log_e.index << " and command value is " << log_e.commandLog.value << std::endl;            
            vec.push_back(log_e);
        }
        delete[] tmp;
    }

    file.close();
    mtx.unlock();
}

template<typename command>
void raft_storage<command>::append_snapshot(std::vector<char> snapshot, int term, int index){
    mtx.lock();
    std::string filename = dir_path + std::string("/snapshot.txt");
    std::ofstream file(filename, std::ios::trunc|std::ios::binary);
    int size = snapshot.size();
    for(int i = 0; i < size; i++){
        file << snapshot[i];
        std::cout << "we write snap is " << snapshot[i] << std::endl;
    }

    std::string term_buf;
    term_buf.append((char*)(&term), sizeof(int));
    file << term_buf;
    std::cout << "snap term is " << term << std::endl;

    std::string index_buf;
    term_buf.append((char*)(&index), sizeof(int));
    file << index_buf;
    std::cout << "snap index is " << index << std::endl;

    file.close();
    mtx.unlock();
}

template<typename command>
void raft_storage<command>::find_snapshot(std::vector<char> &vec){
    mtx.lock();
    std::cout << "to find snapshot" << std::endl;
    std::string filename = dir_path + std::string("/snapshot.txt");
    std::fstream file(filename, std::ios::in|std::ios::binary);
    file.seekg(0, std::ios::end);
    if(file.tellg() == 0){
        std::cout << "size 0"<<std::endl;
        file.close();
        mtx.unlock();
        return;
    }
    int snapshotSize = file.tellg();
    int size = snapshotSize - 2 * sizeof(int);
    file.seekg(0, std::ios::beg);
    char *total = new char[size];
    memset(total, '\0', size);
    file.read(total, size);
    int prev = 0;
    for(int i = 0; i < size; i++){
        if(total[i] == ' '){
            char *tmp = new char[128];
            memcpy(tmp, total + prev, i - prev);
            vec.push_back(*tmp);
            std::cout << "we now push  " << tmp << std::endl;
            delete tmp;
            prev = i + 1;
        }
    }
    file.close();

    mtx.unlock();
}
    
template<typename command>
void raft_storage<command>::find_snapshot_meta(int &term, int &index){
    mtx.lock();
    std::cout << "to find metadata  "  << std::endl;
    std::string filename = dir_path + std::string("/metadata.txt");
    std::fstream file(filename, std::ios::in|std::ios::binary);
    file.seekg(0, std::ios::end);
    if(file.tellg() == 0){
        std::cout << "size is 0"<<std::endl;
        file.close();
        mtx.unlock();
    }
    else{
        int offset_init = 2 * sizeof(int);
        file.seekg(0 - offset_init, std::ios::end);
        file.read((char*)&term, sizeof(int));
        file.read((char*)&index, sizeof(int));
        file.close();
        mtx.unlock();
    }
}

#endif // raft_storage_h