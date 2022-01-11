// chfs client.  implements FS operations using extent and lock server
#include "chfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

chfs_client::chfs_client()
{
    ec = new extent_client();

}

chfs_client::chfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client();
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

chfs_client::inum
chfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
chfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
chfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is a dir\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

bool
chfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        return false;
    }
    if (a.type == extent_protocol::T_DIR) {
        return true;
    } 
    return false;
}

bool
chfs_client::issymlink(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        return false;
    }

    if (a.type == extent_protocol::T_SYMLINK) {
        return true;
    } 

    return false;
}

int
chfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
chfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}

#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
chfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    std::string buf = "";
    r = ec->get(ino, buf);
    if(r != OK){
        return r;
    }
    size_t length = buf.size();
    if(length >= size){
        buf = buf.substr(0, size);
    }
    else{
        std::string fill = "\0";
        while(length < size){
            buf.append(fill);
            length++;
        }
    }
    r = ec->put(ino, buf);
    if(r != OK){
        return r;
    }

    return r;
}

int
chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    bool found = false;
    std::string buf;

    r = lookup(parent, name, found, ino_out);
    if(found){
        return r;
    }
    r = ec->create(extent_protocol::T_FILE, ino_out);
    if(r != OK){
        return r;
    }
    ec->get(parent, buf);
    if(r != OK){
        return r;
    }
    direntformat df;
    strcpy(df.name, name);
    df.inum = ino_out;
    buf.append((char*)(&df), sizeof(direntformat));
    r = ec->put(parent, buf);

    return r;
}

int
chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    bool found = false;
    std::string buf;
    
    r = lookup(parent, name, found, ino_out);
    if(found){
        return r;
    }
    r = ec->create(extent_protocol::T_DIR, ino_out);
    if(r != OK){
        return r;
    }
    ec->get(parent, buf);
    if(r != OK){
        return r;
    }
    direntformat df;
    strcpy(df.name, name);
    df.inum = ino_out;
    buf.append((char*)(&df), sizeof(direntformat));
    r = ec->put(parent, buf);

    return r;
}

int
chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    std::string buf;
    r = ec->get(parent, buf);
    const char* bufs = buf.c_str();
    if(r!=extent_protocol::OK){
        return r;
    }
    direntformat df;
    uint32_t dfSize = sizeof(direntformat);
    for(uint32_t i=0;i<buf.size();i += dfSize){
        if(!strcmp(bufs + i, name)){
            memcpy((void*)(&df), bufs + i, dfSize);
            ino_out = df.inum;
            found = true;
            break;
        }
    }
    
    return r;
}

int
chfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    std::string buf;

    r = ec->get(dir, buf);
    int size = buf.size();
    direntformat df;
    int dfSize = sizeof(direntformat);
    const char* bufs = buf.c_str();
    for(int i = 0; i < size; i += dfSize){
        dirent tmp;
        memcpy((void*)(&df), bufs+i, dfSize);
        tmp.name = df.name;
        tmp.inum = df.inum;
        list.push_back(tmp);
    }

    return r;
}

int
chfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */
    std::string buf = "";
    r = ec->get(ino, buf);
    if(r != OK){
        return r;
    }
    size_t bufSize = buf.size();
    if((size_t)off > bufSize){
        return r;
    }
    else{
        if(off + size > bufSize){
            data = buf.substr(off, bufSize - off);
        }
        else{
            data = buf.substr(off, size);
        }
    }

    return r;
}

int
chfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;
    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    std::string buf = "";
    r = ec->get(ino, buf);
    if(r != OK){
        return r;
    }
    size_t length = buf.size();
    std::string tmp;
    tmp.assign(data, size);
    if((size_t)off > length){
        buf.resize(off + size);
    }
    buf.replace(off, size, tmp);
    ec->put(ino, buf);

    return r;
}

int chfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    bool found = false;
    std::list<dirent> entries;
    inum ino = 0;
    r = lookup(parent, name, found, ino);
    if(found == false){
        r = NOENT;
        return r;
    }
    r = ec->remove(ino);
    if(r != OK){
        return r;
    }
    r = readdir(parent, entries);
    
    std::string buf = "";
    std::list<dirent>::iterator it = entries.begin();
    while(it != entries.end()) {
        std::string file = it->name;
        inum ino = it->inum;
        if(file == std::string(name)){
            it++;
            continue;
        }
        direntformat df;
        strcpy(df.name, file.c_str());
        df.inum = ino;
        buf.append((char*)(&df), sizeof(direntformat));
        it++;
    }
    r = ec->put(parent, buf);

    return r;
}

int chfs_client::readlink(inum ino, std::string &buf)
{
    int r = OK;

    r = ec->get(ino, buf);
    if(r != OK){
        r = IOERR;
    }
    
    return r;
}

int
chfs_client::symlink(const char *link, inum parent, const char *name, inum &ino_out){
    int r = OK;
    std::string buf;
    bool found = false;
    r = lookup(parent, name, found, ino_out);
    if(found == true){
        return r;
    }
    r = ec->create(extent_protocol::T_SYMLINK, ino_out);
    if(r != OK){
        return r;
    }
    r = ec->put(ino_out, std::string(link));
    if(r != OK){
        return r;
    }

    r = ec->get(parent, buf);
    if(r != OK){
        r = IOERR;
        return r;
    }
    
    direntformat df;
    strcpy(df.name, name);
    df.inum = ino_out;
    buf.append((char*)(&df), sizeof(direntformat));

    r = ec->put(parent, buf);

    return r;
}

