#include "inode_manager.h"
#include "time.h"

// disk layer -----------------------------------------
int M[52] = {0};
disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  memcpy(buf, blocks[id], BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{
  // std::cout << "do  write_block  " << std::endl;
  memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  int start=IBLOCK(INODE_NUM,BLOCK_NUM) + 1;
  for(blockid_t i = start; i < BLOCK_NUM; i++){
    if(!using_blocks[i]){
      using_blocks[i] = 1;
      return i;
    }
  }

  return 0;
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  using_blocks[id] = 0;
  
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  int i = 0;
      // std::cout << "create  node_____ " << std::endl;
  for(i = 0; i < INODE_NUM; i++){
    inode_t *tmp = get_inode(i+1);
    if(!tmp){
      tmp = (inode_t *)malloc(sizeof(inode_t));
      bzero(tmp, sizeof(inode_t));
      tmp->type = type;
      tmp->atime = time(NULL);
      tmp->mtime = time(NULL);
      tmp->ctime = time(NULL);
      tmp->size = 0;
      put_inode(i+1, tmp);
      free(tmp);
      break;
    }
    free(tmp);
  }

  return i + 1;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  inode_t *tmp = get_inode(inum);
  bzero(tmp, sizeof(inode_t));
  put_inode(inum, tmp);
  free(tmp);

  return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */
  inode_t *tmp = get_inode(inum);
  *size = tmp->size;
  *buf_out = (char*)malloc(*size);
  char overBuf[BLOCK_SIZE];
  int blockNum = *size / BLOCK_SIZE;
  int remainSize = *size % BLOCK_SIZE;
  blockid_t *blocks = tmp->blocks;
  if(remainSize){
    blockNum++;
  }
  int index = 0;
  while(true){
    for(int i = 0; i < blockNum && i < NDIRECT; i++){
      char buf[BLOCK_SIZE];
      if(i == blockNum - 1 && remainSize){
          bm->read_block(blocks[i], buf);
          memcpy(*buf_out + index * BLOCK_SIZE, buf, remainSize);
          break;
      }
      bm->read_block(blocks[i], buf);

      memcpy(*buf_out + index * BLOCK_SIZE, buf, BLOCK_SIZE);
      index++;
    }
    if(blockNum > NDIRECT){
      memset(overBuf, '\0', sizeof(overBuf));
      bm->read_block(tmp->blocks[NDIRECT], overBuf);
      blocks = (blockid_t*)overBuf;

      blockNum -= NDIRECT;
    }
    else{
      break;
    }
  }
  free(tmp);
  
  return;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  inode_t *tmp = get_inode(inum);
  int blockNum = size / BLOCK_SIZE;
  int remainSize = size % BLOCK_SIZE;
  if(remainSize){
    blockNum++;
  }
  char overBuf[BLOCK_SIZE];
  int oldBlockNum = 0;
  if(tmp->size == 0){
    oldBlockNum = 0;
  }
  else{
    oldBlockNum = (tmp->size - 1) / BLOCK_SIZE + 1;
  }
  // std::cout << "blockNum is   " << blockNum << "    and   " << "oldBlockNum is  " << oldBlockNum << std::endl;
  /* 
    I need to free the blocks no longer in use,
    when the size of buf is smaller than the size of original inode.
  */
  if(blockNum < oldBlockNum){
    blockid_t *freeBlocks = tmp->blocks;
    char freeBuf[BLOCK_SIZE];
    int beginIndex = blockNum;
    for(int i = beginIndex; i < oldBlockNum && i < NDIRECT; i++){
      bm->free_block(freeBlocks[i]);
      tmp->blocks[i] = 0;
    }
    if(oldBlockNum > NDIRECT && blockNum <= NDIRECT){
      bm->read_block(freeBlocks[NDIRECT], freeBuf);
      bm->free_block(freeBlocks[NDIRECT]);
      tmp->blocks[NDIRECT] = 0;
      freeBlocks = (blockid_t*)freeBuf;
      oldBlockNum = oldBlockNum - NDIRECT;
      for(int i = 0; i < oldBlockNum && i < NDIRECT; i++){
        bm->free_block(freeBlocks[i]);
      }
    }
    else{
      if(oldBlockNum > NDIRECT && blockNum > NDIRECT){
        bm->read_block(freeBlocks[NDIRECT], freeBuf);
        freeBlocks = (blockid_t*)freeBuf;
        oldBlockNum = oldBlockNum - NDIRECT;
        beginIndex = blockNum - NDIRECT;
        for(int i = beginIndex; i < oldBlockNum && i < NDIRECT; i++){
          bm->free_block(freeBlocks[i]);
        }
      }
    }
  }
  /* 
    I need to allocate space for the blocks that are not allocated now,
    when the size of buf is larger than the size of original inode.
  */
  else{
    if(blockNum > oldBlockNum){
      char allocBuf[BLOCK_SIZE];
      for(int i = oldBlockNum; i < blockNum && i < NDIRECT; i++){
        tmp->blocks[i] = bm->alloc_block();
      }
      if(blockNum > NDIRECT){
        if(!tmp->blocks[NDIRECT]){
          tmp->blocks[NDIRECT] = bm->alloc_block();
        }
        bm->read_block(tmp->blocks[NDIRECT], allocBuf);
        for(int i = NDIRECT; i < blockNum; i++){
          ((blockid_t*)allocBuf)[i - NDIRECT] = bm->alloc_block();
        }
        bm->write_block(tmp->blocks[NDIRECT], allocBuf);
      }
    }
  }

  blockid_t *blocks = tmp->blocks;
  int index = 0;
  
  // std::cout << "will see memcpy " << size << std::endl;
  while(true){
    for(int i = 0; i < blockNum && i < NDIRECT; i++){
      if(i == blockNum - 1 && remainSize){
        char remainBuf[BLOCK_SIZE];
        // std::cout << "memcpy fot doublesize!!"  << std::endl;
        memcpy(remainBuf, buf + index * BLOCK_SIZE, remainSize);
        // std::cout << "end    memcpy fot doublesize!!"   << std::endl;
        bm->write_block(blocks[i], remainBuf);
        break;
      }
      bm->write_block(blocks[i], buf + index * BLOCK_SIZE);
      index++;
    }
    if(blockNum > NDIRECT){
      memset(overBuf,'\0',sizeof(overBuf));
      bm->read_block(tmp->blocks[NDIRECT], overBuf);
      blocks = (blockid_t*)overBuf;
      blockNum -= NDIRECT;
    }
    else{
      break;
    }
  }
  
  // std::cout << "begin   put_inode" << size << std::endl;
  tmp->size = size;
  tmp->atime = time(NULL);
  tmp->mtime = time(NULL);
  tmp->ctime = time(NULL);
  put_inode(inum, tmp);
  free(tmp);
  
  return;
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  
  inode_t *tmp = get_inode(inum);
  if(!tmp){
    return;
  }
  a.type = tmp->type;
  a.size = tmp->size;
  a.atime = tmp->atime;
  a.mtime = tmp->mtime;
  a.ctime = tmp->ctime;
  free(tmp);
  
  return;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  inode_t *tmp = get_inode(inum);
  int blockNum = 0;
  if(tmp->size != 0){
    blockNum = (tmp->size - 1) / BLOCK_SIZE + 1;
  }
  blockid_t *blocks = tmp->blocks;
  char buf[BLOCK_SIZE];
  while(true){
    for(int i = 0; i < blockNum && i < NDIRECT; i++){
      bm->free_block(blocks[i]);
    }
    if(blockNum >= NDIRECT + 1){
      bm->read_block(blocks[NDIRECT], buf);
      bm->free_block(blocks[NDIRECT]);
      blocks = (blockid_t*)buf;
      blockNum -= NDIRECT;
    }
    else{
      break;
    }
  }
  free_inode(inum);
  free(tmp);
  
  return;
}
