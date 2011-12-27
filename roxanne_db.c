/*
Copyright (c) 2011 Joseph Rothrock (rothrock@rothrock.org)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/



#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <errno.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/select.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/mman.h>
#include <semaphore.h>
#include <math.h>
#include <stdbool.h>
#include "longlong.h"
#include "fnv.h"

/*
  Here is the order for setting up a networked connection:
  socket()
  bind()
  listen()
  accept()
*/

// Stuff for .h

// Constants -n- Macros
#define BACKLOG 25
#define BLOCK_SIZE 4096
#define MAX_BLOCKS 134217728
#define BLOCK_BITMAP_BYTES 16777216
#define MSG_SIZE 65536
#define HASH_BITS 16
#define IDX_ENTRY_SIZE 1024
#define KEY_LEN (IDX_ENTRY_SIZE - 3*(sizeof(int)))


struct idx { // structure for an index record.
  char      key[KEY_LEN];
  int       block_offset; // starting block in the db file.
  int       length;       // db blocks consumed.
  int       next;         // overflow ptr to next index_record on disk.
};


struct  db_ptr { // a structure that points to a value in the db file.
  int block_offset;
  int blocks;
};


// Function signatures
int       start_listening(char* port, int backlog);
void      sigchld_handler(int s);
void      sigterm_handler_parent(int s);
void      sigterm_handler_child(int s);
int       get_hash_val(int bits, char* key);
int       guts(int accept_fd, int listen_fd);
int       extract_command(char* msg, int msglen);
int       write_record(char* key, char* data);
int       write_index(char* key, int block_offset, int length);
int       parse_create(char msg[], int msglen, char** key, char** value);
int       bit_array_set(char bit_array[], int bit);
int       bit_array_test(char bit_array[], int bit);
int       bit_array_clear(char bit_array[], int bit);
int       find(char* key);
int       create_block_reservation(int blocks_needed);
char*     read_record(struct db_ptr db_rec);
void      hash_write_lock(int hash_number);
void      hash_write_unlock(int hash_number);
void      cleanup_and_exit();
void      usage(char *argv);


// Globals
sem_t*          DB_WRITE_LOCK;
sem_t*          IDX_WRITE_LOCK;
sem_t*          HASH_WRITE_LOCK;
sem_t*          HASH_READ_LOCK;
char            *SHM_BLOCK_BITMAP;
char            *SHM_HASHBUCKET_BITMAP;
int             BLOCK_BITMAP_FD;
int             DB_FD;
int             IDX_FD;

int main(int argc, char* argv[]) {

  struct sockaddr incoming;
  socklen_t addr_size = sizeof(incoming); 
  int listen_fd, accept_fd;
  char* port = "4080";
  char* host = "::1";
  char db_file[4096] = "/var/roxanne/db";
  char idx_file[4096] = "/var/roxanne/idx";
  char block_bitmap_file[4096] = "/var/roxanne/block_bitmap";
  int chld;
  int shm_block_offset_id;
  key_t shm_block_offset_key = 1;
  int i;
  int ch;


  // parse our cmd line args
  while ((ch = getopt(argc, argv, "d:h:p:")) != -1) {
    switch (ch) {

      case 'd':
        sprintf(db_file, "%s/db", optarg);
        sprintf(idx_file, "%s/idx", optarg);
        sprintf(block_bitmap_file, "%s/block_bitmap", optarg);
        break;

      case 'h':
        host = optarg;
        break;

      case 'p':
        port=optarg;
        break;

     case '?':

     default:
       usage(argv[0]);
     }
  }
  argc -= optind;
  argv += optind;

  

  // Create our global write lock for the db and the block bitmap.
  // This is used to make sure that only one process at a time can 
  // change the block bitmap.
  if ((DB_WRITE_LOCK = sem_open("db_lock", O_CREAT, 0666, 1)) == SEM_FAILED) {
    perror("semaphore init failed");
    exit(-1);
  }
  sem_post(DB_WRITE_LOCK);

  // Create our global write lock for the index file.
  // This is used to safely append to the end of the file.
  if ((IDX_WRITE_LOCK = sem_open("idx_lock", O_CREAT, 0666, 1)) == SEM_FAILED) {
    perror("semaphore init failed");
    exit(-1);
  }
  sem_post(IDX_WRITE_LOCK);

  if ((HASH_WRITE_LOCK = sem_open("hash_write_lock", O_CREAT, 0666, 1)) == SEM_FAILED) {
    perror("semaphore init failed");
    exit(-1);
  }
  sem_post(HASH_WRITE_LOCK);


  if ((HASH_READ_LOCK = sem_open("hash_read_lock", O_CREAT, 0666, 1)) == SEM_FAILED) {
    perror("semaphore init failed");
    exit(-1);
  }
  sem_post(HASH_READ_LOCK);


  // Memory-map our block-bitmap file.
  // Add some logic to create the file if it doesn't exist.
  if ((BLOCK_BITMAP_FD = open(block_bitmap_file, O_RDWR | O_CREAT, 0666)) == -1) {
    fprintf(stderr, "Couldn't open block bitmap file %s\n", block_bitmap_file);
    perror(NULL);
    exit(-1);
  }
  if ((SHM_BLOCK_BITMAP = mmap((caddr_t)0, BLOCK_BITMAP_BYTES, PROT_READ | PROT_WRITE, MAP_SHARED, BLOCK_BITMAP_FD, 0)) == MAP_FAILED) {
    perror("Problem mmapping the block bitmap");
    exit(-1);
  }
  
  if ((SHM_HASHBUCKET_BITMAP = mmap((caddr_t)0, ((1<<HASH_BITS)/8), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0)) == MAP_FAILED) {
    perror("Problem mmapping the hasj bitmap");
    exit(-1);
  }
  
  // register a function to reap our dead children
  signal(SIGCHLD, sigchld_handler); 

  // Register a function to kill our children.
  // We'll unregister this function in our children.
  signal(SIGTERM, sigterm_handler_parent); 

  // Open our database file
  if ((DB_FD = open(db_file, O_RDWR | O_CREAT, 0666)) == -1) {
    fprintf(stderr, "Couldn't open database file named %s\n", db_file);
    perror(NULL);
    exit(-1);
  }

  // Open our index file
  if ((IDX_FD = open(idx_file, O_RDWR | O_CREAT, 0666)) == -1) {
    fprintf(stderr, "Couldn't open index file named %s\n", idx_file);
    perror(NULL);
    exit(-1);
  }

  // Demonize ourself.
  if ((chld = fork()) != 0 ) {printf("%d\n",chld); return(0);};

  // Start listening
  if ((listen_fd = start_listening(port, BACKLOG)) == -1) {
    fprintf(stderr, "Call to start_listening failed\n");
    perror(NULL);
    exit(-1);
  }

  fprintf(stderr, "Started listening.\n");

  while (1) {  

    // Accept new connection.
    if ((accept_fd = accept(listen_fd, (struct sockaddr *)&incoming, &addr_size)) == -1) {
      fprintf(stderr, "Call to accept() failed.\n");
      return(-1);
    }

    fcntl(accept_fd, F_SETFD, O_NONBLOCK);

    // Start a child with the new connection.
    if ((chld = fork()) == 0 ){
      guts(accept_fd, listen_fd);
    } else {
      close(accept_fd);
    }

  } // while (1)

  return(0);

}

void sigchld_handler(int s)
{
    while(waitpid(-1, NULL, WNOHANG) > 0);
}

void sigterm_handler_parent(int s)
{
  // If we get a sigterm, kill everyone.
  if ((killpg(0, SIGTERM)) == -1) {
    perror("Can't kill off my children. Don't know why.");
    exit(-1);
  } else {
    exit(0);
  };
}

void sigterm_handler_child(int s)
{
  // Clean up and exit.
  fprintf(stderr, "Got signal %d\n", s);
  cleanup_and_exit();
}

void cleanup_and_exit() {
  sem_post(DB_WRITE_LOCK);
  sem_post(IDX_WRITE_LOCK);
  sem_post(HASH_WRITE_LOCK);
  msync(SHM_BLOCK_BITMAP, BLOCK_BITMAP_BYTES, MS_SYNC);
  close(IDX_FD);
  close(DB_FD); 
  exit(0);
}

int start_listening(char* port, int backlog) {
  int listen_fd;
  struct addrinfo hints, *res, *p;          // Parms for socket() and bind() calls.
  int yes=1;
  int rc;

  memset(&hints, 0, sizeof(hints)); // Zero out hints.
  hints.ai_family = AF_UNSPEC;      // use IPv4 or IPv6.
  hints.ai_socktype = SOCK_STREAM;  // Normal TCP/IP reliable, buffered I/O.

  // Use getaddrinfo to allocate and populate *res.
  if (rc = getaddrinfo("::1", port, &hints, &res) != 0) { // Flesh out res. We use *res to supply the needed args to socket() and bind().
    fprintf(stderr, "The getaddrinfo() call failed with %d\n", rc);
    return(-1);
  }

  for (p = res; p != NULL; p = p->ai_next) {
    // Make a socket using the fleshed-out res structure:
    if ((listen_fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
      fprintf(stderr, "The socket call failed\n");
      continue;
    }
    // Bind the file descriptor to the port we passed in to getaddrinfo():
    if (bind(listen_fd, res->ai_addr, res->ai_addrlen) == -1) {
      fprintf(stderr, "The bind() call failed. listen_fd is %d\n", listen_fd);
      continue;
    }
    break; //We successfully bound to something. Stop looping.
  }

  // Start listening and put our listener in the connection list.
  if (listen(listen_fd, backlog) == -1) {
    perror("The listen() call failed.\n");
    return(-1);
  }

  return(listen_fd);

}

int extract_command(char* msg, int msglen) {

  char* commands[4] = { "create: ",           // 0
                        "read: ",             // 1
                        "delete: "};          // 2
  int i = 0;
  int cmdlen;
  int max_chars = 0;
  for (; i < 3; i++) {
    cmdlen = strlen(commands[i]);
    max_chars = msglen < cmdlen ? msglen : cmdlen;
    if (strncmp(commands[i], msg, max_chars) == 0) return(i);
  }
  
  return(-1);
}


void release_block_reservation(int block_offset, int blocks_used) {

  int j;

  sem_wait(DB_WRITE_LOCK);

  for (j = 0; j < blocks_used; j++) bit_array_clear(SHM_BLOCK_BITMAP, block_offset + j);  

  sem_post(DB_WRITE_LOCK);

}

int create_block_reservation(int blocks_needed) {
  // Finds an area of free blocks in our database file.

  int i,j;
  bool found = false;
  int retval = -1;

  sem_wait(DB_WRITE_LOCK);

  for (j = 0; j < MAX_BLOCKS; j++) {
    for (i = 0; i < blocks_needed; i++) { 
      if (bit_array_test(SHM_BLOCK_BITMAP, i + j) != 0) {// didn't find a contiguous block
        j += i;
        break; 
      }
    }
    if (i == blocks_needed) {
      found = true;
      break;
    }
  }

  if (found) {
    for (i = 0; i < blocks_needed; i++) // Found a good set of blocks. Mark them as used.
      bit_array_set(SHM_BLOCK_BITMAP, i + j);
    retval = j;
  }

  msync(SHM_BLOCK_BITMAP, BLOCK_BITMAP_BYTES, MS_SYNC); // commit the whole block bitmap to disk

  sem_post(DB_WRITE_LOCK);

  return(retval);
}

struct db_ptr find_db_ptr(char* key) {
  // returns an offset in the index for the given key.
  int     hash_id = get_hash_val(HASH_BITS, key);
  struct  idx index_rec = {};
  struct  db_ptr db_rec = {.block_offset = -1, .blocks = -1};
  int     result;
  int     pos  = hash_id * IDX_ENTRY_SIZE;

  while (1) {

    result = pread(IDX_FD, (void*)&index_rec, IDX_ENTRY_SIZE, pos);

    if (result == 0) {
      fprintf(stderr, "EOF encoutered unexpectedly.\n");
      return db_rec;
    }

    if (result < IDX_ENTRY_SIZE) { // Somehow the read failed.
      perror("index read failed in function find");
      return db_rec;
    } 

    if ((strncmp(key, index_rec.key, KEY_LEN - 1)) == 0)  {// found a match
      db_rec.block_offset = index_rec.block_offset;
      db_rec.blocks = index_rec.length;
      return db_rec;
    }

    if ((pos = index_rec.next) == 0) return db_rec; // return if no next record. Otherwise, keep looping.
    
  }

}

int find(char* key) {
  // returns an offset in the index for the given key.
  int     hash_id = get_hash_val(HASH_BITS, key);
  struct  idx index_rec = {};
  int     result;
  int     pos  = hash_id * IDX_ENTRY_SIZE;

  while (1) {

    result = pread(IDX_FD, (void*)&index_rec, IDX_ENTRY_SIZE, pos);

    if (result == 0) {
      fprintf(stderr, "EOF encoutered unexpectedly.\n");
      return -1;
    }

    if (result < IDX_ENTRY_SIZE) { // Somehow the read failed.
      perror("index read failed in function find");
      return -1;
    } 

    if ((strncmp(key, index_rec.key, KEY_LEN - 1)) == 0)  return pos; // found

    if ((pos = index_rec.next) == 0) return -2; // no next record.

  }

}
  
  
int write_index(char* key, int block_offset, int length) {

  int       hash_id = get_hash_val(HASH_BITS, key);
  struct    idx index_rec = {};
  struct    idx* index_rec_ptr;
  int       result;
  int       pos  = hash_id * IDX_ENTRY_SIZE;
  int       find_results = find(key);

  index_rec_ptr = &index_rec;

  if (find_results == -1) {
    fprintf(stderr, "find() failed when called from write_index. Don't know why.\n");
    return -1;
  }

  if (find_results > 0) {
    return -2; // record with this key already exists.
  }

  while (1) {
    result = pread(IDX_FD, (void*)index_rec_ptr, IDX_ENTRY_SIZE, pos);
    
    if (result == 0) {
      fprintf(stderr, "EOF encoutered unexpectedly.\n");
      return -1;
    }

    if (result < IDX_ENTRY_SIZE) { // Somehow the read failed.
      perror("index read failed in function write_index");
      return -1;
    } 

    // Determine if we can write on this index record, or do we need
    // to start looking down the chain.

    if (index_rec.key[0] == '\0') { // There is space here.
      // Don't change the 'next' field as it may point to addnl records in the chain.
      index_rec.block_offset = block_offset;
      index_rec.length = length;
      strncpy(index_rec_ptr->key, key, KEY_LEN - 1);
      pwrite(IDX_FD, (void*)index_rec_ptr, IDX_ENTRY_SIZE, pos); // write our key here.
      return 0;
    }
      
    // Since we are here, the test above failed. The current index record is in use.
    // If the 'next' pointer is 0, we can just create a new index record for ourself.
    

    if (index_rec.next == 0) { // no next index record in the chain. create one.

      // Lock the index file.
      // we don't want to compete with someone else for appending to the index file.
      if (sem_wait(IDX_WRITE_LOCK) == -1) {
        perror("call to sem_wait in write_index failed.\n");
        return(-1);
      }

      index_rec.next = lseek(IDX_FD, 0, SEEK_END);
      pwrite(IDX_FD, (void*)index_rec_ptr, IDX_ENTRY_SIZE, pos); // update current rec with pointer to next.
      pos = index_rec.next;
      fprintf(stderr, "pos is %d\n", pos);
      index_rec.next = 0;
      index_rec.block_offset = block_offset;
      index_rec.length = length;
      strncpy(index_rec_ptr->key, key, KEY_LEN - 1);
      pwrite(IDX_FD, (void*)index_rec_ptr, IDX_ENTRY_SIZE, pos); // add a new index entry.
      sem_post(IDX_WRITE_LOCK);
      return 0;
    }

    // Since we are here, we need to keep moving down the chain til we find a blank spot or the end.
    pos = index_rec.next; // move on to next record.

  }

}

char* read_record(struct db_ptr db_rec) {
  // read a record from the db file at the given offset.
  char* buffer;
  int byte_count = db_rec.blocks * BLOCK_SIZE;
  int byte_offset = db_rec.block_offset * BLOCK_SIZE;
  int bytes_read = 0;

  // Make a temporary buffer that is at least as big as the
  // data payload that we need to read. The buffer will be
  // zero-padded on the end.
  if ((buffer = malloc(byte_count)) == NULL) {
    perror("malloc failed in read_record()");
    return NULL;
  }

  bzero(buffer, byte_count);
  
  // write to the appropriate location in our file
  if ((bytes_read = pread(DB_FD, (void*)buffer, byte_count, byte_offset)) == -1) {
    perror("pread failed in read_record");
    free(buffer);
    return NULL;
  }
  
  return buffer;
}


int delete_record(char* key) {
  void*       buffer;
  int         byte_count = 0;
  int         block_offset;
  int         byte_offset;
  int         pos = 0;
  int         result;
  struct idx  index_rec;
  int         hash_id = get_hash_val(HASH_BITS, key);
  
  // lock this part of the key-space to make the delete atomic.
  // No one can be creating this key while we are deleting it..
  hash_write_lock(hash_id);

  pos = find(key);
  if (pos <= 0) {
    fprintf(stderr, "Call to find() failed with %d.\n", pos);
    return(-1);
  }
  
  // Fetch the index record so we can find the blocks to delete
  result = pread(IDX_FD, (void*)&index_rec, IDX_ENTRY_SIZE, pos);
  if (result == 0) {
    fprintf(stderr, "EOF encoutered unexpectedly.\n");
    return(-1);
  }
  if (result < IDX_ENTRY_SIZE) { // Somehow the read failed.
    perror("index read failed in function delete_record");
    return(-1);
  }

  index_rec.key[0] = '\0'; // NULL at the beginning of the key means it is free.
  result = pwrite(IDX_FD, (void*)&index_rec, IDX_ENTRY_SIZE, pos); // zap the key.
  hash_write_unlock(hash_id); // key is now deleted.

  byte_count = index_rec.length * BLOCK_SIZE;
  byte_offset = index_rec.block_offset * BLOCK_SIZE;

  // Make a temporary, zero-padded buffer that is at least as big as the
  // data payload that we need to erase. 
  if ((buffer = malloc(byte_count)) == NULL) {
    perror("malloc failed in delete_record()");
    return(-1);
  }

  bzero(buffer, byte_count);
  
  // write the zeros to the appropriate location in our file.
  if ((pwrite(DB_FD, buffer, byte_count, byte_offset)) == -1) {
    perror("pwrite failed in write_record");
    free(buffer);
    return(-1);
  }
  free(buffer);

  // Mark these blocks as usable again.
  release_block_reservation(index_rec.block_offset, index_rec.length);

  return(0);
}

int write_record(char* key, char* value) {

  int       len = strlen(value);
  div_t     qnr = div(len, BLOCK_SIZE);
  int       blocks = qnr.quot;
  void*     buffer;
  int       byte_count = 0;
  int       block_offset;
  int       byte_offset;
  int       index_result;
  int       hash_id = get_hash_val(HASH_BITS, key); 
  
  // lock this part of the key-space to make the write atomic.
  hash_write_lock(hash_id);

  // Figure out how many blocks we need and then requisition
  // them from the block bitmap table.
  if (qnr.rem > 0) blocks++; // round up to the next whole block.  
  if ((block_offset = create_block_reservation(blocks)) == -1) {
    fprintf(stderr, "Failed to reserve space in the block bitmap.\n");
    hash_write_unlock(hash_id);
    return(-1);
  }

  byte_count = blocks * BLOCK_SIZE;
  byte_offset = block_offset * BLOCK_SIZE;

  // Make a temporary buffer that is at least as big as the
  // data payload that we need to write. The buffer will be
  // zero-padded on the end.
  if ((buffer = malloc(byte_count)) == NULL) {
    perror("malloc failed in write_record()");
    // need to free blocks acquired
    hash_write_unlock(hash_id);
    return(-1);
  }

  bzero(buffer, byte_count);
  memcpy(buffer, value, len);
  
  // write to the appropriate location in our file
  if ((pwrite(DB_FD, buffer, byte_count, byte_offset)) == -1) {
    perror("pwrite failed in write_record");
    // need to free blocks acquired
    hash_write_unlock(hash_id);
    free(buffer);
    return(-1);
  }
  free(buffer);

  // Pass the key and the block offset to the index.
  index_result = write_index(key, block_offset, blocks);
  hash_write_unlock(hash_id);
  if (index_result == -2) {
    fprintf(stderr, "key already exists.\n");
    // need to free blocks acquired.
    return -2;
  }
  if (index_result == -1) {
    fprintf(stderr, "write_index failed in write_record..\n");
    // need to free blocks acquired. 
    return(-1);
  }

  return 0;
}
  

int guts(int accept_fd, int listen_fd) {
  
  char buffer[512]      = "";   // recv buffer
  char msg[MSG_SIZE]    = "";   // Holds our incoming and outgoing messages.
  void *msg_cursor;
  char response[MSG_SIZE]   = "";   
  int msglen            = 0;    // length of the assembled message that we receive.
  int recvlen           = 0;    // how many bytes recv call returns.
  int responselen       = 0;   
  int length            = 0;
  char* key;
  char* value;
  char* cmd_offset;
  struct db_ptr db_rec;
  int retval;
    

  // Re-register the sigterm handler to our cleanup function.
  signal(SIGTERM, sigterm_handler_child);
  close(listen_fd); // Close this resource from our parent. We don't need it any more.

  while (1) {

    msglen = 0;
    bzero(msg, MSG_SIZE);
    msg_cursor = (void*)msg;

    // Wait for some data
    while (((recvlen = recv(accept_fd, (void*)buffer, 512, MSG_PEEK)) == -1) && (errno == EAGAIN));
    if (recvlen == 0) {
      fprintf(stderr, "Client closed the connection.\n");
      close(accept_fd);
      cleanup_and_exit(0);
    };

    // Receive data from our buffered stream until we would block.
    while ((recvlen = recv(accept_fd, (void*)buffer, 512, 0)) != -1) {
      if (recvlen == 0) {
        fprintf(stderr, "Client closed the connection.\n");
        close(accept_fd);
        cleanup_and_exit(0);
      };

      if (recvlen == -1) {
        fprintf(stderr, "Got error %d from recv.\n", errno);
        close(accept_fd);
        cleanup_and_exit(-1);
      };

      if ((msglen += recvlen) > (MSG_SIZE)) {
        fprintf(stderr, "Message too big.\n");
        close(accept_fd);
        cleanup_and_exit(-1);
      }

      memcpy(msg_cursor, (void*)buffer, recvlen);
      msg_cursor += recvlen;
      if (memchr((void*)buffer, '\0', recvlen)) break; // Got a terminator character. Go process our message.

    } 
        
    switch (extract_command(msg, msglen))  {
      case 0: // create
        cmd_offset = &(msg[8]); // move beyond c-r-e-a-t-e-:-[space]
        key = strsep(&cmd_offset, "&");
        if ((cmd_offset == NULL) || (*key == '\0')) {
          sprintf(response, "Failed to extract key.\n");
          break;
        }

        if ((length = strnlen(key, KEY_LEN)) >= KEY_LEN) {
          sprintf(response, "Key exceeds %lu bytes.\n", KEY_LEN);
          break;
        }

        value = key + length + 1;
        if (*value == '\0') {
          sprintf(response, "No value supplied\n");
          break;
        }

        retval = write_record(key, value);
        if (retval == 0) {
          sprintf(response, "Write OK.\n", key, value);
        } else if (retval == -2) { // key already exists.
          sprintf(response, "Write failed. Key exists in the index.\n", key, value);
        } else {
          sprintf(response, "write_record() failed. Don't know why. key = %s value = %s\n", key, value);
        }
        break;

      case 1: // read
        key = &(msg[6]); // move beyond r-e-a-d-:[space]
        if (*key == '\0') {
          sprintf(response, "No key supplied\n");
          break;
        }
        db_rec = find_db_ptr(key); 
        if (db_rec.block_offset != -1) {
          value = read_record(db_rec);
          sprintf(response, "%s", value);
          free(value);
        } else {
          sprintf(response, "Not found.\n");
        }
        break;

      case 2:
        sprintf(response, "Got a delete command.\n");
        key = &(msg[8]); // move beyond d-e-l-e-t-e-:[space]
        if (*key == '\0') {
          sprintf(response, "No key supplied\n");
          break;
        }
        if (delete_record(key) == 0) {
          sprintf(response, "Delete OK.\n");
        } else {
          sprintf(response, "Delete failed.\n");
        } 
        break;

      default:
        sprintf(response, "Unknown command.\n");
      
    }

    responselen = strlen(response);
    if((send(accept_fd, (void*)response, responselen, 0) == -1)) perror("Send failed");

  };

  return(0);
}

int bit_array_set(char bit_array[], int bit) {

  int  byte_offset = floor(bit/8);
  int   bit_offset = bit % 8;
  int   cmp = 1 << bit_offset;

  return(bit_array[byte_offset] |= cmp);
}


int bit_array_test(char bit_array[], int bit) {
  // returns > 0 if set. returns 0 if the bit is clear.

  int   byte_offset = floor(bit/8);
  int   bit_offset = bit % 8;
  int   cmp = 1 << bit_offset;

  return(bit_array[byte_offset] & cmp);
}

int bit_array_clear(char bit_array[], int bit) {

  int  byte_offset = floor(bit/8);
  int   bit_offset = bit % 8;
  int   cmp = 1 << bit_offset;

  return(bit_array[byte_offset] &= (~cmp));
}

int get_hash_val(int bits, char* key) {

  int hval;
  int bit_mask;

  bit_mask =  (1 << bits) - 1;
  hval = fnv_32_str(key, FNV1_32_INIT);
  hval = hval & bit_mask;
  return(hval);
};


void hash_write_lock(int hash_number) {
  while (1) {
    sem_wait(HASH_WRITE_LOCK);
    if ((bit_array_test(SHM_HASHBUCKET_BITMAP, hash_number)) == 0) {
      bit_array_set(SHM_HASHBUCKET_BITMAP, hash_number);
      sem_post(HASH_WRITE_LOCK);
      break;
    }
    sem_post(HASH_WRITE_LOCK);
  }
}


void hash_write_unlock(int hash_number) {
  sem_wait(HASH_WRITE_LOCK);
  bit_array_clear(SHM_HASHBUCKET_BITMAP, hash_number);
  sem_post(HASH_WRITE_LOCK);

}


void usage(char *argv) {
  fprintf(stderr, "usage: %s [-h listen_addr] [-p listen_port] [-d /path/to/db/directory]\n", argv);
  exit(-1);
}


