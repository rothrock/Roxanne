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

#include "roxanne_db.h"
#include "status_codes.h"

char DATA_HOME[4096] = "/var/roxanne";

int main(int argc, char* argv[]) {

  struct sockaddr incoming;
  socklen_t addr_size = sizeof(incoming);
  int listen_fd, accept_fd;
  char* port = "4080";
  char* host = "::1";
  char keydb_file[4096];
  char keydb_freelist[4096];
  char db_file[4096];
  char idx_file[4096];
  char block_bitmap_file[4096];
  int chld;
  int i;
  int ch;


  // parse our cmd line args
  while ((ch = getopt(argc, argv, "d:h:p:")) != -1) {
    switch (ch) {

      case 'd':
        sprintf(DATA_HOME, "%s", optarg);
        break;

      case 'h':
        host = optarg;
        break;

      case 'p':
        port = optarg;
        break;

     case '?':

     default:
       usage(argv[0]);
     }
  }
  argc -= optind;
  argv += optind;

  sprintf(keydb_file, "%s/keydb", DATA_HOME);
  sprintf(keydb_freelist, "%s/keydb_freelist", DATA_HOME);
  sprintf(db_file, "%s/db", DATA_HOME);
  sprintf(idx_file, "%s/idx", DATA_HOME);
  sprintf(block_bitmap_file, "%s/block_bitmap", DATA_HOME);


  // Used to coordinate exclusive access to the block bitmap.
  if ((BLOCK_BITMAP_LOCK = sem_open("block_bitmap_lock", O_CREAT, 0666, 1)) == SEM_FAILED) {
    perror("semaphore init failed");
    exit(-1);
  }
  sem_post(BLOCK_BITMAP_LOCK);

  // Used to coordinate exclusive access to the bitmap array of keydb locks.
  if ((KEYDB_LOCK = sem_open("keydb_lock", O_CREAT, 0666, 1)) == SEM_FAILED) {
    perror("semaphore init failed");
    exit(-1);
  }
  sem_post(KEYDB_LOCK);

  // This is used to safely append to the end of the index file.
  if ((IDX_APPEND_LOCK = sem_open("idx_lock", O_CREAT, 0666, 1)) == SEM_FAILED) {
    perror("semaphore init failed");
    exit(-1);
  }
  sem_post(IDX_APPEND_LOCK);

  // Used to coordinate exclusive access to the bitmap array of hash key space locks.
  if ((HASHBUCKET_LOCK = sem_open("hashbucket_lock", O_CREAT, 0666, 1)) == SEM_FAILED) {
    perror("semaphore init failed");
    exit(-1);
  }
  sem_post(HASHBUCKET_LOCK);

  // Memory-map our block bitmap file creating it if necessary.
  // The block bitmap keeps track of free/busy blocks in the db file.
  if ((BLOCK_BITMAP_FD = open(block_bitmap_file, O_RDWR | O_CREAT, 0666)) == -1) {
    fprintf(stderr, "Couldn't open block bitmap file %s\n", block_bitmap_file);
    perror(NULL);
    exit(-1);
  }
  if ((SHM_BLOCK_BITMAP = mmap((caddr_t)0, BLOCK_BITMAP_BYTES, PROT_READ | PROT_WRITE, MAP_SHARED, BLOCK_BITMAP_FD, 0)) == MAP_FAILED) {
    perror("Problem mmapping the block bitmap");
    exit(-1);
  }

  // A mem-mapped block of anonymous memory used to lock parts of the database index.
  // See hash_write_lock() and hash_write_unlock()
  if ((SHM_HASHBUCKET_BITMAP = mmap((caddr_t)0, ((1<<HASH_BITS)/8), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0)) == MAP_FAILED) {
    perror("Problem mmapping the hash bitmap");
    exit(-1);
  }

  // A mem-mapped block of anonymous memory used to lock parts of the keydb tree.
  // See keydb_lock() and keydb_unlock()
  if ((SHM_KEYDB_BITMAP = mmap((caddr_t)0, ((KEYDB_LOCKS)/8), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0)) == MAP_FAILED) {
    perror("Problem mmapping the keydb lock bitmap");
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

  // Open our keydb file
  if ((KEYDB_FD = open(keydb_file, O_RDWR | O_CREAT, 0666)) == -1) {
    fprintf(stderr, "Couldn't open key file named %s\n", keydb_file);
    perror(NULL);
    exit(-1);
  }

  // Open our keydb freelist
  if ((KEYDB_FREELIST_FD = open(keydb_freelist, O_RDWR | O_CREAT, 0666)) == -1) {
    fprintf(stderr, "Couldn't open key file named %s\n", keydb_freelist);
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
  if ((listen_fd = start_listening(host, port, BACKLOG)) == -1) {
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
  sem_post(BLOCK_BITMAP_LOCK);
  sem_post(IDX_APPEND_LOCK);
  sem_post(HASHBUCKET_LOCK);
  msync(SHM_BLOCK_BITMAP, BLOCK_BITMAP_BYTES, MS_SYNC);
  close(IDX_FD);
  close(DB_FD);
  exit(0);
}

int start_listening(char* host, char* port, int backlog) {
  int listen_fd;
  struct addrinfo hints, *res, *p;          // Parms for socket() and bind() calls.
  int yes=1;
  int rc;

  memset(&hints, 0, sizeof(hints)); // Zero out hints.
  hints.ai_family = AF_UNSPEC;      // use IPv4 or IPv6.
  hints.ai_socktype = SOCK_STREAM;  // Normal TCP/IP reliable, buffered I/O.

  // Use getaddrinfo to allocate and populate *res.
  if (rc = getaddrinfo(host, port, &hints, &res) != 0) { // Flesh out res. We use *res to supply the needed args to socket() and bind().
    fprintf(stderr, "The getaddrinfo() call failed with %d\n", rc);
    return(-1);
  }

  for (p = res; p != NULL; p = p->ai_next) {
    // Make a socket using the fleshed-out res structure:
    if ((listen_fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
      fprintf(stderr, "The socket call failed\n");
      return(-1);
    }
    // Bind the file descriptor to the port we passed in to getaddrinfo():
    if (bind(listen_fd, res->ai_addr, res->ai_addrlen) == -1) {
      fprintf(stderr, "The bind() call failed. listen_fd is %d\n", listen_fd);
      return(-1);
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

int tokenize_command(char* msg, char* token_vector[]) {
  char **token_ptr;
  int i = 0;
  for (token_ptr = token_vector; (*token_ptr = strsep(&msg, " ")) != NULL;)
    if (**token_ptr != '\0') {
        i++;
        if (++token_ptr >= &token_vector[MAX_ARGS])
          return -1;
    }
  return i;
}


int extract_command(char *token_vector[], int token_count) {

  char* commands[5] = { "quit",   // 0
                        "create", // 1
                        "read",   // 2
                        "delete", // 3
                        "keys"    // 4
                      };
  int i = 0;
  if (token_count < 1) return -1;
  for (; i < 5; i++)
    if (strcmp(commands[i], token_vector[0]) == 0) return(i);
  return -1;
}


void release_block_reservation(int block_offset, int blocks_used) {

  int j;

  sem_wait(BLOCK_BITMAP_LOCK);

  for (j = 0; j < blocks_used; j++) bit_array_clear(SHM_BLOCK_BITMAP, block_offset + j);

  sem_post(BLOCK_BITMAP_LOCK);

}

int create_block_reservation(int blocks_needed) {
  // Finds an area of free blocks in our database file.

  int i,j;
  bool found = false;
  int retval = -1;

  sem_wait(BLOCK_BITMAP_LOCK);

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

  sem_post(BLOCK_BITMAP_LOCK);

  return(retval);
}

struct db_ptr find_db_ptr(char* key) {
  // Attempts to find the key in the hash table and return a structure
  // that points to the record in the db file.
  int       hash_id = get_hash_val(HASH_BITS, key);
  struct    idx index_rec = {};
  struct    db_ptr db_rec = {.block_offset = -1, .blocks = -1};
  int       result;
  int64_t   pos  = hash_id * IDX_ENTRY_SIZE;

  while (1) {

    result = pread(IDX_FD, (void*)&index_rec, IDX_ENTRY_SIZE, pos);

    if (result == 0) {
      fprintf(stderr, "EOF encoutered unexpectedly.\n");
      return db_rec;
    }

    if (result < IDX_ENTRY_SIZE) { // Somehow the read failed.
      perror("index read failed in function find_db_ptr");
      return db_rec;
    }

    if ((memcmp(key, index_rec.key, KEY_LEN)) == 0)  {// found a match
      db_rec.block_offset = index_rec.block_offset;
      db_rec.blocks = index_rec.length;
      return db_rec;
    }

    if ((pos = index_rec.next) == 0) return db_rec; // return if no next record. Otherwise, keep looping.

  }

}

int find(char* key) {
  // returns an offset in the index for the given key.
  int       hash_id = get_hash_val(HASH_BITS, key);
  struct    idx index_rec = {};
  int       result;
   int64_t  pos  = hash_id * IDX_ENTRY_SIZE;

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

    if ((memcmp(key, index_rec.key, KEY_LEN)) == 0)  return pos; // found

    if ((pos = index_rec.next) == 0) return -2; // no next record.

  }

}


int write_index(char* key, int block_offset, int length) {

  int         hash_id = get_hash_val(HASH_BITS, key);
  struct      idx index_rec = {};
  struct      idx* index_rec_ptr;
  int         result;
  int64_t     pos  = hash_id * IDX_ENTRY_SIZE;
  int         find_results = find(key);

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
      if (sem_wait(IDX_APPEND_LOCK) == -1) {
        perror("call to sem_wait in write_index failed.\n");
        return(-1);
      }

      index_rec.next = lseek(IDX_FD, 0, SEEK_END);
      pwrite(IDX_FD, (void*)index_rec_ptr, IDX_ENTRY_SIZE, pos); // update current rec with pointer to next.
      pos = index_rec.next;
      index_rec.next = 0;
      index_rec.block_offset = block_offset;
      index_rec.length = length;
      strncpy(index_rec_ptr->key, key, KEY_LEN - 1);
      pwrite(IDX_FD, (void*)index_rec_ptr, IDX_ENTRY_SIZE, pos); // add a new index entry.
      sem_post(IDX_APPEND_LOCK);
      return 0;
    }

    // Since we are here, we need to keep moving down the chain til we find a blank spot or the end.
    pos = index_rec.next; // move on to next record.

  }

}

char* read_record(struct db_ptr db_rec) {
  // read a record from the db file at the given offset.
  char*     buffer;
  int       byte_count = db_rec.blocks * BLOCK_SIZE;
   int64_t  byte_offset = db_rec.block_offset * BLOCK_SIZE;
  int       bytes_read = 0;

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
  int64_t     byte_offset;
  int64_t     pos = 0;
  int         result;
  struct idx  index_rec;
  int         hash_id = get_hash_val(HASH_BITS, key);

  // lock this part of the key-space to make the delete atomic.
  // No one can be creating this key while we are deleting it.
  hash_write_lock(hash_id);

  pos = find(key);

  if (pos == -1) {
    fprintf(stderr, "Call to find() failed with %lld.\n", pos);
    hash_write_unlock(hash_id);
    return(-1);
  }

  if (pos == -2) {
    hash_write_unlock(hash_id);
    return(-2); // Not in the index
  }

  // Fetch the index record so we can find the blocks to delete
  result = pread(IDX_FD, (void*)&index_rec, IDX_ENTRY_SIZE, pos);
  if (result == 0) {
    fprintf(stderr, "EOF encoutered unexpectedly.\n");
    hash_write_unlock(hash_id);
    return(-1);
  }
  if (result < IDX_ENTRY_SIZE) { // Somehow the read failed.
    perror("index read failed in function delete_record");
    hash_write_unlock(hash_id);
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
    perror("pwrite failed in delete_record");
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
  int64_t   byte_offset;
  int       index_result;
  int       hash_id = get_hash_val(HASH_BITS, key);
  int       find_result;

  // lock this part of the key-space to make the write atomic.
  hash_write_lock(hash_id);

  if (find(key) > 0)  { // Bail if we've got a same-key collision.
    hash_write_unlock(hash_id);
    return -2;
  }

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
    release_block_reservation(block_offset, blocks);
    hash_write_unlock(hash_id);
    return(-1);
  }

  bzero(buffer, byte_count);
  memcpy(buffer, value, len);

  // write to the appropriate location in our file
  if ((pwrite(DB_FD, buffer, byte_count, byte_offset)) == -1) {
    perror("pwrite failed in write_record");
    release_block_reservation(block_offset, blocks);
    hash_write_unlock(hash_id);
    free(buffer);
    return(-1);
  }
  free(buffer);

  // Pass the key and the block offset to the index.
  index_result = write_index(key, block_offset, blocks);
  hash_write_unlock(hash_id);
  if (index_result < 0) {
    fprintf(stderr, "write_index failed in write_record with %d.\n", index_result);
    release_block_reservation(block_offset, blocks);
    return(-1);
  }

  return 0;
}


int guts(int accept_fd, int listen_fd) {

  char buffer[RECV_WINDOW] = "";   // recv buffer
  int msgbuflen = MSG_SIZE;
  char status_msg[MSG_SIZE];
  char *msg; // Incoming message.
  char *send_msg; // Outgoing message.
  char *tmp_msg;
  void *msg_cursor;
  struct response_struct response;
  int msglen = 0; // length of the assembled message that we receive.
  int recvlen = 0; // how many bytes recv call returns.
  int responselen = 0;
  int offset;
  int retval;
  char* token_vector[MAX_ARGS] = {'\0'};
  int token_count = 0;

  // Re-register the sigterm handler to our cleanup function.
  signal(SIGTERM, sigterm_handler_child);
  close(listen_fd); // Close this resource from our parent. We don't need it any more.

  while (1) {

    msglen = 0;
    msgbuflen = MSG_SIZE;
    msg = malloc(sizeof(char) * msgbuflen);
    msg_cursor = (void*)msg;
    bzero(msg, msgbuflen);


    // Wait for some data
    while (((recvlen = recv(accept_fd, (void*)buffer, RECV_WINDOW, MSG_PEEK)) == -1) && (errno == EAGAIN));
    if (recvlen == 0) {
      fprintf(stderr, "Client closed the connection.\n");
      close(accept_fd);
      cleanup_and_exit(0);
    };

    // Receive data from our buffered stream until we would block.
    while ((recvlen = recv(accept_fd, (void*)buffer, RECV_WINDOW, 0)) != -1) {
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

      // Extend our message buffer if need be.
      if ((msglen += recvlen) > (msgbuflen)) {
        msgbuflen += msgbuflen;
        offset = msg_cursor - (void*)msg;
        tmp_msg = malloc(sizeof(char) * msgbuflen);
        bzero(tmp_msg, msgbuflen);
        memcpy(tmp_msg, msg, offset);
        msg_cursor = tmp_msg + offset;
        free(msg);
        msg = tmp_msg;
        fprintf(stderr, "msgbuflen expanded to %d\n", msgbuflen);
      }

      memcpy(msg_cursor, (void*)buffer, recvlen);
      msg_cursor += recvlen;
      if (memchr((void*)buffer, '\n', recvlen)) break; // Got a terminator character. Go process our message.

    }

    tmp_msg = msg;
    strsep(&tmp_msg, "\r\n");

    token_count = tokenize_command(msg, token_vector);

    switch (extract_command(token_vector, token_count))  {

      case 0: // quit
        cleanup_and_exit();
        break;

      case 1: // create
        response = create_command(token_vector, token_count);
        break;

      case 2: // read
        response = read_command(token_vector, token_count);
        break;

      case 3: // delete
        response = delete_command(token_vector, token_count);
        break;

      case 4: // subkeys
        response = keys_command(token_vector, token_count);
        break;

      default:
        if ((response.msg = malloc(sizeof(char) * MSG_SIZE)) == NULL) {
          perror(NULL);
          cleanup_and_exit;
        }
        bzero(response.msg, MSG_SIZE);
        sprintf(response.msg, "Unknown command.");
        response.status = 1;
    }

    responselen = prepare_send_msg(response, &send_msg);

    if((send(accept_fd, (void*)send_msg, responselen, 0) == -1)) perror("Send failed");
    free(msg);
    free(response.msg);
    free(send_msg);

  };

  return(0);
}

int prepare_send_msg(struct response_struct response, char** send_msg) {
  char status_msg[MSG_SIZE] = { '\0' };
  int responselen;

  sprintf(status_msg, "STATUS: %s\nSIZE: %d\n",
    STATUS_CODES[response.status],
    (int)strlen(response.msg));
  responselen = strlen(response.msg) + strlen(status_msg) + 2;
  if ((*send_msg = malloc(responselen)) == NULL) {
    perror(NULL);
    cleanup_and_exit();
  }
  *send_msg[0] = '\0';
  strcat(*send_msg, status_msg);
  strcat(*send_msg, response.msg);
  strcat(*send_msg, "\n\n");
  return responselen;
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
    sem_wait(HASHBUCKET_LOCK);
    if ((bit_array_test(SHM_HASHBUCKET_BITMAP, hash_number)) == 0) {
      bit_array_set(SHM_HASHBUCKET_BITMAP, hash_number);
      sem_post(HASHBUCKET_LOCK);
      break;
    }
    sem_post(HASHBUCKET_LOCK);
  }
}


void hash_write_unlock(int hash_number) {
  sem_wait(HASHBUCKET_LOCK);
  bit_array_clear(SHM_HASHBUCKET_BITMAP, hash_number);
  sem_post(HASHBUCKET_LOCK);

}

struct response_struct create_command(char* token_vector[], int token_count) {

  int length            = 0;
  int i = 0;
  char* part            = NULL;
  char* previous_part   = NULL;
  int retval            = 0;
  char key[KEY_LEN]     = "";
  struct keydb_column *tuple = NULL;
  struct keydb_column *head = NULL;
  struct keydb_column *tmp;
  struct response_struct response;

  response.status = 0;

  if ((response.msg = malloc(sizeof(char) * MSG_SIZE)) == NULL) {
    perror(NULL);
    cleanup_and_exit();
  }
  bzero(response.msg, MSG_SIZE);

  if (token_count < 3) {
    response.status = 1;
    sprintf(response.msg, "Not enough arguments.");
    return response;
  }

  for (i = 1; (part = token_vector[i]) && (i < MAX_ARGS); i++) {

    if (previous_part != NULL) {
      length += strlen(previous_part);
      if (length > KEY_LEN - 1) {
        response.status = 1;
        sprintf(response.msg, "Key too large.");
        return response;
      }

      // Save away the list of key composites
      if ((tmp = malloc(sizeof(struct keydb_column))) == NULL) {
        perror(NULL);
        cleanup_and_exit;
      }
      strncpy(tmp->column, previous_part, KEY_LEN);
      tmp->next = NULL;
      if (tuple == NULL) {
        tuple = tmp;
        head = tmp;
      } else {
        tuple->next = tmp;
        tuple = tuple->next;
        tuple->next = NULL;
      }
      strcat(key, previous_part);

    }
    previous_part = part;
  }

  if (key[0] == '\0') {
    response.status = 1;
    sprintf(response.msg, "Failed to get value.");
    return response;
  }

  retval = write_record(key, previous_part);
  if (retval == 0) {
    if (composite_insert(KEYDB_FD, head) == -1) {
      delete_record(key); // undo what we did.
      fprintf(stderr, "Composite key insertion failed.");
      response.status = 1;
      sprintf(response.msg, "Internal error.");
    } else {
      sprintf(response.msg, "Write OK.");
    }
  } else if (retval == -2) { // key already exists.
    response.status = 1;
    sprintf(response.msg, "Write failed. Key exists in the index.");
  } else {
    response.status = 1;
    sprintf(response.msg, "Internal error.");
  }

  while (head) { // free our list of key composites.
    tmp = head->next;
    free(head);
    head = tmp;
  };
  return response;
}

struct response_struct read_command(char* token_vector[], int token_count) {

  char* part = NULL;
  char* value;
  int retval = 0;
  char key[KEY_LEN] = "";
  int length = 0;
  struct db_ptr db_rec;
  int responselen = 0;
  int i;
  struct response_struct response;

  response.status = 0;

  response.msg = malloc(sizeof(char) * MSG_SIZE);
  bzero(response.msg, MSG_SIZE);

  if (token_count == 1) {
    sprintf(response.msg, "No keys supplied.");
    response.status = 1;
    return response;
  }

  for (i = 1; token_vector[i] && i < MAX_ARGS; i++) {
    strcat(key, token_vector[i]);
    if (strlen(key) > KEY_LEN - 1) {
      sprintf(response.msg, "Key too long.");
      response.status = 1;
      return response;
    }
  }

  db_rec = find_db_ptr(key);
  if (db_rec.block_offset != -1) {
    value = read_record(db_rec);
    responselen = strlen(value);
    if (responselen >= MSG_SIZE) { // need to expand response.
      free(response.msg);
      response.msg = malloc(sizeof(char) * (responselen + 2));
    }
    sprintf(response.msg, "%s", value);
    free(value);
  } else {
    sprintf(response.msg, "Not found.");
    response.status = 1;
  }
  return response;
}

struct response_struct delete_command(char* token_vector[], int token_count){

  char* part            = NULL;
  char key[KEY_LEN]     = "";
  int length            = 0;
  int i = 0;
  int retval;
  struct keydb_column *tuple = NULL;
  struct keydb_column *head = NULL;
  struct keydb_column *tmp;
  int responselen = 0;
  struct response_struct response;

  response.status = 0;

  if ((response.msg = malloc(sizeof(char) * MSG_SIZE)) == NULL) {
    perror(NULL);
    cleanup_and_exit();
  }

  if (token_count < 2) {
    response.status = 1;
    sprintf(response.msg, "Not enough arguments");
    return response;
  }

  for (i = 1; (part = token_vector[i]) && (i < MAX_ARGS); i++) {
    length += strlen(part);
    if (length > KEY_LEN - 1) {
      response.status = 1;
      sprintf(response.msg, "Key too large");
      return response;
    }

    // Save away the list of key composites
    if ((tmp = malloc(sizeof(struct keydb_column))) == NULL) {
      perror(NULL);
      cleanup_and_exit();
    }
    strncpy(tmp->column, part, KEY_LEN);
    tmp->next = NULL;
    if (tuple == NULL) {
      tuple = tmp;
      head = tmp;
    } else {
      tuple->next = tmp;
      tuple = tuple->next;
      tuple->next = NULL;
    }
    strcat(key, part);
  }

  if (length == 0) {
    response.status = 1;
    sprintf(response.msg, "Failed to extract key.");
    return response;
  }

  retval = delete_record(key);

  if (retval == 0) {
    if (composite_delete(KEYDB_FD, head) == -1) {
      fprintf(stderr, "Composite key delete failed.");
      response.status = 1;
      sprintf(response.msg, "Internal Error.");
    } else {
      sprintf(response.msg, "Delete OK.");
    }
  } else if (retval == -2) {
    response.status = 1;
    sprintf(response.msg, "Not found.");
  } else {
    response.status = 1;
    sprintf(response.msg, "Could not delete record.");
  }

  while (head) { // free our list of key composites.
    tmp = head->next;
    free(head);
    head = tmp;
  };
  return response;
}

struct response_struct keys_command(char* token_vector[], int token_count) {

  char* part = NULL;
  char key[KEY_LEN] = "";
  int length = 0;
  int i = 0;
  int retval;
  int64_t pos = 0;
  struct keydb_node *node;
  struct keydb_column *list, *tmp, *cursor;
  bool some_content = false; // Does our key list have any keys in it?
  list = NULL;
  char* tmp_response;
  int response_free_bytes = MSG_SIZE;
  int responselen = 0;
  int column_size;
  struct response_struct response;

  response.status = 0;
  if ((response.msg = malloc(sizeof(char) * MSG_SIZE)) == NULL) {
    perror(NULL);
    cleanup_and_exit();
  }
  bzero(response.msg, MSG_SIZE);

  for (i = 1; (part = token_vector[i]) && (i < MAX_ARGS); i++) {
    length += strlen(part);
    if (length > KEY_LEN - 1) {
      response.status = 1;
      sprintf(response.msg, "Key too large.");
      return response;
    }

    node = keydb_find(KEYDB_FD, part, pos);

    if (!(node = keydb_find(KEYDB_FD, part, pos))) {
      response.status = 1;
      sprintf(response.msg, "Not found.");
      return response;
    }

    if (node->refcount <= 0) {
      response.status = 1;
      sprintf(response.msg, "Not found.");
      return response;
    }

    pos = node->next;
    free(node);
    if (pos == 0) { // There is no next subtree.
      response.status = 1;
      sprintf(response.msg, "No subkeys.");
      return response;
    }

  }

  keydb_tree(KEYDB_FD, pos, &list);
  while (list) {
    if (list->column[0] != '\0') {
      column_size = strlen(list->column) + 2;
      response_free_bytes -= column_size;
      if (response_free_bytes < 1) { // need to expand response.
        responselen = strlen(response.msg);
        responselen += column_size;
        tmp_response = malloc(sizeof(char) * responselen);
        strcpy(tmp_response, response.msg);
        free(response.msg);
        response.msg = tmp_response;
        response_free_bytes = 0;
      }
      strcat(response.msg, list->column);
      strcat(response.msg, " ");
      some_content = true;
    }
    tmp = list->next;
    free(list);
    list = tmp;
  }

  if (!some_content) {
    sprintf(response.msg, "No subkeys.");
    response.status = 1;
  }
  response.msg[strlen(response.msg) - 1] = '\0'; // Knock out that last extra space.
  return response;
}

void usage(char *argv) {
  fprintf(stderr, "usage: %s [-h listen_addr] [-p listen_port] [-d /path/to/db/directory]\n", argv);
  exit(-1);
}

int keydb_txlog_reset() {
  // Creates a transaction log file if one doesn't exist.
  // Otherwise it truncates the transaction log file.

  int retval;
  int pid = getpid();
  char log_file[4096];

  snprintf(log_file, (4096 - sizeof(int)), "%s/%d.txlog", DATA_HOME, pid);

 if ((retval = open(log_file, O_TRUNC | O_APPEND | O_CREAT, 0666)) == -1) {
      fprintf(stderr, "Couldn't open tx_log %s\n", log_file);
      perror(NULL);
      return -1;
  }

  return retval;
}
