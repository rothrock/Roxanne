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
#include <sys/stat.h>
#include <semaphore.h>
#include <signal.h>
#include <math.h>
#include <stdint.h>
#include <stdbool.h>
#include <libgen.h>
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
#define LEFT 0
#define RIGHT 1
#define BACKLOG 25
#define BLOCK_SIZE 4096
#define MAX_BLOCKS 1073741824
#define BLOCK_BITMAP_BYTES 134217728

// Default starting message size.
// MSG_SIZE must be larger than RECV_WINDOW
#define MSG_SIZE 1024
#define RECV_WINDOW 512
#define HASH_BITS 16
#define IDX_ENTRY_SIZE 1024
#define KEY_LEN (IDX_ENTRY_SIZE - 2*(sizeof(int)) - sizeof(int64_t))
#define KEYDB_LOCKS 1024
#define MAX_ARGS 100

struct response_struct {
  unsigned int status;
  char* msg;
};


struct idx { // structure for an index record.
  char      key[KEY_LEN];
  int       block_offset; // starting block in the db file.
  int       length;       // db blocks consumed.
  int64_t   next;         // overflow ptr to next index_record on disk.
};


struct  db_ptr { // a structure that points to a value in the db file.
  int64_t   block_offset;
  int       blocks;
};


struct keydb_column {
  char      column[KEY_LEN];
  int       refcount;
  struct    keydb_column *next;
};


struct keydb_node {
  char      column[KEY_LEN];
  int       refcount;
  int64_t   left;
  int64_t   right;
  int64_t   next;
  int64_t   pos; // file position of this record.
};


// Globals
sem_t*          BLOCK_BITMAP_LOCK;
sem_t*          HASHBUCKET_LOCK;
sem_t*          KEYDB_LOCK;
sem_t*          IDX_APPEND_LOCK;
char            *SHM_BLOCK_BITMAP;
char            *SHM_HASHBUCKET_BITMAP;
char            *SHM_KEYDB_BITMAP;
int             BLOCK_BITMAP_FD;
int             KEYDB_FD;
int             KEYDB_FREELIST_FD;
int             DB_FD;
int             IDX_FD;



// Function signatures
int       start_listening(char* host, char* port, int backlog);
void      sigchld_handler(int s);
void      sigterm_handler_parent(int s);
void      sigterm_handler_child(int s);
int       get_hash_val(int bits, char* key);
int       guts(int accept_fd, int listen_fd);
int       extract_command(char *token_vector[], int token_count);
int       tokenize_command(char* msg, char* token_vector[]);
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
void      cleanup_and_exit(int retval);
void      usage(char *argv);
struct response_struct create_command(char* token_vector[], int token_count);
struct response_struct read_command(char* token_vector[], int token_count);
struct response_struct delete_command(char* token_vector[], int token_count);
struct response_struct keys_command(char* token_vector[], int token_count);
int       keydb_insert(int fd, char column[], int64_t pos, bool go_next);
void      keydb_lock(int64_t pos);
void      keydb_unlock(int64_t pos);
int       composite_insert(int KEYDB_FD, struct keydb_column *tuple);
int				composite_delete(int fd, struct keydb_column *tuple);
struct    keydb_node* keydb_find(int fd, char *key, int64_t pos);
void*     keydb_tree(int fd, int64_t pos, struct keydb_column **list);
int       find_free_key_node(int keydb_fd);
int       connect_and_add_node(int direction, struct keydb_node* buffer, char column[], int pos, int fd);
int       new_subkey_tree(int fd, char column[], int64_t pos, struct keydb_node *buffer);
int       prepare_send_msg(struct response_struct response, char** send_msg);
