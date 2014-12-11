#include "roxanne_db.h"

void keydb_lock(int64_t pos) {

  char key[1024] = "";
  int hash_number;

  sprintf(key, "%llu", pos); // turn pos into a string.
  hash_number = get_hash_val(10, key); // 2^10 = 1024. See SHM_KEYDB_BITMAP.

  while (1) {
    sem_wait(KEYDB_LOCK);
    if ((bit_array_test(SHM_KEYDB_BITMAP, hash_number)) == 0) {
      bit_array_set(SHM_KEYDB_BITMAP, hash_number);
      sem_post(KEYDB_LOCK);
      break;
    }
    sem_post(KEYDB_LOCK);
  }

}

void keydb_unlock(int64_t pos) {

  char key[1024] = "";
  int hash_number;

  sprintf(key, "%llu", pos); // turn pos into a string.
  hash_number = get_hash_val(10, key); // 2^10 = 1024. See SHM_KEYDB_BITMAP.

  sem_wait(KEYDB_LOCK);
  bit_array_clear(SHM_KEYDB_BITMAP, hash_number);
  sem_post(KEYDB_LOCK);

}

struct keydb_node* key_buf_read(int fd, int64_t pos) {
  // Fetches the keydb record from disk located at position 'pos'.
  // Returns a pointer to a keydb_node that must be freed
  // by the caller if the pointer is not NULL.
  //
  // Returns NULL on failure to read.

  int n;
  struct keydb_node *buffer;

  if ((buffer = malloc(sizeof(struct keydb_node))) == NULL) {
    perror("Call to malloc() failed in key_buf_read.\n");
    return NULL;
  }
  bzero(buffer, sizeof(struct keydb_node));

  n = pread(fd, buffer, sizeof(struct keydb_node), pos);

  if (n == -1) {
    perror("pread() failed in key_buf_read.\n");
    free(buffer);
    return NULL;
  }

  if (n == 0) { // nothing here. We're at EOF.
    free(buffer);
    return(NULL);
  }


  return buffer;
}


void* keydb_tree(int fd, int64_t pos, struct keydb_column **list) {
  // Return a linked list of keys (stored as struct keydb_column)
  // found in the tree pointed at by pos. The caller must free the list.

  struct keydb_node* parent;
  struct keydb_node* buffer;
  struct keydb_column *mid = NULL;
  int64_t next_pos;
  int64_t n;

  buffer = key_buf_read(fd, pos);

  if (buffer == NULL) return NULL;

  if ((mid = malloc(sizeof(struct keydb_column))) == NULL) {
    perror(NULL);
    cleanup_and_exit;
  }
  mid->next = NULL;

  if (buffer->refcount <= 0) { //this record is tombstoned.
    mid->column[0] = '\0'; // Put a terminator at the start of the string.
  } else {
    memcpy(mid->column, buffer->column, KEY_LEN); // otherwise load it up.
    mid->refcount = buffer->refcount;
  }

  // Right
  if (buffer->right != 0) keydb_tree(fd, buffer->right, list);

  // Middle
  mid->next = *list;
  *list = mid;

  // Left
  if (buffer->left  != 0) keydb_tree(fd, buffer->left, list);

  free(buffer);

  return NULL;
}


struct keydb_node* keydb_find(int fd, char *key, int64_t pos) {
  // finds the node that matches key in the tree pointed at by pos.
  // returns a pointer that must be freed by the caller.

  int64_t n;
  struct keydb_node *buffer;
  int cmp;

  buffer = key_buf_read(fd, pos);

  if (buffer == NULL) return NULL;

  cmp = strncmp(buffer->column, key, KEY_LEN);

  if (cmp == 0) {
    if (pos != buffer->pos) fprintf(stderr, "positions don't match.\n");
    return(buffer);
  } else if (cmp < 0) { // Go right
    pos = buffer->right;
    free(buffer);
    if (pos != 0)  return(keydb_find(fd, key, pos));
  } else { // Go left
    pos = buffer->left;
    free(buffer);
    if (pos != 0) return(keydb_find(fd, key, pos));
  }

  return NULL;
}

int composite_delete(int fd, struct keydb_column *tuple) {
  // Reduces the refcounts of the given list of key components into the keydb.

  int64_t pos = 0;
  struct keydb_node *node;

  //sem_wait(KEYDB_LOCK);
  while (tuple) {
    keydb_lock(pos); // Lock the tree we're deleting from.
    node = keydb_find(fd, tuple->column, pos);
    if (node == NULL) {
      //sem_post(KEYDB_LOCK);
      keydb_unlock(pos);
      return -1;
    }
    if (node->refcount == 0) { // nothing here. We can't lower refcount below zero.
      free(node);
      //sem_post(KEYDB_LOCK);
      keydb_unlock(pos);
      return(-1);
    }
    keydb_unlock(pos);
    pos = node->pos;
    keydb_lock(pos);
    node->refcount--;
    if ((pwrite(fd, node, sizeof(struct keydb_node), pos)) == -1) {
      fprintf(stderr, "Problem writing node at %llu in composite_delete().\n", pos);
      keydb_unlock(pos);
      return(-1);
      free(node);
    };
    keydb_unlock(pos);
    pos = node->next;
    tuple = tuple->next;
    free(node);

  }

  //sem_post(KEYDB_LOCK);
  return 0;
}


int composite_insert(int fd, struct keydb_column *tuple) {
  // Inserts the given list of key components into the keydb.

  int64_t pos = 0;
  int64_t previous_pos = 0;

  keydb_lock(pos);
  previous_pos = pos;
  pos = keydb_insert(fd, tuple->column, pos, false);
  keydb_unlock(previous_pos);
  if (pos == -1) {
    return -1;
  }
  tuple = tuple->next;

  while (tuple) {
    keydb_lock(pos);
    previous_pos = pos;
    pos = keydb_insert(fd, tuple->column, pos, true);
    keydb_unlock(previous_pos);
    if (pos == -1){
      return -1;
    }
    tuple = tuple->next;
  };

  return 0;
}


int new_subkey_tree(int fd, char column[], int64_t pos, struct keydb_node *buffer) {
  // Create a new subkey tree below the given keydb_node.

  int64_t next_pos;

  if ((next_pos = find_free_key_node(fd)) == -1) {
    fprintf(stderr, "find_free_key_node_failed.\n");
    free(buffer);
    return -1;
  }
  buffer->next = next_pos;
  if (pwrite(fd, buffer, sizeof(struct keydb_node), pos) == -1) {
    perror("pwrite() failed in new_subkey_tree.");
    free(buffer);
    return -1;
  }
  bzero(buffer, sizeof(struct keydb_node));
  strcpy(buffer->column, column);
  buffer->refcount = 1;
  buffer->pos = next_pos;
  if (pwrite(fd, buffer, sizeof(struct keydb_node), next_pos) == -1) {
    perror("pwrite() failed in new_subkey_tree.");
    free(buffer);
    return -1;
  }
  free(buffer);
  return next_pos;
}


int keydb_insert(int fd, char column[], int64_t pos, bool go_next) {
  // inserts a node in the keydb tree. The go_next flag determines
  // whether or not the key goes into the binary tree pointed at by
  // the record stored at pos, or instead into the 'next' binary
  // tree pointed at by the record at pos.
  // returns the offset in the file where the insert occurred.

  int n;
  int comparison;
  int64_t next_pos;
  struct keydb_node *buffer;

  if ((buffer = malloc(sizeof(struct keydb_node))) == NULL) {
    perror("Call to malloc() failed in keydb_insert.");
    return -1;
  }

  bzero(buffer, sizeof(struct keydb_node));

  n = pread(fd, buffer, sizeof(struct keydb_node), pos);
  if (n == -1) {
    perror("pread() failed in keydb_insert.");
    free(buffer);
    return -1;
  }

  if (go_next) {
    if (n == 0) { // We can't go 'next' off a record that doesn't exist.
      fprintf(stderr, "pos is at EOF but we need to read a real record.\n");
      free(buffer);
      return -1;
    }
    if (buffer->next == 0) {

      return(new_subkey_tree(fd, column, pos, buffer));

    } else { // insert our node in the tree that next points to. We're
             // adding a subkey to an existing tree. Calling ourself with
             // go_next set to false moves us to the logic below.
      next_pos = buffer->next;
      free(buffer);
      return(keydb_insert(fd, column, next_pos, false));
    }
  }

  //Since we're here, We should write our node into this particular tree. (go_next is false).

  if (n == 0) { // nothing here. zero-length file. Just write and leave.

    memcpy(buffer->column, column, KEY_LEN);
    buffer->refcount = 1;
    buffer->pos = pos;
    if (pwrite(fd, buffer, sizeof(struct keydb_node), pos) == -1) {
      perror("pwrite() failed in keydb_insert.");
      free(buffer);
      return -1;
    }
    free(buffer);
    return pos;
  }

  // Start looking for a place to insert our new node

  comparison = strcmp(buffer->column, column);

  if (comparison > 0) { // node on disk is bigger, we need to go left.

    if (buffer->left != 0) { // need to keep going left.

      pos = buffer->left;
      free(buffer);
      return keydb_insert(fd, column, pos, false);

    } else { // There is no left node. Make one.

      return connect_and_add_node(LEFT, buffer, column, pos, fd);

    }

  } else if (comparison < 0) { // node on disk is smaller, we need to go right.

    if (buffer->right != 0) { // need to keep going right

      pos = buffer->right;
      free(buffer);
      return keydb_insert(fd, column, pos, false);

    } else { // There is no right node. Make one.

      return connect_and_add_node(RIGHT, buffer, column,  pos, fd);
    }

  } else { // we match the node here. Simply up the refcount.

    buffer->refcount++;
    if (pwrite(fd, buffer, sizeof(struct keydb_node), pos) == -1) {
      perror("pwrite() failed in keydb_insert.");
      free(buffer);
      return -1;
    }
    free(buffer);
    return pos;

  }

  return 0;
}


int find_free_key_node(int fd) {
  // The hope is that one day there will be a freelist of returned keydb
  // blocks that we re-use. For now, this is a placeholder.

  uint64_t pos = 0;
  struct keydb_node buffer = {};
  if (pos == 0) { // Nothing on the freelist. Add at the end of the keydb file.
    if ((pos = lseek(fd, 0, SEEK_END)) == -1) {
      perror("lseek failed in find_free_key_node\n");
      return -1;
    } else {
      //extend the file by one buffer length.
      if (pwrite(fd, &buffer, sizeof(buffer), pos) == -1) {
        perror("pwrite() failed in find_free_key_node.");
        return -1;
      }
      return pos;
    }
  }

};

int keydb_txlog_append(int64_t pos) {

  return 0;
}


int connect_and_add_node(int direction, struct keydb_node* buffer, char column[], int pos, int fd) {
  // update the current node with a link to the new
  // child node that we will also create.

  int next_pos;

  if ((next_pos = find_free_key_node(fd)) == -1) {
    fprintf(stderr, "find_free_key_node_failed.\n");
    free(buffer);
    return -1;
  }

  if (direction == LEFT) {
    buffer->left = next_pos;
  }  else {
    buffer->right = next_pos;
  }

  if ((pwrite(fd, buffer, sizeof(struct keydb_node), pos)) == -1) { // point current node to new child.
    perror("Call to pwrite failed in connect_and_add_node.\n");
    return -1;
  }
  bzero(buffer, sizeof(struct keydb_node));
  memcpy(buffer->column, column, KEY_LEN);
  buffer->refcount = 1;
  buffer->pos = next_pos;
  if ((pwrite(fd, buffer, sizeof(struct keydb_node), next_pos)) == -1) { // create the new child.
    perror("Call to pwrite failed in connect_and_add_node.\n");
    free(buffer);
    return -1;
  }
  free(buffer);
  return next_pos;
}
