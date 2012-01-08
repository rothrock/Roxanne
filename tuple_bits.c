#include "roxanne_db.h"

int keydb_lock(int64_t pos) {

  return 0;
}

int keydb_unlock(int64_t pos) {

  return 0;
}

struct keydb_node* key_buf_read(int fd, int64_t pos) {

  int64_t n;
  struct keydb_node *buffer;

  if ((buffer = malloc(sizeof(struct keydb_node))) == NULL) {
    perror("Call to malloc() failed in keydb_find.\n");
    return NULL;
  }
  bzero(buffer, sizeof(struct keydb_node));

  n = pread(fd, buffer, sizeof(struct keydb_node), pos);

  if (n == -1) {
    perror("pread() failed in keydb_find.\n");
    free(buffer);
    return NULL;
  }

  if (n == 0) { // nothing here. We're at EOF.
    free(buffer);
    return(NULL);
  }

  if (buffer->refcount == 0) { // This record is tombstoned.
    free(buffer);
    return(NULL);
  }

  return buffer;
}

  
struct keydb_column* keydb_tree(int fd, int64_t pos) {
  // Return a linked list of keys (stored as struct keydb_column)
  // found in the tree pointed at by pos. The caller must free the list.
  
  struct keydb_node* parent;
  struct keydb_node* buffer;
  struct keydb_column *left, *mid, *right;
  int64_t next_pos;
  int64_t n;

  buffer = key_buf_read(fd, pos);

  if (buffer == NULL)  return(NULL);

  // OK, there may really be something here.
  if ((mid = malloc(sizeof(struct keydb_column))) == NULL) {
    perror("Call to malloc() failed in keydb_tree.\n");
    return NULL;
  }
  memcpy(mid, buffer->column, KEY_LEN);
  left = keydb_tree(fd, buffer->left);
  right = keydb_tree(fd, buffer->right);

  mid->next = right;
  left->next = mid;
  free(buffer);
  return left;

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
     
}  


int composite_insert(int fd, struct keydb_column *tuple) {
  int pos = 0;

  pos = keydb_insert(fd, tuple->column, pos, false);
  if (pos == -1) return -1;
  tuple = tuple->next;  

  while (tuple) {
    pos = keydb_insert(fd, tuple->column, pos, true);
    if (pos == -1) return -1;
    tuple = tuple->next;
  };

  return 0;
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
  struct stat stat_info;

  keydb_lock(pos);
  if ((buffer = malloc(sizeof(struct keydb_node))) == NULL) {
    perror("Call to malloc() failed in keydb_insert.\n");
    keydb_unlock(pos);
    return -1;
  }

  bzero(buffer, sizeof(struct keydb_node));

  n = pread(fd, buffer, sizeof(struct keydb_node), pos);
  if (n == -1) {
    perror("pread() failed in keydb_insert.\n");
    keydb_unlock(pos);
    free(buffer);
    return -1;
  }


  if (go_next) {
    if (n == 0) { // We can't go 'next' on a zero-length file.
      keydb_unlock(pos);
      fprintf(stderr, "pos is at EOF but we need to read a real record.\n");
      free(buffer);
      return -1;
    }
    if (buffer->next == 0) { // create our node and return it's position.
      if ((next_pos = lseek(fd, 0, SEEK_END)) == -1) {
        perror("lseek failed in keydb_insert\n");
        keydb_unlock(pos);
        free(buffer);
        return -1;
      }

      keydb_lock(next_pos);
      buffer->next = next_pos;
      pwrite(fd, buffer, sizeof(struct keydb_node), pos);
      bzero(buffer, sizeof(struct keydb_node));
      strcpy(buffer->column, column);
      buffer->refcount = 1;
      pwrite(fd, buffer, sizeof(struct keydb_node), next_pos);
      keydb_unlock(next_pos);
      free(buffer);
      return next_pos;

    } else { // insert our node in the tree that next points to.
      next_pos = buffer->next;
      free(buffer);
      keydb_unlock(pos);
      return(keydb_insert(fd, column, next_pos, false));
    }
  }

  //Since we're here, We should write our node into this particular tree. (go_next is false).

  if (n == 0) { // nothing here. zero-length file. Just write and leave.
    
    memcpy(buffer->column, column, KEY_LEN);
    buffer->refcount = 1;
    pwrite(fd, buffer, sizeof(struct keydb_node), pos);
    keydb_unlock(pos);
    free(buffer);
    return pos;
  }

  // Start looking for a place to insert our new node

  comparison = strcmp(buffer->column, column);
  
  if (comparison > 0) { // node on disk is bigger, we need to go left.

    if (buffer->left != 0) { // go try to insert on the left node.
      keydb_unlock(pos);
      pos = buffer->left;
      free(buffer);
      return keydb_insert(fd, column, pos, false);

    } else { // There is no left node. Make one.
      if ((next_pos = lseek(fd, 0, SEEK_END)) == -1) {
        perror("lseek failed in keydb_insert\n");
        keydb_unlock(pos);
        free(buffer);
        return -1;
      }
      keydb_lock(next_pos);
      buffer->left = next_pos;
      pwrite(fd, buffer, sizeof(struct keydb_node), pos);
      bzero(buffer, sizeof(struct keydb_node));
      strcpy(buffer->column, column);
      buffer->refcount = 1;
      pwrite(fd, buffer, sizeof(struct keydb_node), next_pos);
      keydb_unlock(next_pos);
      free(buffer);
      return next_pos;
      }

  } else if (comparison < 0) { // node on disk is smaller, we need to go right.

    if (buffer->right != 0) { // go try to insert on the right node.
      keydb_unlock(pos);
      pos = buffer->right;
      free(buffer);
      return keydb_insert(fd, column, pos, false);

    } else { // There is no left node. Make one.
      if ((next_pos = lseek(fd, 0, SEEK_END)) == -1) {
        perror("lseek failed in keydb_insert\n");
        keydb_unlock(pos);
        free(buffer);
        return -1;
      }
      keydb_lock(next_pos);
      buffer->right = next_pos;
      pwrite(fd, buffer, sizeof(struct keydb_node), pos);
      bzero(buffer, sizeof(struct keydb_node));
      strcpy(buffer->column, column);
      buffer->refcount = 1;
      pwrite(fd, buffer, sizeof(struct keydb_node), next_pos);
      keydb_unlock(next_pos);
      free(buffer);
      return next_pos;
      }

  } else { // we match the node here. Simply up the refcount.

    buffer->refcount++;
    pwrite(fd, buffer, sizeof(struct keydb_node), pos);
    keydb_unlock(pos);
    free(buffer);
    return pos;
  
  }
  
  return 0;
}

