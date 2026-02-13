#include <errno.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>

#define BUFF_SIZE 1024
#define TABLE_SIZE 1024

#define FNV_OFFSET 14695981039346656037UL
#define FNV_PRIME 1099511628211UL

#ifdef DEBUG
#define DEBUG_PRINT(fmt, ...)                                                  \
  fprintf(stderr, "[DEBUG] %s:%d:%s(): " fmt "\n", __FILE__, __LINE__,         \
          __func__, ##__VA_ARGS__)
#else
#define DEBUG_PRINT(fmt, ...) ((void)0)
#endif

typedef struct {
  char *key;
  char *val;
  int used;
  int deleted;
} kv_entry;

typedef struct {
  kv_entry entries[TABLE_SIZE];
} hashmap;

uint64_t fnv_hash(const char *x) {
  uint64_t hash = FNV_OFFSET;
  for (const char *p = x; *p; p++) {
    hash ^= (uint64_t)(unsigned char)(*p);
    hash *= FNV_PRIME;
  }
  return hash;
}

hashmap *hashmap_create() {
  hashmap *hm = (hashmap *)malloc(sizeof(hashmap));
  if (hm == NULL) {
    return NULL;
  }
  memset(hm->entries, 0, sizeof(hm->entries));
  return hm;
}

void hashmap_destroy(hashmap *hm) {
  for (size_t i = 0; i < TABLE_SIZE; i++) {
    if (hm->entries[i].used) {
      free(hm->entries[i].key);
      free(hm->entries[i].val);
    }
  }
  free(hm);
}

void hashmap_set(hashmap *hm, const char *key, const char *val) {
  uint64_t hash = fnv_hash(key);
  size_t idx = hash & (TABLE_SIZE - 1);

  for (size_t i = 0; i < TABLE_SIZE; i++) {
    size_t probe = (idx + i) & (TABLE_SIZE - 1);

    if (!hm->entries[probe].used || hm->entries[probe].deleted) {
      hm->entries[probe].key = strdup(key);
      hm->entries[probe].val = strdup(val);
      hm->entries[probe].used = 1;
      hm->entries[probe].deleted = 0;
      return;
    }

    // If we found the same key, update its value
    if (strcmp(hm->entries[probe].key, key) == 0) {
      free(hm->entries[probe].val);
      hm->entries[probe].val = strdup(val);
      return;
    }
  }
}

char *hashmap_get(hashmap *hm, const char *key) {
  uint64_t hash = fnv_hash(key);
  size_t idx = hash & (TABLE_SIZE - 1);

  for (size_t i = 0; i < TABLE_SIZE; i++) {
    size_t probe = (idx + i) & (TABLE_SIZE - 1);

    if (!hm->entries[probe].used) {
      return NULL;
    }

    if (hm->entries[probe].deleted) {
      continue;
    }

    if (strcmp(hm->entries[probe].key, key) == 0) {
      return hm->entries[probe].val;
    }
  }

  return NULL;
}

void hashmap_delete(hashmap *hm, const char *key) {
  uint64_t hash = fnv_hash(key);
  size_t idx = hash & (TABLE_SIZE - 1);

  for (size_t i = 0; i < TABLE_SIZE; i++) {
    size_t probe = (idx + i) & (TABLE_SIZE - 1);

    if (!hm->entries[probe].used) {
      return;
    }

    if (strcmp(hm->entries[probe].key, key) == 0) {
      free(hm->entries[probe].key);
      free(hm->entries[probe].val);
      hm->entries[probe].deleted = 1;
      return;
    }
  }
}

void handle_pkt(int fd, hashmap *hm) {

  char buf[BUFF_SIZE];
  char *msg, *cmd, *key, *val, *reply;
  size_t off = 0;
  size_t len;

  // Extract length
  while (off < 8) {
    ssize_t r = read(fd, buf + off, 1);
    if (r == 0)
      return; // No date
    if (r < 0) {
      if (errno == EINTR)
        continue;
      else {
        perror("read() failed");
        exit(1);
      }
    }
    if (buf[off] == ':')
      break;
    off += r;
  }
  buf[off] = '\0';
  len = atol(buf);
  DEBUG_PRINT("Message length: %zu", len);

  msg = malloc((len + 1) * sizeof(char));
  off = 0;

  while (off < len) {
    ssize_t r = read(fd, msg + off, len - off);
    if (r == 0)
      break;
    if (r < 0) {
      if (errno == EINTR)
        continue;
      else {
        perror("read() failed");
        exit(1);
      }
    }
    off += r;
  }
  msg[off] = '\0';

  DEBUG_PRINT("Message received: %s", msg);

  cmd = strtok(msg, ":");
  reply = NULL;
  if (strcmp(cmd, "get") == 0) {
    if ((key = strtok(NULL, ":"))) {
      reply = hashmap_get(hm, key);
      DEBUG_PRINT("Get: %s", key);
    }
  } else if (strcmp(cmd, "set") == 0) {
    if ((key = strtok(NULL, ":")) && (val = strtok(NULL, ":"))) {
      hashmap_set(hm, key, val);
      DEBUG_PRINT("Set: %s -> %s", key, val);
    }
  } else if (strcmp(cmd, "del") == 0) {
    if ((key = strtok(NULL, ":"))) {
      hashmap_delete(hm, key);
      DEBUG_PRINT("Del: %s", key);
    }
  }

  if (reply) {
    write(fd, reply, strlen(reply));
    DEBUG_PRINT("Reply: %s", reply);
  }

  free(msg);
}

static volatile sig_atomic_t done = 0;

static void handle_sig(int sig) { done = 1; }

void usage(const char *prog) {
  fprintf(stderr,
          "%s <port> <timeout>\n"
          "\n"
          "  port     TCP port number\n"
          "  timeout  Time in seconds (non-positive = run forever)\n",
          prog);
}

int main(int argc, char *argv[]) {

  unsigned port;
  int timeout;
  int sockfd, connfd = -1;
  socklen_t len;
  struct sockaddr_in servaddr, cli;

  hashmap *hm = hashmap_create();

  if (argc != 3) {
    usage(argv[0]);
    exit(1);
  }

  port = atoi(argv[1]);
  timeout = atoi(argv[2]);

  DEBUG_PRINT("port: %d", port);
  DEBUG_PRINT("timeout: %d", timeout);

  if (timeout > 0) {
    signal(SIGALRM, handle_sig);
    alarm(timeout);
  }

  signal(SIGTERM, handle_sig);

  if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    perror("socket() failed");
    exit(1);
  }
  DEBUG_PRINT("socket() succeeded");

  bzero(&servaddr, sizeof(servaddr));

  servaddr.sin_family = AF_INET;
  servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
  servaddr.sin_port = htons(port);

  if ((bind(sockfd, (struct sockaddr *)&servaddr, sizeof(servaddr))) < 0) {
    perror("bind() failed");
    exit(1);
  }
  DEBUG_PRINT("bind() succeeded");

  if ((listen(sockfd, 5)) < 0) {
    perror("listen() failed");
    exit(1);
  }
  DEBUG_PRINT("listen() succeeded");
  len = sizeof(cli);

  while (!done) {
    connfd = accept(sockfd, (struct sockaddr *)&cli, &len);
    if (connfd < 0) {
      if (errno == EINTR) {
        continue;
      }
      perror("accept() failed");
      exit(1);
    }
    DEBUG_PRINT("accept() succeeded");
    handle_pkt(connfd, hm);
    close(connfd);
    connfd = -1;
  }

  if (connfd >= 0) {
    close(connfd);
  }
  close(sockfd);

  hashmap_destroy(hm);

  return 0;
}
