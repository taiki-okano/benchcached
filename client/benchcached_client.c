#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#define KEY_MAX 64
#define VAL_MAX 128
#define BODY_MAX 256
#define REPLY_MAX 256

typedef struct {
  uint64_t count;
  uint64_t total_ns;
} metric;

static void usage(const char *prog) {
  fprintf(stderr,
          "%s <host> <port> <requests> <keyspace>\n"
          "\n"
          "Workload mix:\n"
          "  get: 70%%\n"
          "  set: 20%%\n"
          "  del: 10%%\n"
          "\n"
          "Example:\n"
          "  %s 127.0.0.1 12345 50000 1024\n",
          prog, prog);
}

static uint64_t now_ns(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static int send_all(int fd, const char *buf, size_t len) {
  size_t off = 0;
  while (off < len) {
    ssize_t w = write(fd, buf + off, len - off);
    if (w < 0) {
      if (errno == EINTR) {
        continue;
      }
      return -1;
    }
    off += (size_t)w;
  }
  return 0;
}

static int connect_to(const char *host, int port) {
  struct sockaddr_in addr;
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    return -1;
  }

  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons((uint16_t)port);

  if (inet_pton(AF_INET, host, &addr.sin_addr) != 1) {
    close(fd);
    return -1;
  }

  if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
    close(fd);
    return -1;
  }

  return fd;
}

static int send_cmd(const char *host, int port, const char *body, int want_reply,
                    char *reply_buf, size_t reply_cap) {
  char packet[512];
  int fd;
  size_t body_len = strlen(body);
  int n = snprintf(packet, sizeof(packet), "%zu:%s", body_len, body);
  if (n <= 0 || (size_t)n >= sizeof(packet)) {
    return -1;
  }

  fd = connect_to(host, port);
  if (fd < 0) {
    return -1;
  }

  if (send_all(fd, packet, (size_t)n) < 0) {
    close(fd);
    return -1;
  }

  if (want_reply) {
    struct timeval tv = {.tv_sec = 0, .tv_usec = 200000};
    if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
      close(fd);
      return -1;
    }

    ssize_t r = read(fd, reply_buf, reply_cap - 1);
    if (r < 0) {
      if (errno == EWOULDBLOCK || errno == EAGAIN) {
        reply_buf[0] = '\0';
      } else {
        close(fd);
        return -1;
      }
    } else if (r == 0) {
      reply_buf[0] = '\0';
    } else {
      reply_buf[r] = '\0';
    }
  }

  close(fd);
  return 0;
}

static void record(metric *m, uint64_t elapsed_ns) {
  m->count++;
  m->total_ns += elapsed_ns;
}

int main(int argc, char *argv[]) {
  const char *host;
  int port;
  long requests;
  long keyspace;
  long i;
  metric get_m = {0, 0}, set_m = {0, 0}, del_m = {0, 0};
  uint64_t start_ns, end_ns;
  uint64_t failures = 0;
  unsigned rng = 0x9e3779b9U;

  if (argc != 5) {
    usage(argv[0]);
    return 1;
  }

  host = argv[1];
  port = atoi(argv[2]);
  requests = atol(argv[3]);
  keyspace = atol(argv[4]);

  if (port <= 0 || requests <= 0 || keyspace <= 0) {
    usage(argv[0]);
    return 1;
  }

  printf("Target: %s:%d\n", host, port);
  printf("Requests: %ld, Keyspace: %ld\n", requests, keyspace);

  // Warm-up and populate keys so get has a hit rate.
  for (i = 0; i < keyspace; i++) {
    char key[KEY_MAX];
    char val[VAL_MAX];
    char body[BODY_MAX];

    snprintf(key, sizeof(key), "k%ld", i);
    snprintf(val, sizeof(val), "v%ld", i);
    snprintf(body, sizeof(body), "set:%s:%s", key, val);

    if (send_cmd(host, port, body, 0, NULL, 0) < 0) {
      failures++;
    }
  }

  start_ns = now_ns();

  for (i = 0; i < requests; i++) {
    uint32_t key_id;
    uint32_t bucket;
    char key[KEY_MAX];
    char val[VAL_MAX];
    char body[BODY_MAX];
    char reply[REPLY_MAX];
    uint64_t t0, t1;

    // Tiny LCG for reproducible pseudo-random workload.
    rng = rng * 1664525U + 1013904223U;
    bucket = rng % 100;

    rng = rng * 1664525U + 1013904223U;
    key_id = rng % (uint32_t)keyspace;

    snprintf(key, sizeof(key), "k%u", key_id);

    if (bucket < 70) {
      snprintf(body, sizeof(body), "get:%s", key);
      t0 = now_ns();
      if (send_cmd(host, port, body, 1, reply, sizeof(reply)) < 0) {
        failures++;
      }
      t1 = now_ns();
      record(&get_m, t1 - t0);
    } else if (bucket < 90) {
      snprintf(val, sizeof(val), "v%u", key_id ^ rng);
      snprintf(body, sizeof(body), "set:%s:%s", key, val);
      t0 = now_ns();
      if (send_cmd(host, port, body, 0, NULL, 0) < 0) {
        failures++;
      }
      t1 = now_ns();
      record(&set_m, t1 - t0);
    } else {
      snprintf(body, sizeof(body), "del:%s", key);
      t0 = now_ns();
      if (send_cmd(host, port, body, 0, NULL, 0) < 0) {
        failures++;
      }
      t1 = now_ns();
      record(&del_m, t1 - t0);
    }
  }

  end_ns = now_ns();

  {
    uint64_t total_ns = end_ns - start_ns;
    double seconds = (double)total_ns / 1e9;
    double rps = (double)requests / seconds;

    printf("\nResults\n");
    printf("  Total time: %.3f s\n", seconds);
    printf("  Throughput: %.0f ops/s\n", rps);
    printf("  Failures: %llu\n", (unsigned long long)failures);

    if (get_m.count) {
      printf("  GET avg: %.3f us (%llu ops)\n",
             ((double)get_m.total_ns / (double)get_m.count) / 1e3,
             (unsigned long long)get_m.count);
    }
    if (set_m.count) {
      printf("  SET avg: %.3f us (%llu ops)\n",
             ((double)set_m.total_ns / (double)set_m.count) / 1e3,
             (unsigned long long)set_m.count);
    }
    if (del_m.count) {
      printf("  DEL avg: %.3f us (%llu ops)\n",
             ((double)del_m.total_ns / (double)del_m.count) / 1e3,
             (unsigned long long)del_m.count);
    }
  }

  return failures == 0 ? 0 : 2;
}
