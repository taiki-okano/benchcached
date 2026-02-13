#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define KEY_MAX 64
#define VAL_MAX 128

typedef struct {
  char *key;
  char *val;
  int used;
  int deleted;
} kv_entry;

typedef struct {
  kv_entry *entries;
  size_t cap;
} hashmap;

typedef struct {
  uint64_t count;
  uint64_t total_ns;
} metric;

static void usage(const char *prog) {
  fprintf(stderr,
          "%s <requests> <keyspace>\n"
          "\n"
          "Workload mix:\n"
          "  get: 70%%\n"
          "  set: 20%%\n"
          "  del: 10%%\n"
          "\n"
          "Example:\n"
          "  %s 500000 1024\n",
          prog, prog);
}

static uint64_t now_ns(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static uint64_t fnv_hash(const char *s) {
  uint64_t hash = 14695981039346656037ULL;
  while (*s) {
    hash ^= (uint64_t)(unsigned char)(*s++);
    hash *= 1099511628211ULL;
  }
  return hash;
}

static size_t next_pow2(size_t x) {
  size_t p = 1;
  while (p < x) {
    p <<= 1;
  }
  return p;
}

static hashmap *hashmap_create(size_t expected_items) {
  hashmap *hm;
  size_t cap = next_pow2(expected_items * 2 + 1);

  hm = (hashmap *)malloc(sizeof(*hm));
  if (!hm) {
    return NULL;
  }

  hm->entries = (kv_entry *)calloc(cap, sizeof(kv_entry));
  if (!hm->entries) {
    free(hm);
    return NULL;
  }

  hm->cap = cap;
  return hm;
}

static void hashmap_destroy(hashmap *hm) {
  size_t i;
  if (!hm) {
    return;
  }
  for (i = 0; i < hm->cap; i++) {
    if (hm->entries[i].used && !hm->entries[i].deleted) {
      free(hm->entries[i].key);
      free(hm->entries[i].val);
    }
  }
  free(hm->entries);
  free(hm);
}

static int hashmap_set(hashmap *hm, const char *key, const char *val) {
  size_t i;
  size_t idx = (size_t)(fnv_hash(key) & (uint64_t)(hm->cap - 1));
  ssize_t first_tomb = -1;

  for (i = 0; i < hm->cap; i++) {
    size_t probe = (idx + i) & (hm->cap - 1);
    kv_entry *e = &hm->entries[probe];

    if (!e->used) {
      kv_entry *dst;
      if (first_tomb >= 0) {
        dst = &hm->entries[(size_t)first_tomb];
      } else {
        dst = e;
      }
      dst->key = strdup(key);
      dst->val = strdup(val);
      if (!dst->key || !dst->val) {
        free(dst->key);
        free(dst->val);
        dst->key = NULL;
        dst->val = NULL;
        dst->used = 0;
        dst->deleted = 0;
        return -1;
      }
      dst->used = 1;
      dst->deleted = 0;
      return 0;
    }

    if (e->deleted) {
      if (first_tomb < 0) {
        first_tomb = (ssize_t)probe;
      }
      continue;
    }

    if (strcmp(e->key, key) == 0) {
      char *new_val = strdup(val);
      if (!new_val) {
        return -1;
      }
      free(e->val);
      e->val = new_val;
      return 0;
    }
  }

  return -1;
}

static const char *hashmap_get(const hashmap *hm, const char *key) {
  size_t i;
  size_t idx = (size_t)(fnv_hash(key) & (uint64_t)(hm->cap - 1));

  for (i = 0; i < hm->cap; i++) {
    size_t probe = (idx + i) & (hm->cap - 1);
    const kv_entry *e = &hm->entries[probe];

    if (!e->used) {
      return NULL;
    }
    if (e->deleted) {
      continue;
    }
    if (strcmp(e->key, key) == 0) {
      return e->val;
    }
  }

  return NULL;
}

static void hashmap_delete(hashmap *hm, const char *key) {
  size_t i;
  size_t idx = (size_t)(fnv_hash(key) & (uint64_t)(hm->cap - 1));

  for (i = 0; i < hm->cap; i++) {
    size_t probe = (idx + i) & (hm->cap - 1);
    kv_entry *e = &hm->entries[probe];

    if (!e->used) {
      return;
    }
    if (e->deleted) {
      continue;
    }
    if (strcmp(e->key, key) == 0) {
      free(e->key);
      free(e->val);
      e->key = NULL;
      e->val = NULL;
      e->deleted = 1;
      return;
    }
  }
}

static void record(metric *m, uint64_t elapsed_ns) {
  m->count++;
  m->total_ns += elapsed_ns;
}

int main(int argc, char *argv[]) {
  long requests;
  long keyspace;
  long i;
  metric get_m = {0, 0}, set_m = {0, 0}, del_m = {0, 0};
  uint64_t start_ns, end_ns;
  uint64_t failures = 0;
  unsigned rng = 0x9e3779b9U;
  hashmap *hm;

  if (argc != 3) {
    usage(argv[0]);
    return 1;
  }

  requests = atol(argv[1]);
  keyspace = atol(argv[2]);
  if (requests <= 0 || keyspace <= 0) {
    usage(argv[0]);
    return 1;
  }

  hm = hashmap_create((size_t)keyspace);
  if (!hm) {
    fprintf(stderr, "failed to allocate hashmap\n");
    return 1;
  }

  printf("Standalone benchmark\n");
  printf("Requests: %ld, Keyspace: %ld\n", requests, keyspace);

  for (i = 0; i < keyspace; i++) {
    char key[KEY_MAX];
    char val[VAL_MAX];
    snprintf(key, sizeof(key), "k%ld", i);
    snprintf(val, sizeof(val), "v%ld", i);
    if (hashmap_set(hm, key, val) < 0) {
      failures++;
    }
  }

  // start_ns = now_ns();

  for (i = 0; i < requests; i++) {
    uint32_t key_id;
    uint32_t bucket;
    char key[KEY_MAX];
    char val[VAL_MAX];
    uint64_t t0, t1;

    rng = rng * 1664525U + 1013904223U;
    bucket = rng % 100;

    rng = rng * 1664525U + 1013904223U;
    key_id = rng % (uint32_t)keyspace;

    snprintf(key, sizeof(key), "k%u", key_id);

    if (bucket < 70) {
      t0 = now_ns();
      (void)hashmap_get(hm, key);
      t1 = now_ns();
      record(&get_m, t1 - t0);
    } else if (bucket < 90) {
      snprintf(val, sizeof(val), "v%u", key_id ^ rng);
      t0 = now_ns();
      if (hashmap_set(hm, key, val) < 0) {
        failures++;
      }
      t1 = now_ns();
      record(&set_m, t1 - t0);
    } else {
      t0 = now_ns();
      hashmap_delete(hm, key);
      t1 = now_ns();
      record(&del_m, t1 - t0);
    }
  }

  // end_ns = now_ns();

  {
    // uint64_t total_ns = end_ns - start_ns;
    // double seconds = (double)total_ns / 1e9;
    // double rps = (double)requests / seconds;

    printf("\nResults\n");
    // printf("  Total time: %.3f s\n", seconds);
    // printf("  Throughput: %.0f ops/s\n", rps);
    printf("  Failures: %llu\n", (unsigned long long)failures);

    // if (get_m.count) {
    //   printf("  GET avg: %.3f us (%llu ops)\n",
    //          ((double)get_m.total_ns / (double)get_m.count) / 1e3,
    //          (unsigned long long)get_m.count);
    // }
    // if (set_m.count) {
    //   printf("  SET avg: %.3f us (%llu ops)\n",
    //          ((double)set_m.total_ns / (double)set_m.count) / 1e3,
    //          (unsigned long long)set_m.count);
    // }
    // if (del_m.count) {
    //   printf("  DEL avg: %.3f us (%llu ops)\n",
    //          ((double)del_m.total_ns / (double)del_m.count) / 1e3,
    //          (unsigned long long)del_m.count);
    // }
  }

  hashmap_destroy(hm);
  return failures == 0 ? 0 : 2;
}
