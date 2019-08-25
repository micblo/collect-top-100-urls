//
// Created by Peiran Luo on 2019/8/24.
//

#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <pthread/pthread.h>
#include <limits.h>
#include "reduce.h"
#include "utils.h"

typedef struct {
    Context *context;
    int bucket_id;
} Job;

#define JOB_BUCKET ( job->context->buckets[job->bucket_id])

// Count of ran jobs
int ran_job_cnt = 0;

int traversing_map(char *key, any_t value, void *ctx) {
    Job *job = ctx;
    char *old_url;
    size_t old_n;

    if (heap_size(&JOB_BUCKET.hp) >= job->context->top_n) {
        if (heap_min(&JOB_BUCKET.hp, (void **) &old_url, (void **) &old_n) == 1) {
            // Ignore lessen URL
            if (old_n > (size_t) value) {
                return MAP_OK;
            }
        }
    }
    heap_insert(&JOB_BUCKET.hp, key, (void *) value);

    return MAP_OK;
}

int compare_size_values(void* _v1, void* _v2) {
    // Cast them as int* and read them in
    size_t v1 = (size_t) _v1;
    size_t v2 = (size_t) _v2;

    // Perform the comparison
    if (v1 < v2)
        return -1;
    else if (v1 == v2)
        return 0;
    else
        return 1;
}

void traversing(void *_job) {
    Job *job = _job;

    struct timeval starttime, endtime;
    size_t len;

    if (job->context->verbosity) {
        printf("bucket %d is reducing\n", job->bucket_id);
    }

    gettimeofday(&starttime, 0);

    char *url;
    char *tail = JOB_BUCKET.buf + JOB_BUCKET.n;
    size_t n;
    for (url = JOB_BUCKET.buf; url < tail; url += len + 1) {
        len = strlen(url);
        if (len == 0) continue;

        if (hashmap_get(JOB_BUCKET.map, url, (void *)&n) == MAP_MISSING) {
            n = 0;
        }
        n++;
        hashmap_put(JOB_BUCKET.map, url, (void *)n);
    }
    gettimeofday(&endtime, 0);

    if (job->context->verbosity) {
        printf("bucket %d is reduced\n", job->bucket_id);
        printf("#%d time: %lu ms\n", job->bucket_id, (1000000 * (endtime.tv_sec - starttime.tv_sec) + endtime.tv_usec - starttime.tv_usec) / 1000);
    }

    hashmap_iterate(JOB_BUCKET.map, traversing_map, job);
}

int reduce(Context *context) {
    int ret;
    struct stat st;
    size_t n;

    char path[PATH_MAX];

    Job *job;

    for (int i = 0; i < context->bucket_size; ++i) {
        // If bucket file is closed or not opened:
        if (BUCKET_FP(i) == NULL) {
            sprintf(path, "%s/%d", context->tmp_path, i);

            BUCKET_FP(i) = fopen(path, "w+e");
            if (BUCKET_FP(i) == NULL) {
                return errno;
            }
        }

        // Get size of bucket file
        ret = fstat(BUCKET_FD(i), &st);
        if (ret == -1) {
            switch (errno) {
                case EACCES:
                    perrorf("No access to stat partition file: id = %d\n", i);
                    break;

                default:
                    perrorf("Failed to stat partition file: id = %d, errno = %d\n", i,  errno);
            }
            return errno;
        }

        BUCKET_N(i) = st.st_size;

        // If bucket is empty, ignore it~
        if (BUCKET_N(i) != 0) {
            // mmap file to memory
            BUCKET_BUF(i) = mmap(
                    NULL, st.st_size, PROT_READ, MAP_SHARED, BUCKET_FD(i), 0);
            if (BUCKET_BUF(i) == MAP_FAILED) {
                BUCKET_BUF(i) = NULL;
                perrorf("mmap partition file failed: id = %d, fd = %d, size = %lu\n",
                        i, BUCKET_FD(i), context->bucket_threshold);
                return errno;
            }

            BUCKET_MAP(i) = hashmap_new();

            heap_create(&BUCKET_HEAP(i), context->top_n, compare_size_values);
        }

    }

    // scan all URLs in different heaps
    if (context->thread_count == 1) {
        // Single Thread
        job = malloc(sizeof(Job));
        if (job == NULL) {
            perrorf("malloc failed: %d\n", errno);
            return errno;
        }

        job->context = context;

        for (int i = 0; i < context->bucket_size; ++i) {
            if (context->buckets[i].n <= 0) {
                if (context->verbosity) {
                    printf("bucket %d is empty, ignored\n", i);
                }
                continue;
            }

            job->bucket_id = i;
            traversing(job);
        }

        free(job);

    } else {
        for (int i = 0; i < context->thread_count; ++i) {
            // TODO: multi-thread version
        }
        // TODO: sync
    }

    return 0;
}

typedef struct {
    heap *hp;
    int top_n;
} HeapContext;

void func_merge(void* key, void* value, void* hc_) {
    char *old_url;
    size_t old_n;
    HeapContext *hc = hc_;

    if (heap_size(hc->hp) >= hc->top_n) {
        if (heap_min(hc->hp, (void **) &old_url, (void **) &old_n) == 1) {
            // Ignore lessen URL
            if (old_n > (size_t) value) {
                return;
            }

            heap_delmin(hc->hp, (void **) &old_url, (void **) &old_n);
        }
    }

    heap_insert(hc->hp, key, (void *) value);
}

int merge(Context *context) {
    // Find the first heap as base
    heap *hp = NULL;
    HeapContext hc;
    int i;
    size_t size;
    for (i = 0; i < context->bucket_size; ++i) {
        size = heap_size(&context->buckets[i].hp);
        if (size > 0) {
            hp = &context->buckets[i].hp;
            break;
        }
    }

    if (hp == NULL) {
        perrorf("No available heaps in buckets!");
        return ENODATA;
    }

    hc.top_n = context->top_n;
    hc.hp = hp;

    for (; i < context->bucket_size; ++i) {
        size = heap_size(&context->buckets[i].hp);
        if (size <= 0) {
            continue;
        }

        heap_foreach(&context->buckets[i].hp, func_merge, &hc);
    }

    char *url;
    size_t n;
    size = heap_size(hp);
    printf("size: %lu\n", size);
    for (i = size; i > 0; i--) {
        heap_delmin(hp, (void **) &url, (void **) &n);
        printf("%lu: %s\n", n, url);
    }

    return 0;
}
