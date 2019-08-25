//
// Created by Peiran Luo on 2019/8/24.
//

#ifndef TOP100_URL_COMMON_H
#define TOP100_URL_COMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <sys/errno.h>
#include "utils.h"

#define perrorf(...) (fprintf(stderr, __VA_ARGS__))

typedef struct {
    FILE *fp;
    char *buf;
    size_t n;
    size_t mmap_size;
    char is_broken;
    map_t map;
    heap hp;
} Bucket;

#define BUCKET_FP(i) (context->buckets[i].fp)
#define BUCKET_BUF(i) (context->buckets[i].buf)
#define BUCKET_FD(i) (context->buckets[i].fp->_file)
#define BUCKET_N(i) (context->buckets[i].n)
#define BUCKET_MAP(i) (context->buckets[i].map)
#define BUCKET_HEAP(i) (context->buckets[i].hp)
#define BUCKET_IS_BROKEN(i) (context->buckets[i].is_broken)
#define BUCKET_MMAP_SIZE(i) (context->buckets[i].mmap_size)

typedef struct {
    const char *dataset_path;
    const char *tmp_path;
    int bucket_size;
    int thread_count;
    int top_n;

    char verbosity;
    char ignore_partition;
    char ignore_clean;

    Bucket *buckets;
    size_t bucket_threshold;
} Context;

#endif //TOP100_URL_COMMON_H
