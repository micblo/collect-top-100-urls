//
// Created by Peiran Luo on 2019/8/24.
//

#ifndef TOP100_URL_MINHEAP_H
#define TOP100_URL_MINHEAP_H

#include <stddef.h>

typedef struct {
    const char *url;
    size_t n;
} URLPair;

typedef struct {
    int max;
    size_t n;
    URLPair **heap;
} MinHeap;

MinHeap *minheap_new(int max);
void minheap_free(MinHeap *h);
void minheap_insert(MinHeap *context, const char *url, size_t n);
void minheap_insert2(MinHeap *context, URLPair *pair);
URLPair *minheap_deleteMin(MinHeap *context);
URLPair *minheap_returnMin(MinHeap *context);
void minheap_print(MinHeap *context);

#endif
