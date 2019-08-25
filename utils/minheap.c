//
// Created by Peiran Luo on 2019/8/24.
//

#include "minheap.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"

MinHeap *minheap_new(int max) {
    if (max <= 0) {
        return NULL;
    }

    MinHeap *h = malloc(sizeof(MinHeap));
    if (h == NULL) {
        return h;
    }

    h->n = 0;
    h->max = max;
    h->heap = malloc((max + 1) * sizeof(URLPair *));
    if (h->heap == NULL) {
        free(h);
        return NULL;
    }
    memset(h->heap, 0, (max + 1) * sizeof(URLPair *));

    return h;
}

void minheap_free(MinHeap *h) {
    if (h != NULL) {
        if (h->heap != NULL) {
            for(size_t i = 1; i <= h->n; i++){
                free(h->heap[i]);
            }

            free(h->heap);
        }
        free(h);
    }
}

void heapifyUp(MinHeap *context, URLPair *v) { // last element
    int i = context->n;

    for (; context->heap[i/2] != NULL && context->heap[i/2]->n > v->n; i /= 2){
        context->heap[i] = context->heap[i/2];
    }

    context->heap[i] = v;
}

void heapifyDown(MinHeap *context) { // top element
    int i = 1;

    while(i < context->n){
        int c1 = 2*i;
        int c2 = 2*i + 1;
        int t;
        if (c1 <= context->n) {
            t = c1;
        } else {
            break;
        }
        if (c2 <= context->n && context->heap[c1]->n > context->heap[c2]->n){
            t = c2;
        }

        if(context->heap[i]->n >= context->heap[t]->n) break;

        URLPair *temp = context->heap[i];
        context->heap[i] = context->heap[t];
        context->heap[t] = temp;
        i = t;
    }
}

void minheap_insert(MinHeap *context, const char *url, size_t n) {
    if (context->n >= context->max) {
        if (n <= minheap_returnMin(context)->n) {
            return; // IGNORE TOO SHORT N, MAKE SURE THAT HEAP CONTAINS TOP N URLS
        }

        URLPair *p = minheap_deleteMin(context);
        free(p);
    }

    URLPair *pair = malloc(sizeof(URLPair));
    pair->url = url;
    pair->n = n;

    heapifyUp(context, pair);
    context->n++;
}


void minheap_insert2(MinHeap *context, URLPair *pair) {
    if (context->n >= context->max) {
        if (pair->n <= minheap_returnMin(context)->n) {
            free(pair);
            return; // IGNORE TOO SHORT N, MAKE SURE THAT HEAP CONTAINS TOP N URLS
        }

        URLPair *p = minheap_deleteMin(context);
        free(p);
    }

    heapifyUp(context, pair);
    context->n++;
}

URLPair *minheap_returnMin(MinHeap *context) {
    return context->heap[1];
}

URLPair *minheap_deleteMin(MinHeap *context) {
    URLPair *t = context->heap[1];
    context->heap[1] = context->heap[context->n];
    context->n--;
    heapifyDown(context);
    return t;
}

void minheap_print(MinHeap *context) {
    if (context != NULL) return;

    int i = 1;
    printf("MinHeap:\n");
    while(i <= context->n) {
        printf("%lu: %s\n", context->heap[i]->n, context->heap[i]->url);
        i++;
    }
    printf("\n");
}

