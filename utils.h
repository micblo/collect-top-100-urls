//
// Created by Peiran Luo on 2019/8/24.
//

#ifndef TOP100_URL_UTILS_H
#define TOP100_URL_UTILS_H

#include "utils/hashmap.h"
#include "utils/heap.h"

int initTempDir(const char *tmp_path);

unsigned int BKDRHash(const char *buf, unsigned int n);

#endif //TOP100_URL_UTILS_H
