//
// Created by Peiran Luo on 2019/8/24.
//

#include "partition.h"
#include "utils.h"

#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
#include <limits.h>
#include <mach/machine.h>

int initBuckets(Context *context, size_t file_size) {
    char path[PATH_MAX];
    int ret;

    context->bucket_threshold = ((file_size / context->bucket_size) / PAGE_SIZE + 1) * PAGE_SIZE;

    for (int i = 0; i < context->bucket_size; ++i) {
        sprintf(path, "%s/%d", context->tmp_path, i);

        if (context->verbosity) {
            printf("bucket %d: %s\n", i, path);
        }

        BUCKET_FP(i) = fopen(path, "w+e");
        if (BUCKET_FP(i) == NULL) {
            return errno;
        }

        // Pre-create a file whose size is bucket_threshold, for effective I/O,
        // directly write URLs when n + len(URL) + 1 is lower than mmap_size.
        ret = ftruncate(BUCKET_FD(i), context->bucket_threshold);
        if (ret == -1) {
            perrorf("ftruncate failed: %s; size = %lu\n", path, context->bucket_threshold);
            return errno;
        }

        BUCKET_MMAP_SIZE(i) = context->bucket_threshold;

        BUCKET_BUF(i) = mmap(NULL, BUCKET_MMAP_SIZE(i), PROT_READ | PROT_WRITE, MAP_SHARED, BUCKET_FD(i), 0);
        if (BUCKET_BUF(i) == MAP_FAILED) {
            BUCKET_BUF(i) = NULL;
            perrorf("mmap failed: %s; fd = %d, size = %lu\n",
                    path, BUCKET_FD(i), BUCKET_MMAP_SIZE(i));
            return errno;
        }
    }
    return 0;
}

int partition(Context *context) {
    int ret;
    int fd;
    size_t n;
    struct stat st;
    unsigned int hash;
    char blank = '\0';
    FILE *fp;

    char *buf;

    size_t sp = 0;
    char tmp[4096];

    // Open dataset file
    fd = open(context->dataset_path, O_RDONLY, S_IRUSR);
    if (fd == -1) {
        switch (errno) {
            case ENOENT:
                perrorf("dataset file is not found: %s\n", context->dataset_path);
                break;

            case EACCES:
                perrorf("No access to access dataset file: %s\n", context->dataset_path);
                break;

            default:
                perrorf("Failed to open dataset file: %s, errno = %d\n", context->tmp_path, errno);
        }
        return errno;
    }

    // Get stat of dataset file
    ret = fstat(fd, &st);
    if (ret == -1) {
        switch (errno) {
            case EACCES:
                perrorf("No access to stat dataset file: %s\n", context->dataset_path);
                break;

            default:
                perrorf("Failed to stat dataset file: %s, errno = %d\n", context->tmp_path, errno);
        }
        return errno;
    }

    printf("Size of dataset file: %llu\n", st.st_size);

    // mmap
    buf = mmap(NULL, st.st_size, PROT_READ, MAP_SHARED, fd, 0);
    if (buf == MAP_FAILED) {
        perrorf("Failed to mmap dataset file: %s, errno = %d\n", context->tmp_path, errno);
        goto finish;
    }

    // init buckets for partition
    ret = initBuckets(context, st.st_size);
    if (ret) {
        perrorf("Failed to init buckets: errno = %d\n", errno);
        goto finish;
    }

    // traversing
    for (size_t i = 0; i < st.st_size; ++i) {
        if (buf[i] == '\n') {
            n = i - sp;
            if (context->verbosity) {
                strncpy(tmp, buf + sp, n);
                tmp[n] = 0;
                printf("%s\n", tmp);
            }

            hash = BKDRHash(buf + sp, n) % context->bucket_size;

            if (BUCKET_N(hash) + n + 1 <= context->bucket_threshold) {
                memcpy(BUCKET_BUF(hash) + BUCKET_N(hash), buf + sp, n);
            } else {
                fp = BUCKET_FP(hash);

                if (!BUCKET_IS_BROKEN(hash)) {
                    fseek(fp, BUCKET_N(hash), SEEK_SET);
                    BUCKET_IS_BROKEN(hash) = 1;
                }

                fwrite(buf + sp, 1, n, fp);
                fwrite(&blank, 1, 1, fp);
            }
            BUCKET_N(hash) += n + 1;

            sp = i + 1;
        }
    }

    finish:
    if (buf > 0) {
        ret = munmap(buf, st.st_size);
        if (ret == -1) {
            perrorf("Failed to munmap dataset file: %s, errno = %d\n", context->tmp_path, errno);
            return errno;
        }
    }

    for (int i = 0; i < context->bucket_size; ++i) {
        if (BUCKET_BUF(i) != NULL) {
            ret = munmap(BUCKET_BUF(i), context->bucket_threshold);
            if (ret) {
                perrorf("Failed to munmap partition file: id = %d, errno = %d\n", i, errno);
                return errno;
            }

            BUCKET_BUF(i) = NULL;
        }

        if (BUCKET_N(i) < BUCKET_MMAP_SIZE(i)) {
            ftruncate(BUCKET_FD(i), BUCKET_N(i));
            // CAN IGNORE ERROR OF ftruncate
        } else {
            fflush(BUCKET_FP(i));
        }

        // DO NOT CLOSE fd! BECAUSE fd WILL BE USED WHEN REDUCING!
    }

    if (fd > 0) {
        ret = close(fd);
        if (ret) {
            perrorf("Failed to close dataset file: %s, errno = %d\n", context->tmp_path, errno);
            return errno;
        }
    }

    return 0;
}
