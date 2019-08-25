//
// Created by Peiran Luo on 2019/8/24.
//

#include "../common.h"
#include <fcntl.h>
#include <sys/stat.h>

/**
 * Initial temporary directory
 * @param tmp_path Path of a temporary directory
 * @return code. 0: success, others: errno
 */
int initTempDir(const char *tmp_path) {
    int ret;
    struct stat _stat;

    ret = fstatat(AT_FDCWD, tmp_path, &_stat, 0);
    if (ret) {
        switch (errno) {
            case ENOENT:
                ret = mkdir(tmp_path, 0777);
                if (ret) {
                    perrorf("Failed to create temp dir: %s\n", tmp_path);
                    return errno;
                }
                return 0;

            case EACCES:
                perrorf("No access to access temp dir: %s\n", tmp_path);
                break;

            default:
                perrorf("Failed to access temp dir: %s, errno = %d\n", tmp_path, errno);
        }
        return errno;
    }

    if (!S_ISDIR(_stat.st_mode)) {
        perrorf("Temp dir is not a directory: %s\n", tmp_path);
        return ENOTDIR;
    }

    return 0;
}

unsigned int BKDRHash(const char *buf, unsigned int n) {
    const int seed = 131; // 31 131 1313 13131 131313 etc..
    unsigned int hash = 0;
    for (unsigned int j = 0; j < n; ++j) {
        hash = hash * seed + buf[j];
    }
    return hash;
}
