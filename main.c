#include "common.h"
#include "utils.h"
#include "partition.h"
#include "reduce.h"
#include <getopt.h>
#include <sys/mman.h>
#include <sys/time.h>

// partition time: 1571243 ms

static const char *optString = "d:n:t:j:vh?";

static const struct option longOpts[] = {
        { "dataset", required_argument, NULL, 'd' },
        { "bucket-size", required_argument, NULL, 0 },
        { "jobs", optional_argument, NULL, 'j' },
        { "top-n", required_argument, NULL, 'n' },
        { "tmp-path", required_argument, NULL, 't' },
        { "verbose", no_argument, NULL, 'v' },
        { "ignore-partition", no_argument, NULL, 0 },
        { "ignore-clean", no_argument, NULL, 0 },
        { "help", no_argument, NULL, 'h' },
        { NULL, no_argument, NULL, 0 }
};

void display_usage() {
    printf(
            "top100_url -d dataset_path [-tvh] [--bucket_size=30]\n"
            "-d DATASET_PATH, --dataset DATASET_PATH \t Dataset path\n"
            "-n N, --top-n=N \t Select TOP N URLs, default is 100."
            "-t TEMP_DIR_PATH \t Temporary path, for partition\n"
            "-j [N], --jobs[=N] \t Allow N jobs at once; infinite jobs with no arg."
            "--bucket-size \t Size of partition buckets\n"
            "--ignore-partition \t Ignore partition and use partition files in temporary directory\n"
            "--ignore-clean \t Ignore clean partition files in temporary directory\n"
            "-v \t Verbose, print more detail messages\n"
            "-h \t Show help\n"
            );
}

int main(int argc, char *argv[]) {
    Context _context;
    Context *context = &_context;
    int ret;
    int longIndex;

    struct timeval starttime, endtime;

    const char default_tmp_path[] = "./temp";

    memset(context, 0, sizeof(Context));
    context->tmp_path = default_tmp_path;
    context->bucket_size = 30;

    int opt = getopt_long( argc, argv, optString, longOpts, &longIndex );
    while( opt != -1 ) {
        switch( opt ) {
            case 'd':
                context->dataset_path = optarg;
                break;

            case 'n':
                context->top_n = atoi(optarg);
                break;

            case 't':
                context->tmp_path = optarg;
                break;

            case 'j':
                context->thread_count = atoi(optarg);
                break;

            case 'v':
                context->verbosity++;
                break;

            case 'h':   /* fall-through is intentional */
            case '?':
                display_usage();
                return 0;

            case 0:     /* long option without a short arg */
                if( strcmp( "bucket-size", longOpts[longIndex].name ) == 0 ) {
                    context->bucket_size = atoi(optarg);
                } else if( strcmp( "ignore-partition", longOpts[longIndex].name ) == 0 ) {
                    context->ignore_partition = 1;
                } else if( strcmp( "ignore-clean", longOpts[longIndex].name ) == 0 ) {
                    context->ignore_clean = 1;
                }
                break;

            default:
                /* You won't actually get here. */
                break;
        }

        opt = getopt_long( argc, argv, optString, longOpts, &longIndex );
    }

    if (context->dataset_path == NULL) {
        perrorf("dataset is required!\n");
        return 0;
    }
    if (context->bucket_size <= 0) {
        perrorf("bucket_size must be bigger than 0!\n");
        return 0;
    }
    if (context->thread_count <= 0) {
        context->thread_count = 1;
    }
    if (context->top_n <= 0) {
        context->top_n = 100;
    }

    if (context->verbosity) {
        printf("=============================\n");
        printf("Params:\n");
        printf("dataset_path = %s\n", context->dataset_path);
        printf("tmp_path = %s\n", context->tmp_path);
        printf("bucket_size = %d\n", context->bucket_size);
        printf("thread_count(jobs) = %d\n", context->thread_count);
        printf("ignore_partition = %s\n", context->ignore_partition ? "true" : "false");
        printf("ignore_clean = %s\n", context->ignore_clean ? "true" : "false");
        printf("verbose = %s\n", context->verbosity ? "true" : "false");
        printf("=============================\n");
    }

    ret = initTempDir(context->tmp_path);
    if (ret) {
        goto finish;
    }

    // Init buckets
    context->buckets = malloc(context->bucket_size * sizeof(Bucket));
    memset(context->buckets, 0, context->bucket_size * sizeof(Bucket));

    // Step 1: partition URLs to buckets
    if (!context->ignore_partition) {
        gettimeofday(&starttime, 0);
        ret = partition(context);
        gettimeofday(&endtime, 0);

        printf("partition time: %lu ms\n", (1000000 * (endtime.tv_sec - starttime.tv_sec) + endtime.tv_usec - starttime.tv_usec) / 1000);
        if (ret) {
            goto finish;
        }
    }

    // Step 2: calculate count of URLs in different buckets and make min heap
    gettimeofday(&starttime, 0);
    ret = reduce(context);
    gettimeofday(&endtime, 0);

    printf("reduce time: %lu ms\n", (1000000 * (endtime.tv_sec - starttime.tv_sec) + endtime.tv_usec - starttime.tv_usec) / 1000);

    if (ret) {
        goto finish;
    }

    // Step 3: Merge min heaps
    gettimeofday(&starttime, 0);
    ret = merge(context);
    gettimeofday(&endtime, 0);

    printf("merge time: %lu ms\n", (1000000 * (endtime.tv_sec - starttime.tv_sec) + endtime.tv_usec - starttime.tv_usec) / 1000);
    if (ret) {
        goto finish;
    }

    finish:
    if (context->buckets != NULL) {
        for (int i = 0; i < context->bucket_size; ++i) {
            if (BUCKET_BUF(i) != NULL) {
                munmap(BUCKET_BUF(i), BUCKET_MMAP_SIZE(i));
                BUCKET_BUF(i) = NULL;
            }

            if (BUCKET_FP(i) != NULL) {
                fclose(BUCKET_FP(i));
                BUCKET_FP(i) = NULL;
            }

            if (BUCKET_MAP(i) != NULL) {
                hashmap_free(BUCKET_MAP(i));
                BUCKET_MAP(i) = NULL;
            }

            heap_destroy(&BUCKET_HEAP(i));
        }
        free(context->buckets);
    }

    return ret;
}