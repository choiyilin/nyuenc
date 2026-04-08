#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define CHUNK_SIZE 65536UL

typedef struct {
    unsigned char ch;
    unsigned char count;
} Run;

typedef struct {
    const unsigned char *data;
    size_t len;
    Run *runs;
    size_t run_count;
    bool done;
} Task;

typedef struct {
    int fd;
    size_t size;
    unsigned char *map;
} MappedFile;

typedef struct {
    Task *tasks;
    size_t num_tasks;
    size_t next_task;
    size_t completed;
    bool stop;
    pthread_mutex_t mutex;
    pthread_cond_t has_work;
    pthread_cond_t has_completion;
} ThreadPool;

static void die(const char *msg) {
    perror(msg);
    exit(EXIT_FAILURE);
}

static void write_all(int fd, const void *buf, size_t count) {
    const unsigned char *p = (const unsigned char *)buf;
    size_t left = count;
    while (left > 0) {
        ssize_t n = write(fd, p, left);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            die("write");
        }
        p += (size_t)n;
        left -= (size_t)n;
    }
}

static void emit_run(unsigned char ch, size_t count) {
    while (count > 255U) {
        unsigned char out[2];
        out[0] = ch;
        out[1] = 255U;
        write_all(STDOUT_FILENO, out, sizeof(out));
        count -= 255U;
    }
    if (count > 0U) {
        unsigned char out[2];
        out[0] = ch;
        out[1] = (unsigned char)count;
        write_all(STDOUT_FILENO, out, sizeof(out));
    }
}

static void process_task(Task *task) {
    if (task->len == 0) {
        task->runs = NULL;
        task->run_count = 0;
        return;
    }

    Run *runs = (Run *)malloc(task->len * sizeof(Run));
    if (runs == NULL) {
        die("malloc");
    }

    size_t out_idx = 0;
    unsigned char current = task->data[0];
    unsigned int count = 1;

    for (size_t i = 1; i < task->len; ++i) {
        const unsigned char ch = task->data[i];
        if (ch == current && count < 255U) {
            count++;
            continue;
        }

        runs[out_idx].ch = current;
        runs[out_idx].count = (unsigned char)count;
        out_idx++;

        current = ch;
        count = 1;
    }

    runs[out_idx].ch = current;
    runs[out_idx].count = (unsigned char)count;
    out_idx++;

    task->runs = runs;
    task->run_count = out_idx;
}

static void *worker_main(void *arg) {
    ThreadPool *pool = (ThreadPool *)arg;

    for (;;) {
        size_t idx = 0;

        pthread_mutex_lock(&pool->mutex);
        while (pool->next_task >= pool->num_tasks && !pool->stop) {
            pthread_cond_wait(&pool->has_work, &pool->mutex);
        }

        if (pool->stop) {
            pthread_mutex_unlock(&pool->mutex);
            break;
        }

        idx = pool->next_task;
        pool->next_task++;
        pthread_mutex_unlock(&pool->mutex);

        process_task(&pool->tasks[idx]);

        pthread_mutex_lock(&pool->mutex);
        pool->tasks[idx].done = true;
        pool->completed++;
        pthread_cond_broadcast(&pool->has_completion);
        pthread_mutex_unlock(&pool->mutex);
    }

    return NULL;
}

static void encode_sequential(char **files, int file_count) {
    bool has_pending = false;
    unsigned char pending_char = 0;
    size_t pending_count = 0;
    unsigned char buf[CHUNK_SIZE];

    for (int i = 0; i < file_count; ++i) {
        FILE *fp = fopen(files[i], "rb");
        if (fp == NULL) {
            die("fopen");
        }

        for (;;) {
            size_t n = fread(buf, 1, sizeof(buf), fp);
            if (n == 0) {
                if (ferror(fp) != 0) {
                    fclose(fp);
                    die("fread");
                }
                break;
            }

            for (size_t j = 0; j < n; ++j) {
                unsigned char ch = buf[j];
                if (!has_pending) {
                    pending_char = ch;
                    pending_count = 1;
                    has_pending = true;
                    continue;
                }

                if (ch == pending_char) {
                    pending_count++;
                    continue;
                }

                emit_run(pending_char, pending_count);
                pending_char = ch;
                pending_count = 1;
            }
        }

        if (fclose(fp) != 0) {
            die("fclose");
        }
    }

    if (has_pending) {
        emit_run(pending_char, pending_count);
    }
}

static MappedFile map_file(const char *path) {
    MappedFile mf;
    mf.fd = -1;
    mf.size = 0;
    mf.map = NULL;

    mf.fd = open(path, O_RDONLY);
    if (mf.fd < 0) {
        die("open");
    }

    struct stat st;
    if (fstat(mf.fd, &st) != 0) {
        close(mf.fd);
        die("fstat");
    }
    if (st.st_size < 0) {
        close(mf.fd);
        errno = EINVAL;
        die("fstat");
    }

    mf.size = (size_t)st.st_size;
    if (mf.size == 0) {
        return mf;
    }

    mf.map = mmap(NULL, mf.size, PROT_READ, MAP_PRIVATE, mf.fd, 0);
    if (mf.map == MAP_FAILED) {
        close(mf.fd);
        die("mmap");
    }

    return mf;
}

static void unmap_file(MappedFile *mf) {
    if (mf->map != NULL) {
        if (munmap(mf->map, mf->size) != 0) {
            die("munmap");
        }
        mf->map = NULL;
    }
    if (mf->fd >= 0) {
        if (close(mf->fd) != 0) {
            die("close");
        }
        mf->fd = -1;
    }
}

static size_t count_tasks_from_files(const MappedFile *files, int file_count) {
    size_t total = 0;
    for (int i = 0; i < file_count; ++i) {
        if (files[i].size == 0) {
            continue;
        }
        total += (files[i].size + CHUNK_SIZE - 1U) / CHUNK_SIZE;
    }
    return total;
}

static void encode_parallel(char **files, int file_count, int num_threads) {
    MappedFile *mapped = (MappedFile *)calloc((size_t)file_count, sizeof(MappedFile));
    if (mapped == NULL) {
        die("calloc");
    }

    for (int i = 0; i < file_count; ++i) {
        mapped[i] = map_file(files[i]);
    }

    const size_t num_tasks = count_tasks_from_files(mapped, file_count);
    if (num_tasks == 0) {
        for (int i = 0; i < file_count; ++i) {
            unmap_file(&mapped[i]);
        }
        free(mapped);
        return;
    }

    Task *tasks = (Task *)calloc(num_tasks, sizeof(Task));
    if (tasks == NULL) {
        die("calloc");
    }

    size_t idx = 0;
    for (int i = 0; i < file_count; ++i) {
        const unsigned char *base = mapped[i].map;
        size_t remaining = mapped[i].size;
        size_t offset = 0;
        while (remaining > 0) {
            size_t len = remaining > CHUNK_SIZE ? CHUNK_SIZE : remaining;
            tasks[idx].data = base + offset;
            tasks[idx].len = len;
            tasks[idx].runs = NULL;
            tasks[idx].run_count = 0;
            tasks[idx].done = false;
            idx++;
            offset += len;
            remaining -= len;
        }
    }

    ThreadPool pool;
    pool.tasks = tasks;
    pool.num_tasks = num_tasks;
    pool.next_task = 0;
    pool.completed = 0;
    pool.stop = false;

    if (pthread_mutex_init(&pool.mutex, NULL) != 0) {
        die("pthread_mutex_init");
    }
    if (pthread_cond_init(&pool.has_work, NULL) != 0) {
        die("pthread_cond_init");
    }
    if (pthread_cond_init(&pool.has_completion, NULL) != 0) {
        die("pthread_cond_init");
    }

    pthread_t *threads = (pthread_t *)calloc((size_t)num_threads, sizeof(pthread_t));
    if (threads == NULL) {
        die("calloc");
    }

    for (int i = 0; i < num_threads; ++i) {
        if (pthread_create(&threads[i], NULL, worker_main, &pool) != 0) {
            die("pthread_create");
        }
    }

    pthread_mutex_lock(&pool.mutex);
    pthread_cond_broadcast(&pool.has_work);
    pthread_mutex_unlock(&pool.mutex);

    bool has_pending = false;
    unsigned char pending_char = 0;
    size_t pending_count = 0;

    for (size_t t = 0; t < num_tasks; ++t) {
        pthread_mutex_lock(&pool.mutex);
        while (!tasks[t].done) {
            pthread_cond_wait(&pool.has_completion, &pool.mutex);
        }
        pthread_mutex_unlock(&pool.mutex);

        for (size_t r = 0; r < tasks[t].run_count; ++r) {
            unsigned char ch = tasks[t].runs[r].ch;
            size_t count = tasks[t].runs[r].count;
            if (!has_pending) {
                has_pending = true;
                pending_char = ch;
                pending_count = count;
                continue;
            }
            if (ch == pending_char) {
                pending_count += count;
                continue;
            }
            emit_run(pending_char, pending_count);
            pending_char = ch;
            pending_count = count;
        }

        free(tasks[t].runs);
        tasks[t].runs = NULL;
    }

    if (has_pending) {
        emit_run(pending_char, pending_count);
    }

    pthread_mutex_lock(&pool.mutex);
    pool.stop = true;
    pthread_cond_broadcast(&pool.has_work);
    pthread_mutex_unlock(&pool.mutex);

    for (int i = 0; i < num_threads; ++i) {
        if (pthread_join(threads[i], NULL) != 0) {
            die("pthread_join");
        }
    }

    if (pthread_cond_destroy(&pool.has_completion) != 0) {
        die("pthread_cond_destroy");
    }
    if (pthread_cond_destroy(&pool.has_work) != 0) {
        die("pthread_cond_destroy");
    }
    if (pthread_mutex_destroy(&pool.mutex) != 0) {
        die("pthread_mutex_destroy");
    }

    free(threads);
    free(tasks);
    for (int i = 0; i < file_count; ++i) {
        unmap_file(&mapped[i]);
    }
    free(mapped);
}

static int parse_threads(int argc, char **argv, int *threads, int *first_file_idx) {
    *threads = 1;
    *first_file_idx = 1;

    if (argc >= 3 && strcmp(argv[1], "-j") == 0) {
        char *end = NULL;
        long parsed = strtol(argv[2], &end, 10);
        if (end == argv[2] || *end != '\0' || parsed <= 0 || parsed > 1024) {
            fprintf(stderr, "Invalid thread count: %s\n", argv[2]);
            return -1;
        }
        *threads = (int)parsed;
        *first_file_idx = 3;
    }

    if (*first_file_idx >= argc) {
        fprintf(stderr, "Usage: %s [-j threads] file1 [file2 ...]\n", argv[0]);
        return -1;
    }

    return 0;
}

int main(int argc, char **argv) {
    int threads = 1;
    int first_file_idx = 1;
    if (parse_threads(argc, argv, &threads, &first_file_idx) != 0) {
        return EXIT_FAILURE;
    }

    char **files = &argv[first_file_idx];
    int file_count = argc - first_file_idx;

    if (threads == 1) {
        encode_sequential(files, file_count);
    } else {
        encode_parallel(files, file_count, threads);
    }

    return EXIT_SUCCESS;
}
