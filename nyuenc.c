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

#define CHUNK 4096UL
#define OUTSZ (1U << 20)

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
} FileMap;

typedef struct {
    Task *tasks;
    size_t *q;
    size_t head;
    size_t tail;
    bool shutdown;
    pthread_mutex_t lock;
    pthread_cond_t cv_work;
    pthread_cond_t cv_done;
} Pool;

typedef struct {
    int active;
    unsigned char c;
    size_t n;
} Pending;

static struct {
    unsigned char buf[OUTSZ];
    size_t used;
} out;

static void oom(const char *where) {
    perror(where);
    exit(EXIT_FAILURE);
}

static void write_full(int fd, const void *v, size_t n) {
    const unsigned char *p = v;
    while (n > 0) {
        ssize_t w = write(fd, p, n);
        if (w < 0) {
            if (errno == EINTR) {
                continue;
            }
            oom("write");
        }
        p += (size_t)w;
        n -= (size_t)w;
    }
}

static void out_flush(void) {
    if (out.used == 0) {
        return;
    }
    write_full(STDOUT_FILENO, out.buf, out.used);
    out.used = 0;
}

static void out_put(unsigned char a, unsigned char b) {
    if (out.used + 2 > OUTSZ) {
        out_flush();
    }
    out.buf[out.used++] = a;
    out.buf[out.used++] = b;
}

static void emit_run(unsigned char ch, size_t count) {
    while (count > 255) {
        out_put(ch, 255);
        count -= 255;
    }
    if (count > 0) {
        out_put(ch, (unsigned char)count);
    }
}

static inline void stitch(Pending *p, unsigned char ch, size_t count) {
    if (!p->active) {
        p->c = ch;
        p->n = count;
        p->active = 1;
        return;
    }
    if (ch == p->c) {
        p->n += count;
        return;
    }
    emit_run(p->c, p->n);
    p->c = ch;
    p->n = count;
}

static void stitch_flush(Pending *p) {
    if (p->active) {
        emit_run(p->c, p->n);
    }
}

static void encode_chunk(Task *t) {
    if (t->len == 0) {
        t->runs = NULL;
        t->run_count = 0;
        return;
    }

    Run local[CHUNK];
    size_t nr = 0;
    unsigned char cur = t->data[0];
    unsigned run = 1;

    for (size_t i = 1; i < t->len; i++) {
        unsigned char b = t->data[i];
        if (b == cur && run < 255) {
            run++;
            continue;
        }
        local[nr].ch = cur;
        local[nr].count = (unsigned char)run;
        nr++;
        cur = b;
        run = 1;
    }
    local[nr].ch = cur;
    local[nr].count = (unsigned char)run;
    nr++;

    t->runs = malloc(nr * sizeof(Run));
    if (t->runs == NULL) {
        oom("malloc");
    }
    memcpy(t->runs, local, nr * sizeof(Run));
    t->run_count = nr;
}

static void *worker(void *vp) {
    Pool *pool = vp;

    for (;;) {
        pthread_mutex_lock(&pool->lock);
        while (pool->head == pool->tail && !pool->shutdown) {
            pthread_cond_wait(&pool->cv_work, &pool->lock);
        }
        if (pool->shutdown && pool->head == pool->tail) {
            pthread_mutex_unlock(&pool->lock);
            break;
        }

        size_t k = pool->q[pool->head++];
        pthread_mutex_unlock(&pool->lock);

        encode_chunk(&pool->tasks[k]);

        pthread_mutex_lock(&pool->lock);
        pool->tasks[k].done = true;
        pthread_cond_signal(&pool->cv_done);
        pthread_mutex_unlock(&pool->lock);
    }
    return NULL;
}

static FileMap open_mmap(const char *path) {
    FileMap m;
    m.fd = -1;
    m.size = 0;
    m.map = NULL;

    m.fd = open(path, O_RDONLY);
    if (m.fd < 0) {
        oom("open");
    }

    struct stat st;
    if (fstat(m.fd, &st) != 0) {
        close(m.fd);
        oom("fstat");
    }
    if (st.st_size < 0) {
        close(m.fd);
        errno = EINVAL;
        oom("fstat");
    }

    m.size = (size_t)st.st_size;
    if (m.size == 0) {
        return m;
    }

    m.map = mmap(NULL, m.size, PROT_READ, MAP_PRIVATE, m.fd, 0);
    if (m.map == MAP_FAILED) {
        close(m.fd);
        oom("mmap");
    }
    (void)madvise(m.map, m.size, MADV_SEQUENTIAL);

    return m;
}

static void close_mmap(FileMap *m) {
    if (m->map) {
        if (munmap(m->map, m->size) != 0) {
            oom("munmap");
        }
        m->map = NULL;
    }
    if (m->fd >= 0) {
        if (close(m->fd) != 0) {
            oom("close");
        }
        m->fd = -1;
    }
}

static void encode_sequential(char **paths, int nfiles) {
    Pending pend = {0};

    for (int fi = 0; fi < nfiles; fi++) {
        FileMap m = open_mmap(paths[fi]);
        for (size_t i = 0; i < m.size; i++) {
            stitch(&pend, m.map[i], 1);
        }
        close_mmap(&m);
    }

    stitch_flush(&pend);
    out_flush();
}

static size_t chunk_task_count(FileMap *maps, int nfiles) {
    size_t n = 0;
    for (int i = 0; i < nfiles; i++) {
        if (maps[i].size) {
            n += (maps[i].size + CHUNK - 1) / CHUNK;
        }
    }
    return n;
}

static void encode_parallel(char **paths, int nfiles, int nthr) {
    FileMap *maps = calloc((size_t)nfiles, sizeof(FileMap));
    if (!maps) {
        oom("calloc");
    }

    for (int i = 0; i < nfiles; i++) {
        maps[i] = open_mmap(paths[i]);
    }

    size_t ntask = chunk_task_count(maps, nfiles);
    if (ntask == 0) {
        for (int i = 0; i < nfiles; i++) {
            close_mmap(&maps[i]);
        }
        free(maps);
        return;
    }

    Task *tasks = calloc(ntask, sizeof(Task));
    if (!tasks) {
        oom("calloc");
    }

    size_t k = 0;
    for (int fi = 0; fi < nfiles; fi++) {
        const unsigned char *base = maps[fi].map;
        size_t left = maps[fi].size;
        size_t off = 0;
        while (left) {
            size_t take = left < CHUNK ? left : CHUNK;
            tasks[k].data = base + off;
            tasks[k].len = take;
            tasks[k].runs = NULL;
            tasks[k].run_count = 0;
            tasks[k].done = false;
            k++;
            off += take;
            left -= take;
        }
    }

    size_t *order = malloc(ntask * sizeof(size_t));
    if (!order) {
        oom("malloc");
    }

    Pool pool;
    pool.tasks = tasks;
    pool.q = order;
    pool.head = pool.tail = 0;
    pool.shutdown = false;

    if (pthread_mutex_init(&pool.lock, NULL)) {
        oom("pthread_mutex_init");
    }
    if (pthread_cond_init(&pool.cv_work, NULL)) {
        oom("pthread_cond_init");
    }
    if (pthread_cond_init(&pool.cv_done, NULL)) {
        oom("pthread_cond_init");
    }

    pthread_t *tid = calloc((size_t)nthr, sizeof(pthread_t));
    if (!tid) {
        oom("calloc");
    }

    for (int i = 0; i < nthr; i++) {
        if (pthread_create(&tid[i], NULL, worker, &pool)) {
            oom("pthread_create");
        }
    }

    pthread_mutex_lock(&pool.lock);
    for (size_t i = 0; i < ntask; i++) {
        pool.q[pool.tail++] = i;
    }
    pthread_cond_broadcast(&pool.cv_work);
    pthread_mutex_unlock(&pool.lock);

    Pending pend = {0};

    for (size_t t = 0; t < ntask; t++) {
        pthread_mutex_lock(&pool.lock);
        while (!tasks[t].done) {
            pthread_cond_wait(&pool.cv_done, &pool.lock);
        }
        pthread_mutex_unlock(&pool.lock);

        for (size_t r = 0; r < tasks[t].run_count; r++) {
            stitch(&pend, tasks[t].runs[r].ch, tasks[t].runs[r].count);
        }
        free(tasks[t].runs);
        tasks[t].runs = NULL;
    }

    stitch_flush(&pend);
    out_flush();

    pthread_mutex_lock(&pool.lock);
    pool.shutdown = true;
    pthread_cond_broadcast(&pool.cv_work);
    pthread_mutex_unlock(&pool.lock);

    for (int i = 0; i < nthr; i++) {
        if (pthread_join(tid[i], NULL)) {
            oom("pthread_join");
        }
    }

    if (pthread_cond_destroy(&pool.cv_done)) {
        oom("pthread_cond_destroy");
    }
    if (pthread_cond_destroy(&pool.cv_work)) {
        oom("pthread_cond_destroy");
    }
    if (pthread_mutex_destroy(&pool.lock)) {
        oom("pthread_mutex_destroy");
    }

    free(order);
    free(tid);
    free(tasks);
    for (int i = 0; i < nfiles; i++) {
        close_mmap(&maps[i]);
    }
    free(maps);
}

int main(int argc, char **argv) {
    int jobs = 1;
    int c;

    while ((c = getopt(argc, argv, "j:")) != -1) {
        switch (c) {
        case 'j': {
            char *end = NULL;
            long v = strtol(optarg, &end, 10);
            if (end == optarg || *end || v <= 0 || v > 1024) {
                fprintf(stderr, "Invalid thread count: %s\n", optarg);
                return 1;
            }
            jobs = (int)v;
            break;
        }
        default:
            fprintf(stderr, "usage: %s [-j n] file ...\n", argv[0]);
            return 1;
        }
    }

    if (optind >= argc) {
        fprintf(stderr, "usage: %s [-j n] file ...\n", argv[0]);
        return 1;
    }

    char **files = &argv[optind];
    int nf = argc - optind;

    if (jobs == 1) {
        encode_sequential(files, nf);
    } else {
        encode_parallel(files, nf, jobs);
    }

    return 0;
}
