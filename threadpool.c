/* Thread Pool Based Task Processing System - OSSP Team 16 */
#define _GNU_SOURCE
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* ---------- Module 1: data structures and pool creation ---------- */
typedef void (*task_fn)(void *);
typedef struct { task_fn fn; void *arg; double enq; } task_t;

typedef struct pool pool_t;
typedef struct { pool_t *pool; int id; long tasks; double busy, wait; pthread_t tid; } worker_t;

struct pool {
    task_t *queue; int cap, head, tail, count;   /* bounded circular FIFO */
    pthread_mutex_t lock;
    pthread_cond_t not_empty, not_full;
    worker_t *workers; int nworkers; int stop; int verbose;
};

static double now(void) {
    struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t);
    return t.tv_sec + t.tv_nsec / 1e9;
}

static void *worker_loop(void *arg);

pool_t *pool_create(int nworkers, int qcap, int verbose) {
    pool_t *p = calloc(1, sizeof *p);
    p->queue = calloc(qcap, sizeof(task_t));
    p->workers = calloc(nworkers, sizeof(worker_t));
    p->cap = qcap; p->nworkers = nworkers; p->verbose = verbose;
    pthread_mutex_init(&p->lock, NULL);
    pthread_cond_init(&p->not_empty, NULL);
    pthread_cond_init(&p->not_full, NULL);
    for (int i = 0; i < nworkers; i++) {
        p->workers[i].pool = p; p->workers[i].id = i;
        pthread_create(&p->workers[i].tid, NULL, worker_loop, &p->workers[i]);
    }
    return p;
}

/* ---------- Module 2: worker thread loop (consumer) ---------- */
static void *worker_loop(void *arg) {
    worker_t *w = arg; pool_t *p = w->pool;
    for (;;) {
        pthread_mutex_lock(&p->lock);
        while (p->count == 0 && !p->stop)
            pthread_cond_wait(&p->not_empty, &p->lock);   /* sleep, no busy-wait */
        if (p->count == 0 && p->stop) { pthread_mutex_unlock(&p->lock); break; }
        task_t t = p->queue[p->head];
        p->head = (p->head + 1) % p->cap; p->count--;
        pthread_cond_signal(&p->not_full);
        pthread_mutex_unlock(&p->lock);                   /* run task outside lock */
        double s = now();
        w->wait += s - t.enq;
        if (p->verbose) printf("[Worker %d] picked up task\n", w->id);
        t.fn(t.arg);
        w->busy += now() - s; w->tasks++;
    }
    return NULL;
}

/* ---------- Module 3: task submission and graceful shutdown ---------- */
int pool_submit(pool_t *p, task_fn fn, void *arg) {       /* producer */
    pthread_mutex_lock(&p->lock);
    while (p->count == p->cap && !p->stop)
        pthread_cond_wait(&p->not_full, &p->lock);        /* block if queue full */
    if (p->stop) { pthread_mutex_unlock(&p->lock); return -1; }
    p->queue[p->tail] = (task_t){ fn, arg, now() };
    p->tail = (p->tail + 1) % p->cap; p->count++;
    pthread_cond_signal(&p->not_empty);
    pthread_mutex_unlock(&p->lock);
    return 0;
}

void pool_destroy(pool_t *p) {
    pthread_mutex_lock(&p->lock);
    p->stop = 1;
    pthread_cond_broadcast(&p->not_empty);                /* wake all workers */
    pthread_cond_broadcast(&p->not_full);
    pthread_mutex_unlock(&p->lock);
    for (int i = 0; i < p->nworkers; i++) pthread_join(p->workers[i].tid, NULL);
    pthread_mutex_destroy(&p->lock);
    pthread_cond_destroy(&p->not_empty); pthread_cond_destroy(&p->not_full);
    free(p->queue); free(p->workers); free(p);
}

/* ---------- Module 4: demo program and thread-per-task baseline ---------- */
static volatile sig_atomic_t g_stop = 0;
static void on_signal(int s) { (void)s; g_stop = 1; }     /* only sets a flag */

static void cpu_task(void *a) { (void)a; volatile double x = 0; for (int i = 0; i < 300000; i++) x += i * 0.5; }
static void io_task(void *a)  { (void)a; usleep(2000); }
static void run_task(void *a) { if ((long)a % 2) io_task(a); else cpu_task(a); }

typedef struct { double enq, wait; long idx; } tpt_arg;
static void *tpt_thread(void *a) { tpt_arg *t = a; t->wait = now() - t->enq; run_task((void *)t->idx); return NULL; }

static void benchmark(int ntasks, int nworkers) {
    double t0 = now();
    pool_t *p = pool_create(nworkers, 64, 0);
    for (long i = 0; i < ntasks && !g_stop; i++) pool_submit(p, run_task, (void *)i);
    double busy = 0, wait = 0; long done = 0;
    pthread_mutex_lock(&p->lock); p->stop = 1;
    pthread_cond_broadcast(&p->not_empty); pthread_mutex_unlock(&p->lock);
    for (int i = 0; i < p->nworkers; i++) pthread_join(p->workers[i].tid, NULL);
    for (int i = 0; i < p->nworkers; i++) { busy += p->workers[i].busy; wait += p->workers[i].wait; done += p->workers[i].tasks; }
    double wall = now() - t0;
    printf("POOL %2d workers | %ld tasks | %.3fs | %.0f/s | wait %.1fms | util %.0f%%\n",
           nworkers, done, wall, done / wall, 1000 * wait / done, 100 * busy / (nworkers * wall));
    free(p->queue); free(p->workers); free(p);

    t0 = now();
    pthread_t *th = malloc(sizeof(pthread_t) * ntasks); tpt_arg *ta = malloc(sizeof(tpt_arg) * ntasks);
    for (int i = 0; i < ntasks; i++) { ta[i].enq = now(); ta[i].idx = i; pthread_create(&th[i], NULL, tpt_thread, &ta[i]); }
    double w2 = 0;
    for (int i = 0; i < ntasks; i++) { pthread_join(th[i], NULL); w2 += ta[i].wait; }
    wall = now() - t0;
    printf("THREAD-PER-TASK | %d tasks | %.3fs | %.0f/s | wait %.1fms | %d threads\n",
           ntasks, wall, ntasks / wall, 1000 * w2 / ntasks, ntasks);
    free(th); free(ta);
}

static void demo(void) {
    pool_t *p = pool_create(4, 8, 1);
    for (long i = 0; i < 10 && !g_stop; i++) pool_submit(p, run_task, (void *)i);
    pool_destroy(p);
    printf("Graceful shutdown complete: all pending tasks finished, threads joined.\n");
}

int main(int argc, char **argv) {
    signal(SIGINT, on_signal); signal(SIGTERM, on_signal);
    if (argc > 1 && !strcmp(argv[1], "demo")) { demo(); return 0; }
    int n = argc > 1 ? atoi(argv[1]) : 2000;
    int sizes[] = { 2, 4, 8, 16 };
    for (int i = 0; i < 4; i++) benchmark(n, sizes[i]);
    return 0;
}
