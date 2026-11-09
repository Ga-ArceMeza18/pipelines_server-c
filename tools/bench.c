
// bench.c - Benchmark del servidor concurrente de pipelines
//
// Experimentos:
//   wide      : trabajos independientes, variando agentes.
//   deep      : cadena de dependencias, variando agentes.
//   combined  : varias pipelines simultaneas, variando agentes.
//   threads   : Cpu@1, variando hilos por tarea.
//   wordcount : WordCount@1, variando hilos OpenMP.
//   histogram : Histogram@1, variando hilos OpenMP.
//
// Compilar:
//   gcc -O2 -Wall -Wextra -o bench bench.c net_util.c
//
// Ejemplo:
//   ./bench -s ./server -r 5 -a 1,2,4,8 -t 1,2,4,8 -o benchmark.csv
//
// Solo algunos experimentos:
//   ./bench -e wide,deep,combined -r 5
//
// En otro equipo:
//   ./bench -E servidor_ecci -o benchmark_ecci.csv

#define _GNU_SOURCE

#include "net_util.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <netinet/in.h>
#include <signal.h>
#include <spawn.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

extern char** environ;

#define MAX_LIST 16
#define MAX_PIPELINES 128
#define POLL_MS 5
#define RUN_TIMEOUT_S 900.0

static struct {
    const char* server;
    const char* base;
    const char* csv_path;
    const char* only;
    const char* environment;

    int port;
    int reps;
    int jobs;
    int pipelines;

    int agents[MAX_LIST];
    int n_agents;

    int threads[MAX_LIST];
    int n_threads;

    unsigned long long iters;
    long lines;

    char workdir[512];
} g;

// =============================================================================
// Utilidades

static double now_s(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);

    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static void sleep_ms(long ms) {
    struct timespec ts = {
        ms / 1000,
        (ms % 1000) * 1000000L
    };

    nanosleep(&ts, NULL);
}

typedef struct {
    char* s;
    size_t len;
    size_t cap;
} Sb;

static void sb_append(Sb* b, const char* data, size_t n) {
    if (b->len + n + 1 > b->cap) {
        size_t cap = b->cap ? b->cap : 256;

        while (cap < b->len + n + 1) {
            cap *= 2;
        }

        char* tmp = realloc(b->s, cap);

        if (tmp == NULL) {
            fprintf(stderr, "bench: sin memoria\n");
            exit(EXIT_FAILURE);
        }

        b->s = tmp;
        b->cap = cap;
    }

    memcpy(b->s + b->len, data, n);
    b->len += n;
    b->s[b->len] = '\0';
}

static void sb_printf(Sb* b, const char* fmt, ...) {
    char tmp[512];
    va_list ap;

    va_start(ap, fmt);
    int n = vsnprintf(tmp, sizeof(tmp), fmt, ap);
    va_end(ap);

    if (n < 0) {
        return;
    }

    if ((size_t)n >= sizeof(tmp)) {
        n = (int)sizeof(tmp) - 1;
    }

    sb_append(b, tmp, (size_t)n);
}

static int rm_callback(const char* path, const struct stat* sb,
                       int flag, struct FTW* ftw) {
    (void)sb;
    (void)flag;
    (void)ftw;

    return remove(path);
}

static void clean_workdir(void) {
    struct stat st;

    if (stat(g.workdir, &st) == 0) {
        nftw(g.workdir, rm_callback, 16, FTW_DEPTH | FTW_PHYS);
    }
}

// =============================================================================
// Comunicación con el servidor

static char* request(const char* line, const char* body, size_t body_len) {
    int fd = nu_connect("127.0.0.1", (unsigned short)g.port);

    if (fd < 0) {
        return NULL;
    }

    if (nu_write_all(fd, line, strlen(line)) != 0
        || (body != NULL && nu_write_all(fd, body, body_len) != 0)) {
        close(fd);
        return NULL;
    }

    Sb out = {0};
    char buffer[4096];

    for (;;) {
        ssize_t n = read(fd, buffer, sizeof(buffer));

        if (n < 0 && errno == EINTR) {
            continue;
        }

        if (n <= 0) {
            break;
        }

        sb_append(&out, buffer, (size_t)n);
    }

    close(fd);

    return out.s != NULL ? out.s : strdup("");
}

static int port_open(void) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);

    if (fd < 0) {
        return 0;
    }

    struct sockaddr_in address;
    memset(&address, 0, sizeof(address));

    address.sin_family = AF_INET;
    address.sin_port = htons((unsigned short)g.port);
    inet_pton(AF_INET, "127.0.0.1", &address.sin_addr);

    int ok = connect(fd, (struct sockaddr*)&address, sizeof(address)) == 0;
    close(fd);

    return ok;
}

static pid_t start_server(int agents, int threads) {
    char port[16], ag[16], th[16];
    char* argv[6];
    int n = 0;

    snprintf(port, sizeof(port), "%d", g.port);
    snprintf(ag, sizeof(ag), "%d", agents);
    snprintf(th, sizeof(th), "%d", threads);

    argv[n++] = (char*)g.server;
    argv[n++] = port;
    argv[n++] = ag;
    argv[n++] = g.workdir;

    if (threads > 0) {
        argv[n++] = th;
    }

    argv[n] = NULL;

    posix_spawn_file_actions_t actions;
    posix_spawn_file_actions_init(&actions);

    posix_spawn_file_actions_addopen(&actions, 0, "/dev/null", O_RDONLY, 0);
    posix_spawn_file_actions_addopen(&actions, 1, "/dev/null", O_WRONLY, 0);

    pid_t pid;
    int rc = posix_spawnp(&pid, g.server, &actions, NULL, argv, environ);

    posix_spawn_file_actions_destroy(&actions);

    if (rc != 0) {
        fprintf(stderr, "bench: no se pudo lanzar %s: %s\n",
                g.server, strerror(rc));
        return -1;
    }

    for (int i = 0; i < 2000; i++) {
        int status;

        if (waitpid(pid, &status, WNOHANG) == pid) {
            fprintf(stderr, "bench: el servidor terminó al arrancar\n");
            return -1;
        }

        if (port_open()) {
            return pid;
        }

        sleep_ms(5);
    }

    fprintf(stderr, "bench: el servidor no abrió el puerto %d\n", g.port);

    kill(pid, SIGKILL);
    waitpid(pid, NULL, 0);

    return -1;
}

static void stop_server(pid_t pid) {
    free(request("SHUTDOWN\n", NULL, 0));

    for (int i = 0; i < 4000; i++) {
        int status;
        pid_t w = waitpid(pid, &status, WNOHANG);

        if (w == pid || (w < 0 && errno != EINTR)) {
            return;
        }

        sleep_ms(5);
    }

    kill(pid, SIGKILL);
    waitpid(pid, NULL, 0);
}

// =============================================================================
// Resultados y lectura de STATUS

typedef struct {
    char state[32];
    double wall;
    double server;
    double step;
} Result;

static int is_terminal(const char* s) {
    return strcmp(s, "succeeded") == 0
        || strcmp(s, "partiallySucceeded") == 0
        || strcmp(s, "failed") == 0
        || strcmp(s, "canceled") == 0;
}

static double find_step_time(const char* text, int target) {
    const char* p = text;

    while (*p != '\0') {
        const char* end = strchr(p, '\n');
        size_t len = end ? (size_t)(end - p) : strlen(p);

        char line[512];

        if (len >= sizeof(line)) {
            len = sizeof(line) - 1;
        }

        memcpy(line, p, len);
        line[len] = '\0';

        int number;
        double seconds;

        if (sscanf(line, " Paso %d (%*[^)]) [%*[^]]] %lf",
                   &number, &seconds) == 2 && number == target) {
            return seconds;
        }

        if (end == NULL) {
            break;
        }

        p = end + 1;
    }

    return -1.0;
}

static int submit_pipeline(const char* yaml, int* id) {
    char line[64];

    snprintf(line, sizeof(line), "SUBMIT %zu\n", strlen(yaml));

    char* reply = request(line, yaml, strlen(yaml));

    if (reply == NULL || sscanf(reply, "OK %d", id) != 1) {
        fprintf(stderr, "bench: no se pudo enviar pipeline: %s\n",
                reply != NULL ? reply : "(sin respuesta)");

        free(reply);
        return -1;
    }

    free(reply);
    return 0;
}

static int read_pipeline_status(int id, Result* res, int step_target) {
    char line[64];

    snprintf(line, sizeof(line), "STATUS %d\n", id);

    char* text = request(line, NULL, 0);

    if (text == NULL) {
        return 0;
    }

    const char* p = strstr(text, "Estado: ");
    char state[32];
    double duration;

    if (p != NULL
        && sscanf(p, "Estado: %31s Duracion: %lf", state, &duration) == 2
        && is_terminal(state)) {

        snprintf(res->state, sizeof(res->state), "%s", state);
        res->server = duration;
        res->step = step_target > 0
            ? find_step_time(text, step_target)
            : -1.0;

        free(text);
        return 1;
    }

    free(text);
    return 0;
}

static int run_pipeline(const char* yaml, int step_target, Result* res) {
    memset(res, 0, sizeof(*res));

    strcpy(res->state, "error");
    res->wall = -1.0;
    res->server = -1.0;
    res->step = -1.0;

    double t0 = now_s();
    int id;

    if (submit_pipeline(yaml, &id) != 0) {
        return -1;
    }

    for (;;) {
        if (read_pipeline_status(id, res, step_target)) {
            res->wall = now_s() - t0;
            return 0;
        }

        if (now_s() - t0 > RUN_TIMEOUT_S) {
            fprintf(stderr, "bench: pipeline %d superó el tiempo máximo\n", id);
            return -1;
        }

        sleep_ms(POLL_MS);
    }
}

// =============================================================================
// Pipelines YAML

static void yaml_header(Sb* b, const char* name, const char* stage) {
    sb_printf(b,
              "name: %s\n"
              "stages:\n"
              "  - stage: %s\n"
              "    jobs:\n",
              name, stage);
}

// Wide: todos los trabajos son independientes.
static char* yaml_wide(int jobs, unsigned long long iterations) {
    Sb b = {0};

    yaml_header(&b, "bench-wide", "compute");

    for (int j = 0; j < jobs; j++) {
        sb_printf(&b,
                  "      - job: j%d\n"
                  "        steps:\n"
                  "          - task: Cpu@1\n"
                  "            inputs:\n"
                  "              iterations: %llu\n"
                  "              seed: %d\n",
                  j, iterations, j + 1);
    }

    return b.s;
}

// Deep: cada trabajo depende del anterior.
static char* yaml_deep(int jobs, unsigned long long iterations) {
    Sb b = {0};

    yaml_header(&b, "bench-deep", "compute");

    for (int j = 0; j < jobs; j++) {
        sb_printf(&b, "      - job: j%d\n", j);

        if (j > 0) {
            sb_printf(&b,
                      "        dependsOn:\n"
                      "          - j%d\n",
                      j - 1);
        }

        sb_printf(&b,
                  "        steps:\n"
                  "          - task: Cpu@1\n"
                  "            inputs:\n"
                  "              iterations: %llu\n"
                  "              seed: %d\n",
                  iterations, j + 1);
    }

    return b.s;
}

static char* yaml_data(const char* task, long lines) {
    Sb b = {0};

    yaml_header(&b, "bench-data", "process");

    sb_printf(&b,
              "      - job: data\n"
              "        steps:\n"
              "          - script: \"seq 1 %ld > data.txt\"\n"
              "          - task: %s\n"
              "            inputs:\n"
              "              file: data.txt\n",
              lines, task);

    if (strcmp(task, "Histogram@1") == 0) {
        sb_printf(&b,
                  "              output: hist.csv\n"
                  "              bins: 1000\n");
    } else {
        sb_printf(&b, "              output: out.txt\n");
    }

    return b.s;
}

// =============================================================================
// CSV y ejecución de experimentos

static void write_result(FILE* csv, const char* experiment, const char* task,
                         int agents, int threads, int jobs, int pipelines,
                         int rep, const Result* r) {
    fprintf(csv,
            "%s,%s,%s,%d,%d,%d,%d,%d,%s,%.6f,%.6f,%.6f\n",
            g.environment,
            experiment,
            task,
            agents,
            threads,
            jobs,
            pipelines,
            rep,
            r->state,
            r->wall,
            r->server,
            r->step);

    fflush(csv);
}

static void run_config(FILE* csv, const char* experiment, const char* task,
                       int agents, int threads, int jobs, int pipelines,
                       const char* yaml, int step_target) {
    clean_workdir();

    pid_t pid = start_server(agents, threads);

    if (pid < 0) {
        return;
    }

    for (int rep = 1; rep <= g.reps; rep++) {
        Result r;

        run_pipeline(yaml, step_target, &r);

        write_result(csv, experiment, task, agents, threads,
                     jobs, pipelines, rep, &r);

        fprintf(stderr,
                "[%-9s] agentes=%-2d hilos=%-2d rep=%d/%d: %s %.3f s\n",
                experiment, agents, threads, rep, g.reps, r.state, r.wall);
    }

    stop_server(pid);
    clean_workdir();
}

// Envía varias pipelines antes de consultar sus estados.
// Así permanecen simultáneamente en el servidor y compiten por los agentes.
static int run_combined(const char* yaml, int count, Result* result) {
    int ids[MAX_PIPELINES];
    int done[MAX_PIPELINES] = {0};
    Result results[MAX_PIPELINES];

    memset(result, 0, sizeof(*result));

    strcpy(result->state, "error");
    result->wall = -1.0;
    result->server = -1.0;
    result->step = -1.0;

    double t0 = now_s();

    for (int i = 0; i < count; i++) {
        if (submit_pipeline(yaml, &ids[i]) != 0) {
            return -1;
        }

        memset(&results[i], 0, sizeof(Result));
        strcpy(results[i].state, "running");
        results[i].server = -1.0;
        results[i].step = -1.0;
    }

    int completed = 0;
    double max_server = 0.0;
    int all_succeeded = 1;

    while (completed < count) {
        for (int i = 0; i < count; i++) {
            if (done[i]) {
                continue;
            }

            if (read_pipeline_status(ids[i], &results[i], 0)) {
                done[i] = 1;
                completed++;

                if (results[i].server > max_server) {
                    max_server = results[i].server;
                }

                if (strcmp(results[i].state, "succeeded") != 0) {
                    all_succeeded = 0;
                }
            }
        }

        if (completed == count) {
            break;
        }

        if (now_s() - t0 > RUN_TIMEOUT_S) {
            fprintf(stderr, "bench: tiempo máximo en combined\n");
            return -1;
        }

        sleep_ms(POLL_MS);
    }

    result->wall = now_s() - t0;
    result->server = max_server;
    result->step = -1.0;

    strcpy(result->state, all_succeeded ? "succeeded" : "failed");

    return 0;
}

static void run_combined_config(FILE* csv, int agents, const char* yaml) {
    clean_workdir();

    pid_t pid = start_server(agents, 1);

    if (pid < 0) {
        return;
    }

    for (int rep = 1; rep <= g.reps; rep++) {
        Result r;

        run_combined(yaml, g.pipelines, &r);

        write_result(csv, "combined", "Cpu@1", agents, 1,
                     g.jobs, g.pipelines, rep, &r);

        fprintf(stderr,
                "[combined ] agentes=%-2d pipelines=%-2d rep=%d/%d: %s %.3f s\n",
                agents, g.pipelines, rep, g.reps, r.state, r.wall);
    }

    stop_server(pid);
    clean_workdir();
}

// =============================================================================
// Selección de experimentos y opciones

static int selected(const char* name) {
    if (g.only == NULL) {
        return 1;
    }

    char* copy = strdup(g.only);

    if (copy == NULL) {
        return 0;
    }

    char* save = NULL;
    int found = 0;

    for (char* tok = strtok_r(copy, ",", &save);
         tok != NULL;
         tok = strtok_r(NULL, ",", &save)) {
        if (strcmp(tok, name) == 0) {
            found = 1;
            break;
        }
    }

    free(copy);
    return found;
}

static int parse_list(const char* text, int* out) {
    char* copy = strdup(text);
    char* save = NULL;
    int n = 0;

    if (copy == NULL) {
        return -1;
    }

    for (char* tok = strtok_r(copy, ",", &save);
         tok != NULL && n < MAX_LIST;
         tok = strtok_r(NULL, ",", &save)) {
        char* end;
        long value = strtol(tok, &end, 10);

        if (*tok == '\0' || *end != '\0' || value <= 0 || value > 100000) {
            free(copy);
            return -1;
        }

        out[n++] = (int)value;
    }

    free(copy);
    return n > 0 ? n : -1;
}

static int default_list(int* out) {
    long cpus = sysconf(_SC_NPROCESSORS_ONLN);
    int n = 0;

    if (cpus < 1) {
        cpus = 1;
    }

    for (int v = 1; v <= cpus && n < MAX_LIST; v *= 2) {
        out[n++] = v;
    }

    if (out[n - 1] != cpus && n < MAX_LIST) {
        out[n++] = (int)cpus;
    }

    return n;
}

static void usage(const char* program) {
    fprintf(stderr,
            "Uso: %s [opciones]\n"
            "  -s <ruta>     ejecutable del servidor (./server)\n"
            "  -p <puerto>   puerto (5100)\n"
            "  -w <carpeta>  carpeta base (.)\n"
            "  -o <archivo>  CSV (benchmark.csv)\n"
            "  -E <nombre>   nombre del entorno (local)\n"
            "  -r <n>        repeticiones (5)\n"
            "  -a <lista>    agentes, ej. 1,2,4,8\n"
            "  -t <lista>    hilos OpenMP, ej. 1,2,4,8\n"
            "  -j <n>        trabajos por pipeline (8)\n"
            "  -c <n>        pipelines simultaneas (4)\n"
            "  -i <n>        iteraciones de Cpu@1\n"
            "  -n <n>        lineas para WordCount/Histogram\n"
            "  -e <lista>    wide,deep,combined,threads,wordcount,histogram\n",
            program);
}

int main(int argc, char* argv[]) {
    g.server = "./server";
    g.base = ".";
    g.csv_path = "benchmark.csv";
    g.environment = "local";

    g.port = 5100;
    g.reps = 5;
    g.jobs = 8;
    g.pipelines = 4;

    g.iters = 400000000ULL;
    g.lines = 5000000;

    g.only = NULL;

    g.n_agents = default_list(g.agents);
    g.n_threads = default_list(g.threads);

    int opt;

    while ((opt = getopt(argc, argv, "s:p:w:o:E:r:a:t:j:c:i:n:e:h")) != -1) {
        switch (opt) {
            case 's': g.server = optarg; break;
            case 'p': g.port = atoi(optarg); break;
            case 'w': g.base = optarg; break;
            case 'o': g.csv_path = optarg; break;
            case 'E': g.environment = optarg; break;
            case 'r': g.reps = atoi(optarg); break;
            case 'a': g.n_agents = parse_list(optarg, g.agents); break;
            case 't': g.n_threads = parse_list(optarg, g.threads); break;
            case 'j': g.jobs = atoi(optarg); break;
            case 'c': g.pipelines = atoi(optarg); break;
            case 'i': g.iters = strtoull(optarg, NULL, 10); break;
            case 'n': g.lines = atol(optarg); break;
            case 'e': g.only = optarg; break;

            default:
                usage(argv[0]);
                return EXIT_FAILURE;
        }
    }

    if (g.port <= 0 || g.port > 65535
        || g.reps < 5
        || g.jobs <= 0
        || g.pipelines <= 0 || g.pipelines > MAX_PIPELINES
        || g.iters == 0 || g.lines <= 0
        || g.n_agents <= 0 || g.n_threads <= 0) {
        fprintf(stderr,
                "bench: opciones invalidas (las repeticiones deben ser >= 5)\n");
        usage(argv[0]);
        return EXIT_FAILURE;
    }

    snprintf(g.workdir, sizeof(g.workdir), "%s/bench_tmp", g.base);

    signal(SIGPIPE, SIG_IGN);

    FILE* csv = fopen(g.csv_path, "w");

    if (csv == NULL) {
        fprintf(stderr, "bench: no se pudo crear %s: %s\n",
                g.csv_path, strerror(errno));
        return EXIT_FAILURE;
    }

    fprintf(csv,
            "environment,experiment,task,agents,threads,jobs,pipelines,"
            "rep,state,wall_s,server_s,step_s\n");

    // Wide: trabajos independientes.
    if (selected("wide")) {
        char* yaml = yaml_wide(g.jobs, g.iters);

        for (int i = 0; i < g.n_agents; i++) {
            run_config(csv, "wide", "Cpu@1", g.agents[i], 1,
                       g.jobs, 1, yaml, 0);
        }

        free(yaml);
    }

    // Deep: cadena de dependencias.
    if (selected("deep")) {
        char* yaml = yaml_deep(g.jobs, g.iters);

        for (int i = 0; i < g.n_agents; i++) {
            run_config(csv, "deep", "Cpu@1", g.agents[i], 1,
                       g.jobs, 1, yaml, 0);
        }

        free(yaml);
    }

    // Combined: varias pipelines enviadas antes de esperar resultados.
    if (selected("combined")) {
        char* yaml = yaml_wide(g.jobs, g.iters);

        for (int i = 0; i < g.n_agents; i++) {
            run_combined_config(csv, g.agents[i], yaml);
        }

        free(yaml);
    }

    // Cpu@1 variando los hilos disponibles para las tareas.
    if (selected("threads")) {
        char* yaml = yaml_wide(1, g.iters);

        for (int i = 0; i < g.n_threads; i++) {
            run_config(csv, "threads", "Cpu@1", 1, g.threads[i],
                       1, 1, yaml, 1);
        }

        free(yaml);
    }

    // WordCount@1 variando hilos OpenMP.
    if (selected("wordcount")) {
        char* yaml = yaml_data("WordCount@1", g.lines);

        for (int i = 0; i < g.n_threads; i++) {
            run_config(csv, "wordcount", "WordCount@1", 1,
                       g.threads[i], 1, 1, yaml, 2);
        }

        free(yaml);
    }

    // Histogram@1 variando hilos OpenMP.
    if (selected("histogram")) {
        char* yaml = yaml_data("Histogram@1", g.lines);

        for (int i = 0; i < g.n_threads; i++) {
            run_config(csv, "histogram", "Histogram@1", 1,
                       g.threads[i], 1, 1, yaml, 2);
        }

        free(yaml);
    }

    fclose(csv);

    fprintf(stderr, "\nListo: resultados en %s\n", g.csv_path);

    return EXIT_SUCCESS;
}
