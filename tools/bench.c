// bench.c - Prueba de rendimiento del servidor de pipelines
//
// Para cada configuración (agentes, hilos por tarea) arranca el servidor,
// envía pipelines por el protocolo de texto (SUBMIT / STATUS / SHUTDOWN),
// espera a que terminen y guarda los tiempos en un CSV que luego lee plot.py.
//
// Experimentos:
//   agents    : N trabajos Cpu@1 independientes, variando los agentes (1 hilo por tarea)
//   sleep     : N trabajos Sleep@1 independientes, variando los agentes (mide el
//               costo del planificador sin depender de los núcleos)
//   threads   : un solo trabajo Cpu@1, variando los hilos por tarea
//   wordcount : WordCount@1 sobre un archivo generado, variando los hilos por tarea
//   histogram : Histogram@1 sobre un archivo generado, variando los hilos por tarea
//
// Compilar:  gcc -O2 -Wall -Wextra -o bench bench.c net_util.c
// Usar:      ./bench -s ./server -r 3 -o benchmark.csv
//            ./bench -h   (muestra todas las opciones)

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
#define POLL_MS 5
#define RUN_TIMEOUT_S 900.0

// Configuración (se llena en main a partir de las opciones)
static struct {
    const char* server;       // ejecutable del servidor
    int port;                 // puerto donde se levanta
    const char* base;         // carpeta donde se crea bench_tmp
    char workdir[512];        // <base>/bench_tmp (se borra entre configuraciones)
    int reps;                 // repeticiones por configuración
    const char* csv_path;     // archivo de salida
    int agents[MAX_LIST];
    int n_agents;
    int threads[MAX_LIST];
    int n_threads;
    int jobs;                 // trabajos paralelos en los experimentos agents y sleep
    unsigned long long iters; // iteraciones de Cpu@1
    long lines;               // líneas del archivo de WordCount e Histogram
    double sleep_s;           // segundos de cada Sleep@1
    const char* only;         // experimentos elegidos (NULL = todos)
} g;

// =============================================================================
// Utilidades

static double now_s(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);

    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static void sleep_ms(long ms) {
    struct timespec ts = {ms / 1000, (ms % 1000) * 1000000L};
    nanosleep(&ts, NULL);
}

// Cadena que crece sola
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

static int rm_callback(const char* path, const struct stat* sb, int flag, struct FTW* ftw) {
    (void)sb;
    (void)flag;
    (void)ftw;

    return remove(path);
}

// Borra la carpeta de trabajo del benchmark (siempre <base>/bench_tmp)
static void clean_workdir(void) {
    struct stat st;

    if (stat(g.workdir, &st) == 0) {
        nftw(g.workdir, rm_callback, 16, FTW_DEPTH | FTW_PHYS);
    }
}

// =============================================================================
// Comunicación con el servidor

// Envía una solicitud y devuelve toda la respuesta (hasta que el servidor cierre)
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

// Revisa si ya hay algo escuchando en el puerto (sin imprimir errores)
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

// Arranca el servidor y espera a que acepte conexiones. Devuelve el pid o -1
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

    // El servidor imprime mensajes: se mandan a /dev/null
    posix_spawn_file_actions_t actions;
    posix_spawn_file_actions_init(&actions);
    posix_spawn_file_actions_addopen(&actions, 0, "/dev/null", O_RDONLY, 0);
    posix_spawn_file_actions_addopen(&actions, 1, "/dev/null", O_WRONLY, 0);

    pid_t pid;
    int rc = posix_spawnp(&pid, g.server, &actions, NULL, argv, environ);
    posix_spawn_file_actions_destroy(&actions);

    if (rc != 0) {
        fprintf(stderr, "bench: no se pudo lanzar %s: %s\n", g.server, strerror(rc));
        return -1;
    }

    // Espera hasta 10 s a que el puerto abra (o a que el servidor muera)
    for (int i = 0; i < 2000; i++) {
        int status;

        if (waitpid(pid, &status, WNOHANG) == pid) {
            fprintf(stderr, "bench: el servidor terminó al arrancar (¿puerto %d ocupado?)\n",
                    g.port);
            return -1;
        }
        if (port_open()) {
            return pid;
        }

        sleep_ms(5);
    }

    fprintf(stderr, "bench: el servidor no abrió el puerto %d a tiempo\n", g.port);
    kill(pid, SIGKILL);
    waitpid(pid, NULL, 0);

    return -1;
}

static void stop_server(pid_t pid) {
    free(request("SHUTDOWN\n", NULL, 0));

    // Espera hasta 20 s a que el proceso termine; si no, lo mata
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
// Ejecución de un pipeline y lectura de tiempos

typedef struct {
    char state[32];
    double wall;    // desde el SUBMIT hasta ver el estado final (medido aquí)
    double server;  // "Duracion" que reporta el servidor para la ejecución
    double step;    // duración de un paso concreto (-1 si no aplica)
} Result;

static int is_terminal(const char* s) {
    return strcmp(s, "succeeded") == 0 || strcmp(s, "partiallySucceeded") == 0
        || strcmp(s, "failed") == 0 || strcmp(s, "canceled") == 0;
}

// Busca en el texto de STATUS la duración del paso número 'target' (1, 2, ...)
// Las líneas tienen la forma: "    Paso 2 (task) [succeeded] 0.34s exit=0 : ..."
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

        if (sscanf(line, " Paso %d (%*[^)]) [%*[^]]] %lf", &number, &seconds) == 2
            && number == target) {
            return seconds;
        }

        if (end == NULL) {
            break;
        }
        p = end + 1;
    }

    return -1.0;
}

// Envía el pipeline, espera a que termine y llena res. 0 si salió bien
static int run_pipeline(const char* yaml, int step_target, Result* res) {
    memset(res, 0, sizeof(*res));
    snprintf(res->state, sizeof(res->state), "error");
    res->wall = res->server = res->step = -1.0;

    size_t len = strlen(yaml);
    char line[64];

    snprintf(line, sizeof(line), "SUBMIT %zu\n", len);

    double t0 = now_s();
    char* reply = request(line, yaml, len);
    int id;

    if (reply == NULL || sscanf(reply, "OK %d", &id) != 1) {
        fprintf(stderr, "bench: el servidor no aceptó el pipeline: %s\n",
                reply != NULL ? reply : "(sin respuesta)");
        free(reply);
        return -1;
    }
    free(reply);

    snprintf(line, sizeof(line), "STATUS %d\n", id);

    for (;;) {
        char* text = request(line, NULL, 0);
        double t = now_s();

        if (text != NULL) {
            const char* p = strstr(text, "Estado: ");
            char state[32];
            double duration;

            if (p != NULL && sscanf(p, "Estado: %31s Duracion: %lf", state, &duration) == 2
                && is_terminal(state)) {
                res->wall = t - t0;
                res->server = duration;
                res->step = step_target > 0 ? find_step_time(text, step_target) : -1.0;
                snprintf(res->state, sizeof(res->state), "%s", state);
                free(text);
                return 0;
            }
            free(text);
        }

        if (t - t0 > RUN_TIMEOUT_S) {
            fprintf(stderr, "bench: la ejecución %d superó el tiempo máximo\n", id);
            return -1;
        }

        sleep_ms(POLL_MS);
    }
}

// =============================================================================
// Generadores de pipelines YAML (quien llama libera el resultado)

static void yaml_header(Sb* b, const char* name, const char* stage) {
    sb_printf(b, "name: %s\nstages:\n  - stage: %s\n    jobs:\n", name, stage);
}

static char* yaml_cpu(int jobs, unsigned long long iterations) {
    Sb b = {0};

    yaml_header(&b, "bench-cpu", "compute");

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

static char* yaml_sleep(int jobs, double seconds) {
    Sb b = {0};

    yaml_header(&b, "bench-sleep", "wait");

    for (int j = 0; j < jobs; j++) {
        sb_printf(&b,
                  "      - job: j%d\n"
                  "        steps:\n"
                  "          - task: Sleep@1\n"
                  "            inputs:\n"
                  "              seconds: %.3f\n",
                  j, seconds);
    }

    return b.s;
}

// Un trabajo con dos pasos: el script genera data.txt y la tarea lo procesa.
// Por eso el tiempo que interesa es el del paso 2.
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
        sb_printf(&b, "              output: hist.csv\n              bins: 1000\n");
    } else {
        sb_printf(&b, "              output: out.txt\n");
    }

    return b.s;
}

// =============================================================================
// Experimentos

// Arranca el servidor con (agents, threads), corre el pipeline 'reps' veces y
// agrega una fila al CSV por cada repetición
static void run_config(FILE* csv, const char* experiment, const char* task, int agents,
                       int threads, int jobs, const char* yaml, int step_target) {
    clean_workdir();

    pid_t pid = start_server(agents, threads);

    if (pid < 0) {
        return;
    }

    for (int rep = 1; rep <= g.reps; rep++) {
        Result r;

        run_pipeline(yaml, step_target, &r);

        fprintf(csv, "%s,%s,%d,%d,%d,%d,%s,%.4f,%.4f,%.4f\n", experiment, task, agents,
                threads, jobs, rep, r.state, r.wall, r.server, r.step);
        fflush(csv);

        fprintf(stderr, "[%-9s] agentes=%-2d hilos=%-2d rep %d/%d: %s, %.3f s\n", experiment,
                agents, threads, rep, g.reps, r.state, r.wall);
    }

    stop_server(pid);
    clean_workdir();
}

static int selected(const char* name) {
    return g.only == NULL || strstr(g.only, name) != NULL;
}

// =============================================================================
// Opciones de línea de comandos

static int parse_list(const char* text, int* out) {
    char* copy = strdup(text);
    char* save = NULL;
    int n = 0;

    if (copy == NULL) {
        return -1;
    }

    for (char* tok = strtok_r(copy, ",", &save); tok != NULL && n < MAX_LIST;
         tok = strtok_r(NULL, ",", &save)) {
        int value = atoi(tok);

        if (value <= 0) {
            free(copy);
            return -1;
        }
        out[n++] = value;
    }

    free(copy);

    return n > 0 ? n : -1;
}

// Potencias de 2 hasta la cantidad de núcleos (y los núcleos si no es potencia de 2)
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
            "uso: %s [opciones]\n"
            "  -s <ruta>     ejecutable del servidor (por defecto ./server)\n"
            "  -p <puerto>   puerto del servidor (5100)\n"
            "  -w <carpeta>  carpeta donde se crea bench_tmp (.)\n"
            "  -o <archivo>  CSV de salida (benchmark.csv)\n"
            "  -r <n>        repeticiones por configuración (3)\n"
            "  -a <lista>    agentes a probar, ej: 1,2,4,8 (potencias de 2 hasta los núcleos)\n"
            "  -t <lista>    hilos por tarea a probar, ej: 1,2,4,8 (igual)\n"
            "  -j <n>        trabajos paralelos en agents y sleep (8)\n"
            "  -i <n>        iteraciones de Cpu@1 (400000000)\n"
            "  -n <n>        líneas del archivo de WordCount/Histogram (5000000)\n"
            "  -d <seg>      segundos de cada Sleep@1 (0.5)\n"
            "  -e <lista>    experimentos: agents,sleep,threads,wordcount,histogram (todos)\n",
            program);
}

int main(int argc, char* argv[]) {
    g.server = "./server";
    g.port = 5100;
    g.base = ".";
    g.csv_path = "benchmark.csv";
    g.reps = 3;
    g.jobs = 8;
    g.iters = 400000000ULL;
    g.lines = 5000000;
    g.sleep_s = 0.5;
    g.only = NULL;
    g.n_agents = default_list(g.agents);
    g.n_threads = default_list(g.threads);

    int opt;

    while ((opt = getopt(argc, argv, "s:p:w:o:r:a:t:j:i:n:d:e:h")) != -1) {
        switch (opt) {
            case 's': g.server = optarg; break;
            case 'p': g.port = atoi(optarg); break;
            case 'w': g.base = optarg; break;
            case 'o': g.csv_path = optarg; break;
            case 'r': g.reps = atoi(optarg); break;
            case 'a':
                g.n_agents = parse_list(optarg, g.agents);
                break;
            case 't':
                g.n_threads = parse_list(optarg, g.threads);
                break;
            case 'j': g.jobs = atoi(optarg); break;
            case 'i': g.iters = strtoull(optarg, NULL, 10); break;
            case 'n': g.lines = atol(optarg); break;
            case 'd': g.sleep_s = atof(optarg); break;
            case 'e': g.only = optarg; break;
            default:
                usage(argv[0]);
                return EXIT_FAILURE;
        }
    }

    if (g.port <= 0 || g.port > 65535 || g.reps <= 0 || g.jobs <= 0 || g.iters == 0
        || g.lines <= 0 || g.sleep_s <= 0.0 || g.n_agents < 0 || g.n_threads < 0) {
        fprintf(stderr, "bench: opciones inválidas\n");
        usage(argv[0]);
        return EXIT_FAILURE;
    }

    snprintf(g.workdir, sizeof(g.workdir), "%s/bench_tmp", g.base);

    // Si el servidor se cae, write da error en vez de matar al benchmark
    signal(SIGPIPE, SIG_IGN);

    FILE* csv = fopen(g.csv_path, "w");

    if (csv == NULL) {
        fprintf(stderr, "bench: no se pudo crear %s: %s\n", g.csv_path, strerror(errno));
        return EXIT_FAILURE;
    }

    fprintf(csv, "experiment,task,agents,threads,jobs,rep,state,wall_s,server_s,step_s\n");

    // agents: N trabajos independientes con 1 hilo por tarea, variando agentes
    if (selected("agents")) {
        char* yaml = yaml_cpu(g.jobs, g.iters);

        for (int i = 0; i < g.n_agents; i++) {
            run_config(csv, "agents", "Cpu@1", g.agents[i], 1, g.jobs, yaml, 0);
        }
        free(yaml);
    }

    // sleep: igual pero sin consumir CPU, deja ver el costo del planificador
    if (selected("sleep")) {
        char* yaml = yaml_sleep(g.jobs, g.sleep_s);

        for (int i = 0; i < g.n_agents; i++) {
            run_config(csv, "sleep", "Sleep@1", g.agents[i], 1, g.jobs, yaml, 0);
        }
        free(yaml);
    }

    // threads: un solo trabajo, 1 agente, variando los hilos de la tarea
    if (selected("threads")) {
        char* yaml = yaml_cpu(1, g.iters);

        for (int i = 0; i < g.n_threads; i++) {
            run_config(csv, "threads", "Cpu@1", 1, g.threads[i], 1, yaml, 1);
        }
        free(yaml);
    }

    // wordcount e histogram: el paso 2 es el que usa los hilos
    if (selected("wordcount")) {
        char* yaml = yaml_data("WordCount@1", g.lines);

        for (int i = 0; i < g.n_threads; i++) {
            run_config(csv, "wordcount", "WordCount@1", 1, g.threads[i], 1, yaml, 2);
        }
        free(yaml);
    }

    if (selected("histogram")) {
        char* yaml = yaml_data("Histogram@1", g.lines);

        for (int i = 0; i < g.n_threads; i++) {
            run_config(csv, "histogram", "Histogram@1", 1, g.threads[i], 1, yaml, 2);
        }
        free(yaml);
    }

    fclose(csv);
    fprintf(stderr, "listo: resultados en %s\n", g.csv_path);

    return EXIT_SUCCESS;
}
