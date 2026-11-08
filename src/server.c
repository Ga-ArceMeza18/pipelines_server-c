#define _POSIX_C_SOURCE 200809L

#include "engine.h"
#include "errors.h"
#include "net_util.h"
#include "parser.h"

#include <ctype.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#define LINE_MAX_LEN 512
#define MAX_TOKENS 5
#define RECV_TIMEOUT_SECONDS 30
#define MAX_YAML_BYTES (1024 * 1024)

struct Server;

// Cada conexión activa se guarda en una lista para poder despertar
// a las que están esperando una línea cuando llega SHUTDOWN
typedef struct Conn {
    int fd;
    struct Server* server;
    struct Conn* prev;
    struct Conn* next;
} Conn;

typedef struct Server {
    int listen_fd;
    int agents;
    int threads;
    const char* workdir;

    pthread_mutex_t mutex;    // Protege conns, active, stopping y shutdown_done
    pthread_cond_t changed;   // Cambió active o shutdown_done
    Conn* conns;              // Conexiones activas
    int active;               // Manejadores que no han terminado
    int stopping;             // 1 desde que llegó el primer SHUTDOWN
    int shutdown_done;        // 1 cuando terminó el apagado
} Server;

//Funciones

// Envía una línea de respuesta y le agrega el salto de línea
static void reply(int fd, const char* fmt, ...) {
    char buffer[1024];
    va_list args;

    va_start(args, fmt);
    int n = vsnprintf(buffer, sizeof(buffer) - 1, fmt, args);
    va_end(args);

    if (n < 0) {
        return;
    }
    if ((size_t)n > sizeof(buffer) - 2) {
        n = (int)sizeof(buffer) - 2;
    }
    buffer[n++] = '\n';

    nu_write_all(fd, buffer, (size_t)n);
}

// Envía un texto de varias líneas a como viene del engine
static void reply_text(int fd, const char* text) {
    nu_write_all(fd, text, strlen(text));
}

// Convierte un texto a entero positivo. Solo acepta dígitos
static int parse_int(const char* text, int* out) {
    char* end;

    if (text == NULL || !isdigit((unsigned char)text[0])) {
        return -1;
    }

    errno = 0;
    long value = strtol(text, &end, 10);
    if (errno != 0 || *end != '\0' || value > 1000000000L) {
        return -1;
    }

    *out = (int)value;
    return 0;
}

// Comandos

// SUBMIT <bytes>: después de la línea vienen los bytes del YAML
static void cmd_submit(Server* srv, int fd, int argc, char** argv) {
    int size;

    (void)srv;
    if (argc != 2 || parse_int(argv[1], &size) != 0 || size == 0) {
        reply(fd, "ERROR uso: SUBMIT <bytes>");
        return;
    }
    if (size > MAX_YAML_BYTES) {
        reply(fd, "ERROR el YAML no puede pasar de %d bytes", MAX_YAML_BYTES);
        return;
    }

    char* yaml = malloc((size_t)size + 1);
    if (yaml == NULL) {
        reply(fd, "ERROR servidor sin memoria");
        return;
    }
    if (nu_read_exact(fd, yaml, (size_t)size) != 0) {
        reply(fd, "ERROR no llegaron los %d bytes del YAML", size);
        free(yaml);
        return;
    }
    yaml[size] = '\0';

    // El parser también llama al validator, si algo falla devuelve NULL con los errores
    ErrorList errors;
    errors_init(&errors);
    Pipeline* pipeline = parse_pipeline(yaml, (size_t)size, &errors);
    free(yaml);

    if (pipeline == NULL) {
        reply(fd, "ERROR pipeline inválido (%d error(es))", errors.size);
        for (int i = 0; i < errors.size; i++) {
            if (errors.items[i].line > 0) {
                reply(fd, "línea %d: %s", errors.items[i].line, errors.items[i].message);
            } else {
                reply(fd, "%s", errors.items[i].message);
            }
        }
        if (errors.size == 0) {
            reply(fd, "el YAML está mal formado");
        }
        errors_destroy(&errors);
        return;
    }
    errors_destroy(&errors);

    // El engine se queda con el pipeline aunque falle
    int id = engine_submit(pipeline);
    if (id < 0) {
        reply(fd, "ERROR no se pudo aceptar el pipeline, el servidor se está apagando");
        return;
    }

    reply(fd, "OK %d", id);
}

static void cmd_status(Server* srv, int fd, int argc, char** argv) {
    int id;

    (void)srv;
    if (argc != 2 || parse_int(argv[1], &id) != 0) {
        reply(fd, "ERROR uso: STATUS <id>");
        return;
    }

    char* text = engine_status(id);
    if (text == NULL) {
        reply(fd, "ERROR no existe la ejecución %d", id);
        return;
    }

    reply(fd, "OK");
    reply_text(fd, text);
    free(text);
}

// LOGS <id> [trabajo] [FOLLOW]
static void cmd_logs(Server* srv, int fd, int argc, char** argv) {
    int id;

    (void)srv;
    if (argc < 2 || argc > 4 || parse_int(argv[1], &id) != 0) {
        reply(fd, "ERROR uso: LOGS <id> [trabajo] [FOLLOW]");
        return;
    }

    reply(fd, "ERROR LOGS todavía no está implementado");
}

static void cmd_cancel(Server* srv, int fd, int argc, char** argv) {
    int id;

    (void)srv;
    if (argc != 2 || parse_int(argv[1], &id) != 0) {
        reply(fd, "ERROR uso: CANCEL <id>");
        return;
    }

    // engine_cancel da false si el id no existe o si la ejecución ya terminó
    if (!engine_cancel(id)) {
        reply(fd, "ERROR la ejecución %d no existe o ya terminó", id);
        return;
    }

    reply(fd, "OK ejecución %d cancelada", id);
}

static void cmd_list(Server* srv, int fd, int argc) {
    (void)srv;
    if (argc != 1) {
        reply(fd, "ERROR uso: LIST");
        return;
    }

    char* text = engine_list();
    if (text == NULL) {
        reply(fd, "ERROR servidor sin memoria");
        return;
    }

    reply(fd, "OK");
    reply_text(fd, text);
    free(text);
}

// SHUTDOWN: el primero que llega hace el apagado, los demás esperan a que termine
static void server_shutdown(Server* srv) {
    pthread_mutex_lock(&srv->mutex);

    if (srv->stopping) {
        while (!srv->shutdown_done) {
            pthread_cond_wait(&srv->changed, &srv->mutex);
        }
        pthread_mutex_unlock(&srv->mutex);
        return;
    }
    srv->stopping = 1;

    // Despierta al hilo principal que está bloqueado en accept
    shutdown(srv->listen_fd, SHUT_RDWR);

    // Despierta a los manejadores que esperan una línea de un cliente que no escribe
    for (Conn* conn = srv->conns; conn != NULL; conn = conn->next) {
        shutdown(conn->fd, SHUT_RD);
    }
    pthread_mutex_unlock(&srv->mutex);

    // El engine cancela lo que está corriendo y espera a que terminen los agentes.
    // Se llama sin el mutex porque puede tardar
    engine_shutdown();

    pthread_mutex_lock(&srv->mutex);
    srv->shutdown_done = 1;
    pthread_cond_broadcast(&srv->changed);
    pthread_mutex_unlock(&srv->mutex);
}

static void process_line(Server* srv, int fd, char* line) {
    char* argv[MAX_TOKENS];
    char* save = NULL;
    int argc = 0;

    for (char* token = strtok_r(line, " \t", &save); token != NULL;
         token = strtok_r(NULL, " \t", &save)) {
        if (argc == MAX_TOKENS) {
            reply(fd, "ERROR demasiados argumentos");
            return;
        }
        argv[argc++] = token;
    }

    if (argc == 0) {
        reply(fd, "ERROR línea vacía");
    } else if (strcmp(argv[0], "SUBMIT") == 0) {
        cmd_submit(srv, fd, argc, argv);
    } else if (strcmp(argv[0], "STATUS") == 0) {
        cmd_status(srv, fd, argc, argv);
    } else if (strcmp(argv[0], "LOGS") == 0) {
        cmd_logs(srv, fd, argc, argv);
    } else if (strcmp(argv[0], "CANCEL") == 0) {
        cmd_cancel(srv, fd, argc, argv);
    } else if (strcmp(argv[0], "LIST") == 0) {
        cmd_list(srv, fd, argc);
    } else if (strcmp(argv[0], "SHUTDOWN") == 0) {
        if (argc != 1) {
            reply(fd, "ERROR uso: SHUTDOWN");
        } else {
            server_shutdown(srv);
            reply(fd, "OK apagando");
        }
    } else {
        reply(fd, "ERROR comando desconocido");
    }
}

//Atención de cada conexión 

// Saca la conexión de la lista
static void remove_conn(Server* srv, Conn* conn) {
    if (conn->prev != NULL) {
        conn->prev->next = conn->next;
    } else {
        srv->conns = conn->next;
    }
    if (conn->next != NULL) {
        conn->next->prev = conn->prev;
    }
}

// Cada conexión atiende una sola solicitud y se cierra
static void* handler_thread(void* arg) {
    Conn* conn = arg;
    Server* srv = conn->server;
    char line[LINE_MAX_LEN];

    ssize_t n = nu_read_line(conn->fd, line, sizeof(line));
    if (n == -2) {
        reply(conn->fd, "ERROR línea demasiado larga");
    } else if (n > 0) {
        process_line(srv, conn->fd, line);
    }

    // Se saca de la lista antes de cerrar, para que SHUTDOWN no use un fd ya cerrado
    pthread_mutex_lock(&srv->mutex);
    remove_conn(srv, conn);
    pthread_mutex_unlock(&srv->mutex);

    close(conn->fd);
    free(conn);

    pthread_mutex_lock(&srv->mutex);
    srv->active--;
    pthread_cond_broadcast(&srv->changed);
    pthread_mutex_unlock(&srv->mutex);

    return NULL;
}

// Registra la conexión y lanza su hilo. Nadie le hace join, al apagar el
// hilo principal espera a que active llegue a cero
static int spawn_handler(Server* srv, int fd) {
    Conn* conn = calloc(1, sizeof(*conn));
    if (conn == NULL) {
        return -1;
    }
    conn->fd = fd;
    conn->server = srv;

    // Un cliente que se conecta y no escribe no puede quedarse con el hilo
    struct timeval timeout = {RECV_TIMEOUT_SECONDS, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

    pthread_mutex_lock(&srv->mutex);
    conn->next = srv->conns;
    if (srv->conns != NULL) {
        srv->conns->prev = conn;
    }
    srv->conns = conn;
    srv->active++;
    if (srv->stopping) {
        shutdown(fd, SHUT_RD);
    }
    pthread_mutex_unlock(&srv->mutex);

    pthread_attr_t attr;
    pthread_t thread;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    int rc = pthread_create(&thread, &attr, handler_thread, conn);
    pthread_attr_destroy(&attr);

    if (rc != 0) {
        pthread_mutex_lock(&srv->mutex);
        remove_conn(srv, conn);
        srv->active--;
        pthread_cond_broadcast(&srv->changed);
        pthread_mutex_unlock(&srv->mutex);

        free(conn);
        return -1;
    }

    return 0;
}

// Programa principal 

int main(int argc, char* argv[]) {
    Server srv;
    int port;

    if (argc < 4 || argc > 5) {
        fprintf(stderr, "uso: %s <puerto> <agentes> <directorio> [hilos por tarea]\n", argv[0]);
        return EXIT_FAILURE;
    }

    memset(&srv, 0, sizeof(srv));
    srv.listen_fd = -1;
    srv.workdir = argv[3];
    srv.threads = 0;

    if (parse_int(argv[1], &port) != 0 || port == 0 || port > 65535
        || parse_int(argv[2], &srv.agents) != 0 || srv.agents == 0
        || (argc == 5 && (parse_int(argv[4], &srv.threads) != 0 || srv.threads == 0))) {
        fprintf(stderr, "puerto (1-65535), agentes e hilos deben ser enteros positivos\n");
        return EXIT_FAILURE;
    }

    // Si se escribe en un socket que el cliente ya cerró el servidor no se cae
    signal(SIGPIPE, SIG_IGN);

    if (pthread_mutex_init(&srv.mutex, NULL) != 0) {
        return EXIT_FAILURE;
    }
    if (pthread_cond_init(&srv.changed, NULL) != 0) {
        pthread_mutex_destroy(&srv.mutex);
        return EXIT_FAILURE;
    }

    srv.listen_fd = nu_listen((unsigned short)port, 64);
    if (srv.listen_fd < 0) {
        pthread_cond_destroy(&srv.changed);
        pthread_mutex_destroy(&srv.mutex);
        return EXIT_FAILURE;
    }

    // Con 0 hilos el engine reparte los núcleos entre los agentes
    engine_set_task_threads(srv.threads);
    if (engine_start(srv.agents, srv.workdir) != 0) {
        fprintf(stderr, "no se pudo arrancar el engine en '%s'\n", srv.workdir);
        close(srv.listen_fd);
        pthread_cond_destroy(&srv.changed);
        pthread_mutex_destroy(&srv.mutex);
        return EXIT_FAILURE;
    }

    printf("escuchando en el puerto %d (agentes: %d, hilos por tarea: %d, directorio: %s)\n",
           port, srv.agents, engine_task_threads(), srv.workdir);
    fflush(stdout);

    // Termina cuando SHUTDOWN cierra el socket de escucha
    while (1) {
        int fd = accept(srv.listen_fd, NULL, NULL);

        if (fd < 0) {
            pthread_mutex_lock(&srv.mutex);
            int stopping = srv.stopping;
            pthread_mutex_unlock(&srv.mutex);

            if (stopping) {
                break;
            }
            if (errno == EINTR || errno == ECONNABORTED) {
                continue;
            }
            if (errno == EMFILE || errno == ENFILE || errno == ENOBUFS || errno == ENOMEM) {
                // Sin descriptores libres se espera un poco y se vuelve a intentar
                struct timespec pause = {0, 10 * 1000 * 1000};
                perror("accept");
                nanosleep(&pause, NULL);
                continue;
            }
            perror("accept");
            break;
        }

        if (spawn_handler(&srv, fd) != 0) {
            reply(fd, "ERROR servidor sin recursos");
            close(fd);
        }
    }

    // Se espera a todos los manejadores antes de liberar lo que usan
    pthread_mutex_lock(&srv.mutex);
    while (srv.active > 0) {
        pthread_cond_wait(&srv.changed, &srv.mutex);
    }
    pthread_mutex_unlock(&srv.mutex);

    // Por si se salió del ciclo por un error de accept y no por SHUTDOWN
    engine_shutdown();

    close(srv.listen_fd);
    pthread_cond_destroy(&srv.changed);
    pthread_mutex_destroy(&srv.mutex);

    printf("servidor apagado\n");
    return EXIT_SUCCESS;
}
