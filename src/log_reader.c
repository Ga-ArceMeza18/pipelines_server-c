#define _POSIX_C_SOURCE 200809L

#include "log_reader.h"

#include "engine.h"
#include "net_util.h"
#include "run.h"

#include <fcntl.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define CHUNK_SIZE 8192
#define FOLLOW_PAUSE_MS 200

// Un archivo de log que se está siguiendo
typedef struct {
    char* path;
    char name[256];   // etapa/trabajo para mostrarlo al cliente
    off_t offset;     // hasta dónde ya se mandó
} LogFile;

// Saca "etapa/trabajo" del nombre del archivo, 000_Etapa__Trabajo.log
static void name_from_path(const char* path, char* out, size_t size) {
    const char* base = strrchr(path, '/');
    base = base != NULL ? base + 1 : path;

    const char* start = strchr(base, '_');
    start = start != NULL ? start + 1 : base;

    size_t len = 0;
    for (const char* p = start; *p != '\0' && strcmp(p, ".log") != 0 && len + 1 < size; p++) {
        if (p[0] == '_' && p[1] == '_') {
            out[len++] = '/';
            p++;
        } else {
            out[len++] = *p;
        }
    }
    out[len] = '\0';
}

// Revisa si el cliente cerró la conexión, sin quedarse esperando
static bool client_closed(int fd) {
    struct pollfd pfd = {fd, POLLIN, 0};
    char c;

    if (poll(&pfd, 1, 0) <= 0) {
        return false;
    }
    if (pfd.revents & (POLLHUP | POLLERR)) {
        return true;
    }
    return recv(fd, &c, 1, MSG_PEEK | MSG_DONTWAIT) == 0;
}

// Manda lo nuevo del archivo desde log->offset. Si complete es false solo manda líneas completas
// Lo que quede a medias se manda en la siguiente vuelta.
// Con prefix cada línea va con el nombre del trabajo adelante.
// Devuelve -1 si no se pudo escribir al cliente
static int send_new(int fd, LogFile* log, bool complete, bool prefix) {
    int file = open(log->path, O_RDONLY | O_CLOEXEC);
    if (file < 0) {
        return 0;   // todavía no existe, se intenta en la siguiente vuelta
    }

    char buffer[CHUNK_SIZE];
    int rc = 0;

    while (rc == 0) {
        ssize_t n = pread(file, buffer, sizeof(buffer), log->offset);
        if (n <= 0) {
            break;
        }

        // Se corta en el último salto de línea para no mandar líneas a medias.
        // Si el bloque está lleno y no tiene ninguno, se manda igual para no trabarse
        size_t len = (size_t)n;
        if (!complete) {
            char* last = NULL;
            for (size_t i = len; i > 0; i--) {
                if (buffer[i - 1] == '\n') {
                    last = &buffer[i - 1];
                    break;
                }
            }
            if (last != NULL) {
                len = (size_t)(last - buffer) + 1;
            } else if (len < sizeof(buffer)) {
                break;
            }
        }

        if (!prefix) {
            rc = nu_write_all(fd, buffer, len);
        } else {
            // Una línea a la vez, cada una con [etapa/trabajo] adelante
            size_t start = 0;
            while (rc == 0 && start < len) {
                char* nl = memchr(buffer + start, '\n', len - start);
                size_t end = nl != NULL ? (size_t)(nl - buffer) + 1 : len;
                char tag[300];
                int tag_len = snprintf(tag, sizeof(tag), "[%s] ", log->name);

                rc = nu_write_all(fd, tag, (size_t)tag_len);
                if (rc == 0) {
                    rc = nu_write_all(fd, buffer + start, end - start);
                }
                if (rc == 0 && nl == NULL) {
                    rc = nu_write_all(fd, "\n", 1);
                }
                start = end;
            }
        }

        log->offset += (off_t)len;
        if (len < (size_t)n) {
            break;
        }
    }

    close(file);
    return rc;
}

// Revisa si ya terminó lo que se está siguiendo ya sea el trabajo o toda la ejecución.
// Si la ejecución ya no existe también se da por terminada
static bool finished(int id, const char* job) {
    State state;
    int rc = job != NULL ? engine_job_state(id, job, &state) : engine_run_state(id, &state);

    return rc != 0 || state_is_terminal(state);
}

void send_logs(int fd, int id, const char* job, bool follow) {
    State state;
    char** paths = NULL;

    if (engine_run_state(id, &state) != 0) {
        char msg[64];
        int len = snprintf(msg, sizeof(msg), "ERROR no existe la ejecución %d\n", id);
        nu_write_all(fd, msg, (size_t)len);
        return;
    }

    int count = engine_log_paths(id, job, &paths);
    if (count <= 0) {
        char msg[300];
        int len = snprintf(msg, sizeof(msg), "ERROR no existe el trabajo %s en la ejecución %d\n",
                           job != NULL ? job : "", id);
        nu_write_all(fd, msg, (size_t)len);
        engine_free_paths(paths, count);
        return;
    }

    LogFile* logs = calloc((size_t)count, sizeof(*logs));
    if (logs == NULL) {
        nu_write_all(fd, "ERROR servidor sin memoria\n", 27);
        engine_free_paths(paths, count);
        return;
    }
    for (int i = 0; i < count; i++) {
        logs[i].path = paths[i];
        name_from_path(paths[i], logs[i].name, sizeof(logs[i].name));
    }

    nu_write_all(fd, "OK\n", 3);

    if (!follow) {
        // Todo lo que hay hasta ahora. Con varios trabajos, cada uno con su encabezado
        for (int i = 0; i < count; i++) {
            if (count > 1) {
                char header[300];
                int len = snprintf(header, sizeof(header), "== %s ==\n", logs[i].name);
                if (nu_write_all(fd, header, (size_t)len) != 0) {
                    break;
                }
            }
            if (send_new(fd, &logs[i], true, false) != 0) {
                break;
            }
        }
    } else {
        // Se manda lo nuevo de cada archivo cada FOLLOW_PAUSE_MS. Con toda la ejecución
        // cada línea lleva el nombre de su trabajo, porque llegan intercaladas
        bool prefix = count > 1;
        bool alive = true;

        while (alive && !finished(id, job)) {
            for (int i = 0; alive && i < count; i++) {
                alive = send_new(fd, &logs[i], false, prefix) == 0;
            }
            if (alive && client_closed(fd)) {
                alive = false;
            }

            struct timespec pause = {0, FOLLOW_PAUSE_MS * 1000000L};
            nanosleep(&pause, NULL);
        }

        // Una última vuelta para mandar lo que se escribió justo al final
        for (int i = 0; alive && i < count; i++) {
            alive = send_new(fd, &logs[i], true, prefix) == 0;
        }
    }

    free(logs);
    engine_free_paths(paths, count);
}
