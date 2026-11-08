#define _POSIX_C_SOURCE 200809L

#include "net_util.h"

#include <ctype.h>
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define DEFAULT_HOST "localhost"
#define DEFAULT_PORT 5000
#define LINE_MAX_LEN 4096

static void usage(const char* program) {
    fprintf(stderr,
            "uso: %s [-h host] [-p puerto] <comando>\n"
            "comandos:\n"
            "  submit <archivo.yaml>\n"
            "  status <id>\n"
            "  logs <id> [trabajo] [--follow]\n"
            "  cancel <id>\n"
            "  list\n"
            "  shutdown\n",
            program);
}

// Revisa que el texto sea un número, para no mandarle basura al servidor
static int is_number(const char* text) {
    if (text == NULL || *text == '\0') {
        return 0;
    }
    for (const char* p = text; *p != '\0'; p++) {
        if (!isdigit((unsigned char)*p)) {
            return 0;
        }
    }
    return 1;
}

// Lee un archivo completo en memoria. Quien llama libera el resultado
static char* read_file(const char* path, size_t* len) {
    FILE* file = fopen(path, "rb");
    if (file == NULL) {
        return NULL;
    }

    if (fseek(file, 0, SEEK_END) != 0) {
        fclose(file);
        return NULL;
    }
    long size = ftell(file);
    rewind(file);
    if (size < 0) {
        fclose(file);
        return NULL;
    }

    char* data = malloc((size_t)size + 1);
    if (data == NULL) {
        fclose(file);
        return NULL;
    }

    *len = fread(data, 1, (size_t)size, file);
    data[*len] = '\0';
    fclose(file);

    return data;
}

// Arma la línea del protocolo según el comando. 0 si salió bien, -1 si el uso
// está mal y -2 si no se pudo leer el archivo de submit
static int build_request(int argc, char** argv, char* line, size_t size, char** body, size_t* body_len) {

    const char* cmd = argv[0];

    *body = NULL;
    *body_len = 0;

    if (strcmp(cmd, "submit") == 0 && argc == 2) {
        *body = read_file(argv[1], body_len);
        if (*body == NULL) {
            fprintf(stderr, "no se pudo leer %s: %s\n", argv[1], strerror(errno));
            return -2;
        }
        snprintf(line, size, "SUBMIT %zu\n", *body_len);
    } 
    else if (strcmp(cmd, "status") == 0 && argc == 2 && is_number(argv[1])) {
        snprintf(line, size, "STATUS %s\n", argv[1]);
    } 
    else if (strcmp(cmd, "cancel") == 0 && argc == 2 && is_number(argv[1])) {
        snprintf(line, size, "CANCEL %s\n", argv[1]);
    } 
    else if (strcmp(cmd, "list") == 0 && argc == 1) {
        snprintf(line, size, "LIST\n");
    } 
    else if (strcmp(cmd, "shutdown") == 0 && argc == 1) {
        snprintf(line, size, "SHUTDOWN\n");
    } 
    else if (strcmp(cmd, "logs") == 0 && argc >= 2 && argc <= 4 && is_number(argv[1])) {
        const char* job = NULL;
        int follow = 0;

        // El trabajo y --follow pueden venir en cualquier orden
        for (int i = 2; i < argc; i++) {
            if (strcmp(argv[i], "--follow") == 0 && !follow) {
                follow = 1;
            } else if (job == NULL && argv[i][0] != '-') {
                job = argv[i];
            } else {
                return -1;
            }
        }

        snprintf(line, size, "LOGS %s%s%s%s\n", argv[1],
                 job ? " " : "", job ? job : "", follow ? " FOLLOW" : "");
    } else {
        return -1;
    }

    return 0;
}

// Copia a la salida todo lo que mande el servidor hasta que cierre.
// Se imprime apenas llega para que logs --follow se vea en vivo
static void copy_rest(int fd, FILE* out) {
    char buffer[4096];

    while (1) {
        ssize_t n = read(fd, buffer, sizeof(buffer));

        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n <= 0) {
            break;
        }
        fwrite(buffer, 1, (size_t)n, out);
        fflush(out);
    }
}

int main(int argc, char* argv[]) {
    const char* host = DEFAULT_HOST;
    int port = DEFAULT_PORT;
    int opt;

    while ((opt = getopt(argc, argv, "+h:p:")) != -1) {
        if (opt == 'h') {
            host = optarg;
        } else if (opt == 'p') {
            port = atoi(optarg);
            if (port <= 0 || port > 65535) {
                fprintf(stderr, "el puerto debe estar entre 1 y 65535\n");
                return EXIT_FAILURE;
            }
        } else {
            usage(argv[0]);
            return EXIT_FAILURE;
        }
    }

    if (optind >= argc) {
        usage(argv[0]);
        return EXIT_FAILURE;
    }

    char line[LINE_MAX_LEN];
    char* body;
    size_t body_len;
    int rc = build_request(argc - optind, argv + optind, line, sizeof(line), &body, &body_len);
    if (rc != 0) {
        if (rc == -1) {
            usage(argv[0]);
        }
        return EXIT_FAILURE;
    }

    // Si el servidor se cae a la mitad, write da error en vez de matar al cliente
    signal(SIGPIPE, SIG_IGN);

    int fd = nu_connect(host, (unsigned short)port);
    if (fd < 0) {
        free(body);
        return EXIT_FAILURE;
    }

    if (nu_write_all(fd, line, strlen(line)) != 0
        || (body != NULL && nu_write_all(fd, body, body_len) != 0)) {
        fprintf(stderr, "no se pudo enviar la solicitud\n");
        free(body);
        close(fd);
        return EXIT_FAILURE;
    }
    free(body);

    // La primera línea dice si salió bien (OK) o no (ERROR)
    char first[LINE_MAX_LEN];
    ssize_t n = nu_read_line(fd, first, sizeof(first));
    if (n <= 0) {
        fprintf(stderr, "el servidor cerró la conexión sin responder\n");
        close(fd);
        return EXIT_FAILURE;
    }

    int status = EXIT_SUCCESS;

    if (strncmp(first, "OK", 2) == 0) {
        const char* rest = first[2] == ' ' ? first + 3 : first + 2;

        if (strcmp(argv[optind], "submit") == 0) {
            printf("pipeline aceptado, id de ejecución: %s\n", rest);
        } else if (*rest != '\0') {
            printf("%s\n", rest);
        }
        fflush(stdout);
        copy_rest(fd, stdout);
    } else {
        const char* rest = strncmp(first, "ERROR ", 6) == 0 ? first + 6 : first;

        fprintf(stderr, "error: %s\n", rest);
        copy_rest(fd, stderr);
        status = EXIT_FAILURE;
    }

    close(fd);
    return status;
}
