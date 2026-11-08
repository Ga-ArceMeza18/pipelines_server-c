// test_net_util.c - Pruebas unitarias para las funciones de red

#define _POSIX_C_SOURCE 200809L

#include "net_util.h"

#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define BIG_SIZE (1024 * 1024)

typedef struct {
    const char* name;
    bool (*run)(void);
} NetTest;

// Lo que tiene que escribir el hilo escritor
typedef struct {
    int fd;
    const char* data;
    size_t size;
    int result;
} Writer;

// Escribe todo en otro hilo, para que el socket no se llene mientras el otro lado lee
static void* writer_thread(void* arg) {
    Writer* w = arg;

    w->result = nu_write_all(w->fd, w->data, w->size);
    shutdown(w->fd, SHUT_WR);

    return NULL;
}

// Crea un par de sockets conectados entre sí
static bool make_pair(int fds[2]) {
    return socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0;
}

// Devuelve el puerto que el sistema le asignó a un socket que escucha
static int bound_port(int fd) {
    struct sockaddr_in address;
    socklen_t len = sizeof(address);

    if (getsockname(fd, (struct sockaddr*)&address, &len) != 0) {
        return -1;
    }

    return ntohs(address.sin_port);
}

// Comprueba que nu_listen y nu_connect conectan un cliente con el servidor
static bool test_listen_connect(void) {
    // Con el puerto 0 el sistema elige uno libre
    int listen_fd = nu_listen(0, 4);

    if (listen_fd < 0) {
        return false;
    }

    int port = bound_port(listen_fd);
    int client = port > 0 ? nu_connect("localhost", (unsigned short)port) : -1;
    int server = client >= 0 ? accept(listen_fd, NULL, NULL) : -1;

    bool passed = false;

    // Se manda un mensaje corto para confirmar que la conexión sirve
    if (server >= 0 && nu_write_all(client, "hola\n", 5) == 0) {
        char line[16];
        passed = nu_read_line(server, line, sizeof(line)) == 4 && strcmp(line, "hola") == 0;
    }

    if (server >= 0) {
        close(server);
    }
    if (client >= 0) {
        close(client);
    }
    close(listen_fd);

    return passed;
}

// Comprueba que nu_connect falla si no hay nadie escuchando en el puerto
static bool test_connect_refused(void) {
    // Se abre y se cierra un puerto para tener uno que seguro está libre
    int listen_fd = nu_listen(0, 1);

    if (listen_fd < 0) {
        return false;
    }

    int port = bound_port(listen_fd);
    close(listen_fd);

    int fd = nu_connect("localhost", (unsigned short)port);

    if (fd >= 0) {
        close(fd);
        return false;
    }

    return true;
}

// Comprueba que nu_write_all y nu_read_exact pasan 1 MB completo y sin cambios
static bool test_write_read_big(void) {
    int fds[2];

    if (!make_pair(fds)) {
        return false;
    }

    char* data = malloc(BIG_SIZE);
    char* copy = malloc(BIG_SIZE);

    if (data == NULL || copy == NULL) {
        free(data);
        free(copy);
        close(fds[0]);
        close(fds[1]);
        return false;
    }

    for (size_t i = 0; i < BIG_SIZE; i++) {
        data[i] = (char)(i * 31 + 7);
    }

    Writer w = {fds[0], data, BIG_SIZE, -1};
    pthread_t thread;
    pthread_create(&thread, NULL, writer_thread, &w);

    int result = nu_read_exact(fds[1], copy, BIG_SIZE);

    pthread_join(thread, NULL);

    bool passed = w.result == 0 && result == 0 && memcmp(data, copy, BIG_SIZE) == 0;

    free(data);
    free(copy);
    close(fds[0]);
    close(fds[1]);

    return passed;
}

// Comprueba que nu_read_exact falla si el otro lado cierra antes de mandar todo
static bool test_read_exact_short(void) {
    int fds[2];

    if (!make_pair(fds)) {
        return false;
    }

    nu_write_all(fds[0], "abc", 3);
    close(fds[0]);

    char buffer[10];
    bool passed = nu_read_exact(fds[1], buffer, sizeof(buffer)) != 0;

    close(fds[1]);

    return passed;
}

// Comprueba que nu_read_line separa las líneas, quita \r\n y detecta el cierre
static bool test_read_line(void) {
    int fds[2];

    if (!make_pair(fds)) {
        return false;
    }

    const char* text = "STATUS 3\nLIST\r\n\nfinal";
    nu_write_all(fds[0], text, strlen(text));
    close(fds[0]);

    char line[32];
    bool passed = nu_read_line(fds[1], line, sizeof(line)) == 8 && strcmp(line, "STATUS 3") == 0
        && nu_read_line(fds[1], line, sizeof(line)) == 4 && strcmp(line, "LIST") == 0
        && nu_read_line(fds[1], line, sizeof(line)) == 0 && strcmp(line, "") == 0
        && nu_read_line(fds[1], line, sizeof(line)) == 5 && strcmp(line, "final") == 0
        && nu_read_line(fds[1], line, sizeof(line)) == 0;

    close(fds[1]);

    return passed;
}

// Comprueba que nu_read_line avisa con -2 cuando la línea no cabe
static bool test_read_line_too_long(void) {
    int fds[2];

    if (!make_pair(fds)) {
        return false;
    }

    const char* text = "esta linea es muy larga\n";
    nu_write_all(fds[0], text, strlen(text));
    close(fds[0]);

    char line[8];
    bool passed = nu_read_line(fds[1], line, sizeof(line)) == -2;

    close(fds[1]);

    return passed;
}

// Comprueba que nu_read_line no se come lo que viene después de la línea.
static bool test_line_then_body(void) {
    int fds[2];

    if (!make_pair(fds)) {
        return false;
    }

    const char* request = "SUBMIT 12\nname: prueba";
    nu_write_all(fds[0], request, strlen(request));
    close(fds[0]);

    char line[32];
    char body[13] = {0};

    bool passed = nu_read_line(fds[1], line, sizeof(line)) == 9 && strcmp(line, "SUBMIT 12") == 0
        && nu_read_exact(fds[1], body, 12) == 0 && strcmp(body, "name: prueba") == 0;

    close(fds[1]);

    return passed;
}

// Comprueba que nu_write_all da error y no mata al programa si el otro lado cerró
static bool test_write_closed(void) {
    int fds[2];

    if (!make_pair(fds)) {
        return false;
    }

    close(fds[1]);

    bool passed = nu_write_all(fds[0], "hola", 4) != 0;

    close(fds[0]);

    return passed;
}

int main(void) {
    NetTest tests[] = {
        {"nu_listen y nu_connect conectan cliente y servidor", test_listen_connect},
        {"nu_connect falla si no hay servidor", test_connect_refused},
        {"nu_write_all y nu_read_exact pasan 1 MB completo", test_write_read_big},
        {"nu_read_exact falla si se cierra antes de tiempo", test_read_exact_short},
        {"nu_read_line separa líneas y detecta el cierre", test_read_line},
        {"nu_read_line avisa si la línea no cabe", test_read_line_too_long},
        {"nu_read_line deja intactos los bytes que siguen", test_line_then_body},
        {"nu_write_all da error si el otro lado cerró", test_write_closed}
    };

    int total = sizeof(tests) / sizeof(tests[0]);
    int passed = 0;

    // Igual que en el servidor, escribir en un socket cerrado no debe matar al programa
    signal(SIGPIPE, SIG_IGN);

    // Sin buffer, para que los avisos de nu_connect salgan junto a su prueba
    setvbuf(stdout, NULL, _IONBF, 0);

    printf("================================\n");
    printf("    PRUEBAS UNITARIAS NET UTIL\n");
    printf("================================\n\n");

    for (int i = 0; i < total; i++) {
        if (tests[i].run()) {
            printf("[OK] %s\n", tests[i].name);
            passed++;
        } else {
            printf("[FAIL] %s\n", tests[i].name);
        }
    }

    printf("\n================================\n");
    printf("Pruebas de NetUtil: %d/%d aprobadas\n", passed, total);

    if (passed == total) {
        printf("Todas las pruebas pasaron!\n");
    } else {
        printf("Hay pruebas que fallaron.\n");
    }

    printf("================================\n");

    return passed == total ? EXIT_SUCCESS : EXIT_FAILURE;
}
