#define _POSIX_C_SOURCE 200809L

#include "net_util.h"

#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

int nu_listen(unsigned short port, int backlog) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);

    if (fd < 0) {
        perror("socket");
        return -1;
    }

    // Permite volver a usar el puerto apenas se apaga el servidor
    int yes = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0) {
        perror("setsockopt");
        close(fd);
        return -1;
    }

    struct sockaddr_in address;
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(port);

    if (bind(fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        perror("bind");
        close(fd);
        return -1;
    }

    if (listen(fd, backlog) < 0) {
        perror("listen");
        close(fd);
        return -1;
    }

    return fd;
}

int nu_connect(const char* host, unsigned short port) {
    struct addrinfo hints;
    struct addrinfo* result = NULL;
    char port_text[8];

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    snprintf(port_text, sizeof(port_text), "%u", port);

    int rc = getaddrinfo(host, port_text, &hints, &result);
    if (rc != 0) {
        fprintf(stderr, "no se encontró el host %s: %s\n", host, gai_strerror(rc));
        return -1;
    }

    // Prueba cada dirección del host hasta que una conecte
    int fd = -1;
    for (struct addrinfo* it = result; it != NULL; it = it->ai_next) {
        fd = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
        if (fd < 0) {
            continue;
        }
        if (connect(fd, it->ai_addr, it->ai_addrlen) == 0) {
            break;
        }
        close(fd);
        fd = -1;
    }

    freeaddrinfo(result);

    if (fd < 0) {
        fprintf(stderr, "no se pudo conectar a %s:%u\n", host, port);
    }
    return fd;
}

int nu_write_all(int fd, const void* buffer, size_t size) {
    const char* p = buffer;
    size_t sent = 0;

    while (sent < size) {
        ssize_t n = write(fd, p + sent, size - sent);

        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        sent += (size_t)n;
    }

    return 0;
}

int nu_read_exact(int fd, void* buffer, size_t size) {
    char* p = buffer;
    size_t got = 0;

    while (got < size) {
        ssize_t n = read(fd, p + got, size - got);

        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (n == 0) {
            return -1;
        }
        got += (size_t)n;
    }

    return 0;
}

ssize_t nu_read_line(int fd, char* buffer, size_t size) {
    size_t len = 0;

    if (buffer == NULL || size < 2) {
        return -1;
    }

    // Se lee un byte para no consumir lo que viene después de la línea
    while (1) {
        char c;
        ssize_t n = read(fd, &c, 1);

        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (n == 0 || c == '\n') {
            break;
        }
        if (len + 1 >= size) {
            return -2;
        }
        buffer[len++] = c;
    }

    if (len > 0 && buffer[len - 1] == '\r') {
        len--;
    }
    buffer[len] = '\0';

    return (ssize_t)len;
}
