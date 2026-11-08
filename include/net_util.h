// net_util.h - Funciones de red que usan pipelined y pipectl

#ifndef NET_UTIL_H
#define NET_UTIL_H

#include <stddef.h>
#include <sys/types.h>

// Abre un socket TCP escuchando en el puerto. Devuelve el descriptor o -1
int nu_listen(unsigned short port, int backlog);

// Se conecta a host:puerto. Devuelve el descriptor o -1
int nu_connect(const char* host, unsigned short port);

// Escribe todo el buffer aunque write lo mande por partes. 0 si salió bien, -1 si no
int nu_write_all(int fd, const void* buffer, size_t size);

// Lee exactamente size bytes. 0 si salió bien, -1 si hubo error o se cerró antes
int nu_read_exact(int fd, void* buffer, size_t size);

// Lee una línea sin el salto. Devuelve su largo, 0 si se cerró sin datos,
// -1 si hubo error y -2 si no cabe en el buffer
ssize_t nu_read_line(int fd, char* buffer, size_t size);

#endif
