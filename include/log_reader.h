// log_reader.h - Envía las bitácoras de una ejecución al cliente (LOGS)

#ifndef LOG_READER_H
#define LOG_READER_H

#include <stdbool.h>

// Manda por fd la bitácora de un trabajo o de toda la ejecución si job es NULL. Responde OK o ERROR.
// Con follow sigue mandando las lineas nuevas hasta que el trabajo o la ejecución termine

void send_logs(int fd, int id, const char* job, bool follow);

#endif
