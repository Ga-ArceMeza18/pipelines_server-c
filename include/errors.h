// errors.h - Estructura de datos para manejar errores

#ifndef ERRORS_H
#define ERRORS_H

typedef struct {
    int line;
    char* message;
} Error;

typedef struct {
    Error* items;
    int size;
    int capacity;
} ErrorList;

// Crear una nueva lista de errores
void errors_init(ErrorList* errors);

// Agregar un error a la lista
void errors_add(ErrorList* errors, int line, const char* message);

// Liberar la memoria utilizada por la lista de errores
void errors_destroy(ErrorList* errors);

#endif
