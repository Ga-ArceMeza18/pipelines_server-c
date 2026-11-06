// Declaración de la estructura de datos para almacenar variables y sus valores
#ifndef VARLIST_H
#define VARLIST_H

#include <stdbool.h>

// Par nombre-valor
typedef struct {
    char* name; // Nombre de la variable
    char* value; // Valor de la variable
} Var;

// Lista de variables
typedef struct {
    Var* items; // Arreglo dinámico de variables
    int size; // Número de variables almacenadas
    int capacity; // Capacidad del arreglo dinámico
} VarList;

// Inicialización y liberación
void varlist_init(VarList* list);
void varlist_destroy(VarList* list);

// Operaciones de variables

// Agrega o actualiza una variable en la lista
bool var_set(VarList* list, const char* key, const char* value);

// Obtiene el valor de una variable por su nombre
char* var_get(const VarList* list, const char* key);

// Sustituir referencias por nombre de variable en un texto.
char* substitute(const char* text, const VarList* list);

#endif
