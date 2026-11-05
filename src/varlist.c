// Implementacuión de la estructura de datos para almacenar variables y sus valores

#include "varlist.h"

#include <stdlib.h>
#include <limits.h>
#include <string.h>
#include <stdio.h>

// Funciones auxiliares

// Duplica una cadena reservando memoria. 
static char* duplicate_string(const char* text) {
    if (text == NULL) {
        return NULL;
    }

    char* copy = malloc(strlen(text) + 1);

    if (copy == NULL) {
        return NULL;
    }

    strcpy(copy, text);

    return copy;
}

// Aumenta la capacidad del arreglo cuando sea necesario. 
static bool varlist_grow(VarList* list) {
    int new_capacity;

    if (list->capacity == 0) {
        new_capacity = 4;
    } else {
        new_capacity = list->capacity * 2;
    }

    Var* new_items = realloc(
        list->items,
        new_capacity * sizeof(Var)
    );

    if (new_items == NULL) {
        return false;
    }

    list->items = new_items;
    list->capacity = new_capacity;

    return true;
}

// Inicialización y liberación de la lista de variables

void varlist_init(VarList* list) {
    list->items = NULL;
    list->size = 0;
    list->capacity = 0;
}

void varlist_destroy(VarList* list) {
    for (int i = 0; i < list->size; i++) {
        free(list->items[i].name);
        free(list->items[i].value);
    }

    free(list->items);

    list->items = NULL;
    list->size = 0;
    list->capacity = 0;
}

// Operaciones de variables

bool var_set(VarList* list, const char* key, const char* value) {
    if (list == NULL || key == NULL || value == NULL) {
        return false;
    }

    // Buscar si la variable ya existe. 
    for (int i = 0; i < list->size; i++) {
        if (strcmp(list->items[i].name, key) == 0) {

            char* new_value = duplicate_string(value);

            if (new_value == NULL) {
                return false;
            }

            free(list->items[i].value);
            list->items[i].value = new_value;

            return true;
        }
    }

    // Reservar espacio si el arreglo está lleno. 
    if (list->size == list->capacity) {
        if (!varlist_grow(list)) {
            return false;
        }
    }

    // Crear una nueva variable. 
    char* new_name = duplicate_string(key);
    char* new_value = duplicate_string(value);

    if (new_name == NULL || new_value == NULL) {
        free(new_name);
        free(new_value);
        return false;
    }

    list->items[list->size].name = new_name;
    list->items[list->size].value = new_value;

    list->size++;

    return true;
}

char* var_get(const VarList* list, const char* key) {
    if (list == NULL || key == NULL) {
        return NULL;
    }

    for (int i = 0; i < list->size; i++) {
        if (strcmp(list->items[i].name, key) == 0) {
            return list->items[i].value;
        }
    }

    return NULL;
}

// Funciones auxiliares para substitute

// Agrega una cadena de longitud determinada al resultado. 
static bool append_text(char** result, int* length, int* capacity, 
    const char* text, int text_length) {
        
    if (text_length == 0) {
        return true;
    }

    if (*length > INT_MAX - text_length - 1) {
        return false;
    }

    int required = *length + text_length + 1;

    if (required > *capacity) {
        int new_capacity = *capacity;

        if (new_capacity == 0) {
            new_capacity = 32;
        }

        while (new_capacity < required) {
            if (new_capacity > INT_MAX / 2) {
                new_capacity = required;
                break;
            }

            new_capacity *= 2;
        }

        char* new_result = realloc(*result, new_capacity);

        if (new_result == NULL) {
            return false;
        }

        *result = new_result;
        *capacity = new_capacity;
    }

    memcpy(*result + *length, text, text_length);

    *length += text_length;
    (*result)[*length] = '\0';

    return true;
}


char* substitute(const char* text, const VarList* list) {
    if (text == NULL || list == NULL) {
        return NULL;
    }

    char* result = NULL;
    int length = 0;
    int capacity = 0;

    int i = 0;

    while (text[i] != '\0') {

        // Buscar el inicio de una variable: $( 
        if (text[i] == '$' && text[i + 1] == '(') {

            int start = i + 2;
            int end = start;

            // Buscar el cierre de la variable: ) 
            while (text[end] != '\0' && text[end] != ')') {
                end++;
            }

            if (text[end] == ')') {

                int key_length = end - start;

                char* key = malloc(key_length + 1);

                if (key == NULL) {
                    free(result);
                    return NULL;
                }

                memcpy(key, text + start, key_length);
                key[key_length] = '\0';

                char* value = var_get(list, key);

                free(key);

                if (value != NULL) {
                    // Reemplazar por el valor encontrado. 
                    if (!append_text(&result, &length, &capacity, value,
                        strlen(value))) {

                        free(result);
                        return NULL;
                    }
                } else {
                    // Conservar la referencia si no existe. 
                    if (!append_text(&result, &length, &capacity, text + i,
                        end - i + 1)) {

                        free(result);
                        return NULL;
                    }
                }

                i = end + 1;
                continue;
            }
        }

        // Copiar el carácter actual. 
        if (!append_text(&result, &length, &capacity, text + i, 1)) {
            free(result);
            return NULL;
        }
        i++;
    }

    // Devolver una cadena vacía si no hubo contenido. 
    if (result == NULL) {
        result = duplicate_string("");
    }

    return result;
}
