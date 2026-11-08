// errors.c - Implementación para manejar errores

#include "errors.h"

#include <stdlib.h>
#include <string.h>

static char* duplicate_string(const char* text) {
    if (text == NULL) {
        return NULL;
    }

    size_t length = strlen(text);
    char* copy = malloc(length + 1);

    if (copy == NULL) {
        return NULL;
    }

    memcpy(copy, text, length + 1);
    return copy;
}

void errors_init(ErrorList* errors) {
    if (errors == NULL) {
        return;
    }

    errors->items = NULL;
    errors->size = 0;
    errors->capacity = 0;
}

void errors_add(ErrorList* errors, int line, const char* message) {
    if (errors == NULL || message == NULL) {
        return;
    }

    if (errors->size == errors->capacity) {
        int new_capacity = errors->capacity == 0 ? 4 : errors->capacity * 2;

        Error* new_items = realloc(errors->items, sizeof(Error) * new_capacity);

        if (new_items == NULL) {
            return;
        }

        errors->items = new_items;
        errors->capacity = new_capacity;
    }

    char* message_copy = duplicate_string(message);

    if (message_copy == NULL) {
        return;
    }

    errors->items[errors->size].line = line;
    errors->items[errors->size].message = message_copy;
    errors->size++;
}

void errors_destroy(ErrorList* errors) {
    if (errors == NULL) {
        return;
    }

    for (int i = 0; i < errors->size; i++) {
        free(errors->items[i].message);
    }

    free(errors->items);

    errors->items = NULL;
    errors->size = 0;
    errors->capacity = 0;
}
