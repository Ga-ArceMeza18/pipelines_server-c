// test_parser.c - Prueba unitaria para el analizador de pipelines

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>

#include "parser.h"
#include "pipeline.h"
#include "errors.h"

#define MAX_YAML_SIZE 1048576 // Establacer un tamaño máximo de 1 MB para el contenido YAML solo para fines de prueba

typedef struct {
    const char* filename;
    bool expected_valid;
} ParserTest;

static char* read_yaml(const char* filename, size_t* len) {
    FILE* file = fopen(filename, "rb");

    if (file == NULL) {
        return NULL;
    }

    char* yaml = malloc(MAX_YAML_SIZE);

    if (yaml == NULL) {
        fclose(file);
        return NULL;
    }

    *len = fread(yaml, 1, MAX_YAML_SIZE - 1, file);
    yaml[*len] = '\0';

    fclose(file);

    return yaml;
}

static bool run_test(const ParserTest* test) {
    size_t len = 0;
    char* yaml = read_yaml(test->filename, &len);

    if (yaml == NULL) {
        printf("[FAIL] %s - No se pudo abrir el archivo\n", test->filename);
        return false;
    }

    ErrorList errors;
    errors_init(&errors);

    Pipeline* pipeline = parse_pipeline(yaml, len, &errors);

    bool valid = pipeline != NULL;
    bool passed = (valid == test->expected_valid);

    if (passed) {
        printf("[OK] %s\n", test->filename);
    } else {
        printf("[FAIL] %s\n", test->filename);
        printf("       Esperado: %s\n", test->expected_valid ? "válido" : "inválido");
        printf("       Obtenido: %s\n", valid ? "válido" : "inválido");
    }

    if (errors.size > 0) {
        for (int i = 0; i < errors.size; i++) {
            printf("       Línea %d: %s\n", errors.items[i].line, errors.items[i].message);
        }
    }

    pipeline_free(pipeline);
    errors_destroy(&errors);
    free(yaml);

    return passed;
}

int main(void) {
    ParserTest tests[] = {
        {"pipeline/valid_basic.yaml", true},
        {"pipeline/valid_dependencies.yaml", true},
        {"pipeline/valid_conditions.yaml", true},
        {"pipeline/invalid_syntax.yaml", false},
        {"pipeline/invalid_key.yaml", false},
        {"pipeline/invalid_dependency.yaml", false},
        {"pipeline/invalid_cycle.yaml", false},
        {"pipeline/invalid_duplicate.yaml", false}
    };

    int total = sizeof(tests) / sizeof(tests[0]);
    int passed = 0;

    printf("================================\n");
    printf("       PRUEBAS UNITARIAS DEL PARSER\n");
    printf("================================\n\n");

    for (int i = 0; i < total; i++) {
        if (run_test(&tests[i])) {
            passed++;
        }
    }

    printf("\n================================\n");
    printf("Pruebas del parser: %d/%d aprobadas\n", passed, total);

    if (passed == total) {
        printf("Todas las pruebas pasaron!\n");
    } else {
        printf("Algunas pruebas fallaron.\n");
    }

    printf("================================\n");

    return (passed == total) ? EXIT_SUCCESS : EXIT_FAILURE;
}