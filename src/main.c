#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "engine.h"
#include "errors.h"
#include "parser.h"
#include "run.h"

// comm: leer un archivo completo en memoria (quien llama libera el resultado)
static char *leer_archivo(const char *ruta, size_t *len) {
    FILE *f = fopen(ruta, "rb");

    if (f == NULL) {
        return NULL;
    }

    // comm: medir el archivo
    fseek(f, 0, SEEK_END);
    long size = ftell(f);
    rewind(f);

    if (size < 0) {
        fclose(f);
        return NULL;
    }

    // comm: reservar memoria y leer todo el contenido
    char *data = malloc((size_t)size + 1);

    if (data == NULL) {
        fclose(f);
        return NULL;
    }

    size_t leidos = fread(data, 1, (size_t)size, f);
    fclose(f);

    data[leidos] = '\0';
    *len = leidos;

    return data;
}

int main(int argc, char *argv[]) {
    // comm: validar los argumentos de la línea de comandos
    if (argc < 2) {
        fprintf(stderr, "uso: %s pipeline.yaml [agentes] [carpeta_de_trabajo]\n", argv[0]);
        return 1;
    }

    const char *ruta = argv[1];
    int agentes = (argc > 2) ? atoi(argv[2]) : 2;
    const char *workdir = (argc > 3) ? argv[3] : "run/ejecuciones";

    if (agentes < 1) {
        fprintf(stderr, "error: la cantidad de agentes debe ser >= 1\n");
        return 1;
    }

    // comm: leer el archivo YAML
    size_t len = 0;
    char *yaml = leer_archivo(ruta, &len);

    if (yaml == NULL) {
        fprintf(stderr, "error: no se pudo leer %s\n", ruta);
        return 1;
    }

    // comm: parsear y validar el pipeline, mostrando los errores si falla
    ErrorList errores;
    errors_init(&errores);

    Pipeline *pipeline = parse_pipeline(yaml, len, &errores);

    free(yaml);

    if (pipeline == NULL) {
        fprintf(stderr, "error: el pipeline no es válido\n");

        for (int i = 0; i < errores.size; i++) {
            fprintf(stderr, "  línea %d: %s\n", errores.items[i].line, errores.items[i].message);
        }

        errors_destroy(&errores);
        return 1;
    }

    errors_destroy(&errores);

    // comm: arrancar el motor con la cantidad de agentes pedida
    if (engine_start(agentes, workdir) != 0) {
        fprintf(stderr, "error: no se pudo arrancar el motor\n");
        pipeline_free(pipeline);
        return 1;
    }

    // comm: enviar el pipeline al motor (el motor toma posesión de él)
    int id = engine_submit(pipeline);

    if (id < 0) {
        fprintf(stderr, "error: no se pudo enviar el pipeline\n");
        engine_shutdown();
        return 1;
    }

    printf("Ejecución %d enviada (%d agente(s)), carpeta: %s\n", id, agentes, workdir);

    // comm: esperar a que la ejecución llegue a un estado final
    State estado = STATE_PENDING;

    do {
        struct timespec pausa = {0, 100000000L};  // 100 ms

        nanosleep(&pausa, NULL);

        if (engine_run_state(id, &estado) != 0) {
            break;
        }
    } while (!state_is_terminal(estado));

    // comm: mostrar el estado final de la ejecución
    char *status = engine_status(id);

    if (status != NULL) {
        printf("\n%s", status);
        free(status);
    }

    // comm: apagar el motor y devolver 0 solo si la ejecución terminó bien
    engine_shutdown();

    return (estado == STATE_SUCCESS || estado == STATE_PARTIALLY_SUCCESS) ? 0 : 1;
}
