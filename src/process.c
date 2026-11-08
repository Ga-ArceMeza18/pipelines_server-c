#define _GNU_SOURCE  // posix_spawn_file_actions_addchdir_np, environ, getline
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <spawn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include "process.h"

#define POLL_NS     10000000L  // 10 ms entre cada revisión del hijo
#define GRACE_TICKS 200        // 200 * 10 ms = 2 s de gracia entre SIGTERM y SIGKILL

// =============================================================================
// Funciones privadas

// comm: pausar 10 ms entre revisiones (si una señal interrumpe, se vuelve a revisar)
static void dormir_poll(void) {
    struct timespec ts = {0, POLL_NS};
    nanosleep(&ts, NULL);
}

// comm: matar todo el grupo de procesos (script, hijos y nietos) y cosechar al líder
// primero se pide terminar con SIGTERM, se dan 2 s de gracia y luego se usa SIGKILL
// waitid con WNOWAIT permite mirar si terminó sin cosecharlo, así el pid
// no se puede reutilizar antes de enviar SIGKILL al grupo
static void matar_grupo(pid_t pgid) {
    kill(-pgid, SIGTERM);

    // comm: esperar hasta 2 s a que el líder termine por su cuenta
    for (int i = 0; i < GRACE_TICKS; i++) {
        siginfo_t si;
        si.si_pid = 0;

        if (waitid(P_PID, (id_t)pgid, &si, WEXITED | WNOHANG | WNOWAIT) == 0
            && si.si_pid == pgid) {
            break;
        }

        dormir_poll();
    }

    // comm: eliminar a cualquier sobreviviente y cosechar al líder para evitar un zombi
    kill(-pgid, SIGKILL);

    int st;
    while (waitpid(pgid, &st, 0) < 0 && errno == EINTR) {}
}

// comm: validar que un nombre de variable tenga solo letras, dígitos y '_'
static bool nombre_valido(const char *n) {
    if (n == NULL || n[0] == '\0') {
        return false;
    }

    for (const char *c = n; *c; c++) {
        unsigned char u = (unsigned char)*c;

        if (!isalnum(u) && *c != '_') {
            return false;
        }
    }

    return true;
}

// comm: leer la bitácora desde 'desde' y guardar cada línea "##setvariable NOMBRE=valor"
static void leer_setvariable(const char *log, off_t desde, VarList *vars) {
    FILE *f = fopen(log, "r");

    if (f == NULL) {
        return;
    }

    // comm: saltar lo que escribieron los pasos anteriores
    if (fseeko(f, desde, SEEK_SET) != 0) {
        fclose(f);
        return;
    }

    char *line = NULL;
    size_t cap = 0;
    ssize_t n;

    // comm: recorrer la bitácora línea por línea sin límite de largo
    while ((n = getline(&line, &cap, f)) >= 0) {

        // comm: quitar el fin de línea
        while (n > 0 && (line[n - 1] == '\n' || line[n - 1] == '\r')) {
            line[--n] = '\0';
        }

        // comm: ignorar las líneas que no tengan el formato esperado
        if (strncmp(line, "##setvariable ", 14) != 0) {
            continue;
        }

        char *eq = strchr(line + 14, '=');

        if (eq == NULL) {
            continue;
        }

        // comm: separar el nombre del valor y descartar nombres inválidos
        *eq = '\0';
        const char *nombre = line + 14;
        const char *valor = eq + 1;

        if (!nombre_valido(nombre)) {
            continue;
        }

        // si el nombre se repite en el mismo paso, gana la última línea
        var_set(vars, nombre, valor);
    }

    free(line);
    fclose(f);
}

// =============================================================================
// Función pública

int run_process(char *const argv[], const char *log, Run *run, double timeout_s,
                const char *cwd, VarList *newvars) {

    // comm: validar los argumentos recibidos
    if (argv == NULL || argv[0] == NULL || log == NULL || run == NULL) {
        return PROC_ERROR;
    }

    // comm: recordar dónde empieza este paso en la bitácora
    // los ##setvariable de pasos anteriores no deben contarse
    struct stat sb;
    off_t desde = (stat(log, &sb) == 0) ? sb.st_size : 0;

    // comm: inicializar las acciones sobre descriptores y los atributos del hijo
    posix_spawn_file_actions_t fa;
    posix_spawnattr_t at;

    if (posix_spawn_file_actions_init(&fa) != 0) {
        return PROC_ERROR;
    }

    if (posix_spawnattr_init(&at) != 0) {
        posix_spawn_file_actions_destroy(&fa);
        return PROC_ERROR;
    }

    // comm: configurar los descriptores del hijo
    // stdin viene de /dev/null, stdout y stderr van a la bitácora en modo append
    int rc = 0;
    rc |= posix_spawn_file_actions_addopen(&fa, 0, "/dev/null", O_RDONLY, 0);
    rc |= posix_spawn_file_actions_addopen(&fa, 1, log, O_WRONLY | O_CREAT | O_APPEND, 0644);
    rc |= posix_spawn_file_actions_adddup2(&fa, 1, 2);

    // comm: arrancar el hijo dentro del espacio de trabajo
    if (cwd != NULL) {
        rc |= posix_spawn_file_actions_addchdir_np(&fa, cwd);
    }

    // comm: cerrar los descriptores heredados del servidor (solo en glibc >= 2.34)
#if defined(__GLIBC__) && (__GLIBC__ > 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ >= 34))
    rc |= posix_spawn_file_actions_addclosefrom_np(&fa, 3);
#endif

    // comm: configurar las señales y el grupo de procesos del hijo
    // el hijo no hereda señales bloqueadas y es líder de un grupo nuevo
    sigset_t vacio, todas;
    sigemptyset(&vacio);
    sigfillset(&todas);

    rc |= posix_spawnattr_setsigmask(&at, &vacio);
    rc |= posix_spawnattr_setsigdefault(&at, &todas);
    rc |= posix_spawnattr_setpgroup(&at, 0);
    rc |= posix_spawnattr_setflags(&at, POSIX_SPAWN_SETPGROUP | POSIX_SPAWN_SETSIGMASK
                                        | POSIX_SPAWN_SETSIGDEF);

    if (rc != 0) {
        posix_spawn_file_actions_destroy(&fa);
        posix_spawnattr_destroy(&at);
        return PROC_ERROR;
    }

    // comm: lanzar el proceso (posix_spawn es seguro con varios hilos, a diferencia de fork)
    pid_t pid;
    int err = posix_spawnp(&pid, argv[0], &fa, &at, argv, environ);

    posix_spawn_file_actions_destroy(&fa);
    posix_spawnattr_destroy(&at);

    if (err != 0) {
        return PROC_ERROR;
    }

    // comm: esperar al hijo revisando cada 10 ms si terminó, se canceló o venció el tiempo
    double t0 = now_s();
    int result = PROC_ERROR;
    int status = 0;

    for (;;) {
        siginfo_t si;
        si.si_pid = 0;

        // comm: mirar si el hijo terminó sin cosecharlo todavía
        int w = waitid(P_PID, (id_t)pid, &si, WEXITED | WNOHANG | WNOWAIT);

        if (w < 0 && errno != EINTR) {
            result = PROC_ERROR;
            break;
        }

        // comm: cosechar al hijo y traducir su estado de salida
        // antes se eliminan los procesos que el script dejó en segundo plano
        if (w == 0 && si.si_pid == pid) {
            kill(-pid, SIGKILL);

            while (waitpid(pid, &status, 0) < 0 && errno == EINTR) {}

            if (WIFEXITED(status)) {
                result = WEXITSTATUS(status);
            } else if (WIFSIGNALED(status)) {
                result = 128 + WTERMSIG(status);
            } else {
                result = PROC_ERROR;
            }

            break;
        }

        // comm: matar el grupo si se pidió cancelar la ejecución
        if (atomic_load(&run->cancel)) {
            matar_grupo(pid);
            result = PROC_CANCELED;
            break;
        }

        // comm: matar el grupo si venció el tiempo límite
        if (timeout_s > 0 && now_s() - t0 >= timeout_s) {
            matar_grupo(pid);
            result = PROC_TIMEOUT;
            break;
        }

        dormir_poll();
    }

    // comm: leer las variables definidas por el paso solo si terminó por sí mismo
    if (result >= 0 && newvars != NULL) {
        leer_setvariable(log, desde, newvars);
    }

    return result;
}
