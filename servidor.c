
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stddef.h>  
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <pthread.h>
#include <semaphore.h>
#include "thpool.h"
#include "SQLVerify.h"
#include "sqlComand.h"

sem_t semaforo;

#define FIFO_NAME "cliente_srv"

void thSql(void *arg) {
    char *sql = (char *)arg;  
    // Cria o comando SQL a partir da query
    SQLCommand *cmd = leituraSQL(sql);
    sem_wait(&semaforo); // Espera permissão
    // Executa o comando (por exemplo, INSERT, UPDATE, DELETE, etc.)
    comandoSQL(cmd);
    // Libera a memória associada ao comando
    liberarSQLCommand(cmd);
    sem_post(&semaforo); // Libera permissão
}

int main(void) {
    threadpool thpool = thpool_init(4);
    sem_post(&semaforo); // Libera permissão
    char comando[1024];
    // Cria o pipe
    if (access(FIFO_NAME, F_OK) != 0){
        mkfifo(FIFO_NAME, 0666);
    }

    printf("Servidor pronto para receber comandos SQL...\n");

    while (1) {
        // Abre o pipe para leitura
        int fd = open(FIFO_NAME, O_RDONLY);
        if (fd == -1) {
            perror("Erro ao abrir o FIFO");
            exit(EXIT_FAILURE);
        }
        // Lê a mensagem enviada pelo cliente
        int bytes = read(fd, comando, sizeof(comando));
        if (bytes > 0) {
            comando[bytes] = '\0'; // Garante terminação da string
            printf("Comando recebido: %s\n", comando);
            thpool_add_work(thpool, thSql,(void*)comando);
        }
        close(fd);
    }
    unlink(FIFO_NAME);
    return 0;
}
