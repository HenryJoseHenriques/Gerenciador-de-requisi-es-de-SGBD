#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdbool.h>
#define TAM_CONTAINER 5

typedef struct {
    char **dados;
    int quantidade;
} BufferThread;

// Função para contar quantos comandos SQL existem (separados por ;)
int contarComandosSQL(const char *comandoSQL) {
    int count = 0;
    const char *ptr = comandoSQL;
    while (*ptr) {
        if (*ptr == ';') {
            count++;
            // Pular múltiplos ; consecutivos
            while (*ptr == ';') ptr++;
        } else {
            ptr++;
        }
    }
    return count;
}

// Função para separar os comandos SQL
char **separarComandosSQL(const char *comandoSQL) {
    if (!comandoSQL || !*comandoSQL) return NULL;
    
    int quantComandos = contarComandosSQL(comandoSQL);
    char **arrayComandos = malloc((quantComandos + 1) * sizeof(char*)); // +1 para NULL terminator
    if (!arrayComandos) return NULL;
    
    const char *start = comandoSQL;
    const char *end;
    int i = 0;
    
    while (*start && i < quantComandos) {
        // Encontrar o próximo ;
        end = strchr(start, ';');
        if (!end) break;
        
        // Calcular tamanho do comando (sem o ;)
        size_t len = end - start;
        
        // Alocar memória para o comando
        arrayComandos[i] = malloc(len + 1);
        if (!arrayComandos[i]) {
            // Liberar memória alocada em caso de erro
            for (int j = 0; j < i; j++) free(arrayComandos[j]);
            free(arrayComandos);
            return NULL;
        }
        
        // Copiar o comando
        strncpy(arrayComandos[i], start, len);
        arrayComandos[i][len] = '\0';
        
        // Remover espaços em branco no início/fim
        char *cmd = arrayComandos[i];
        while (isspace((unsigned char)*cmd)) cmd++;
        
        size_t cmd_len = strlen(cmd);
        while (cmd_len > 0 && isspace((unsigned char)cmd[cmd_len - 1])) {
            cmd[cmd_len - 1] = '\0';
            cmd_len--;
        }
        
        // Mover para depois do ;
        start = end + 1;
        
        // Só incrementar se o comando não estiver vazio
        if (*arrayComandos[i]) i++;
        else free(arrayComandos[i]);
    }
    
    arrayComandos[i] = NULL; // Terminador NULL
    return arrayComandos;
}

// Função para liberar a memória alocada
void liberarComandosSQL(char **comandos) {
    if (!comandos) return;
    
    for (int i = 0; comandos[i]; i++) {
        free(comandos[i]);
    }
    free(comandos);
}

bool verificaOrtografia(char **comandos) {
    for(int i = 0; comandos[i]; i++) {
        // Cria uma cópia modificável do comando
        char *comando = strdup(comandos[i]);
        if (!comando) return false; // Falha na alocação
        // Converte para maiúsculas
        for (int j = 0; comando[j]; j++) {
            comando[j] = toupper(comando[j]);
        }
        // Encontra a primeira ocorrência de espaço
        char *primeiroEspaco = strchr(comando, ' ');
        if (primeiroEspaco) {
            // Calcula o tamanho até o primeiro espaço
            size_t tamPrefix = primeiroEspaco - comando;
            // Verifica se é "CREATE TABLE"
            if (strncmp(comando, "CREATE TABLE", tamPrefix) == 0 && 
                tamPrefix == strlen("CREATE TABLE")) {
                free(comando);
                return true;
            }
            else if (strncmp(comando, "UPDATE", tamPrefix) == 0 && 
                tamPrefix == strlen("UPDATE")) {
                free(comando);
                return true;
            }
            else if (strncmp(comando, "DROP TABLE", tamPrefix) == 0 && 
                tamPrefix == strlen("DROP TABLE")) {
                free(comando); // Libera a cópia
                return true;
            }
            else if (strncmp(comando, "INSERT INTO", tamPrefix) == 0 && 
                tamPrefix == strlen("INSERT INTO")) {
                free(comando); // Libera a cópia
                return true;
            }
            else if (strncmp(comando, "SELECT", tamPrefix) == 0 && 
                tamPrefix == strlen("SELECT")) {
                free(comando); // Libera a cópia
                return true;
            }
            else if (strncmp(comando, "DELETE", tamPrefix) == 0 && 
                tamPrefix == strlen("DELETE")) {
                free(comando); // Libera a cópia
                return true;
            }else{
                return false;
            }
        }
    }
}

void retornarBufferThread(char **comandos, int *k, int total, BufferThread *buffer) {
    buffer->dados = malloc(TAM_CONTAINER * sizeof(char *));
    buffer->quantidade = 0;

    for (int i = 0; i < TAM_CONTAINER && (*k) < total; i++) {
        buffer->dados[i] = comandos[*k];
        buffer->quantidade++;
        (*k)++;
    }
}

void processaComandos(char **comandos, int total, BufferThread *buffers) {
    // Calcular quantos buffers serão necessários (arredondando para cima)
    int numBuffers = (total + TAM_CONTAINER - 1) / TAM_CONTAINER;
    buffers = malloc(numBuffers * sizeof(BufferThread));
    
    int k = 0;
    for (int i = 0; i < numBuffers; i++) {
        retornarBufferThread(comandos, &k, total, &buffers[i]);
    }
    
    printf("\nBuffers criados (grupos de até %d comandos):\n", TAM_CONTAINER);
    for (int i = 0; i < numBuffers; i++) {
        printf("Buffer %d (%d comandos):\n", i+1, buffers[i].quantidade);
        for (int j = 0; j < buffers[i].quantidade; j++) {
            printf("  %s\n", buffers[i].dados[j]);
        }
        printf("\n");
    }
}