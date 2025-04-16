#include <stdlib.h>
#include <pthread.h>
#include <stdio.h>
#include <termios.h>
#include <unistd.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <fcntl.h>

#define FIFO_NAME "cliente_srv"

#define botaoCima 65
#define botaoBaixo 66
#define botaoEnter 10
#define botaoEsc 26

int le_entrada(){
    // Struct da biblioteca termios.h que ira guardar as configurações do terminal
    struct termios antigoTer, novoTer;
    int tecla;

    // Salva as configurações atuais do terminal
    tcgetattr(STDIN_FILENO, &antigoTer);
    // Cria uma copia para ser manipulada sem perder as configurações originais
    novoTer = antigoTer;
    // Desativa a necessidade de ser precionado o enter ser pressionado para enviar os dados e desativa o eco do terminal
    novoTer.c_lflag &= ~(ICANON | ECHO);
    // Aplica as novas configurações ao terminal
    tcsetattr(STDIN_FILENO, TCSANOW, &novoTer);

    // Retira o ESC
    tecla = getchar();
    if(tecla == botaoEsc){
        int bytesDigitados;
        // Pega do teclados quantos bytes foram inseridos e grava
        ioctl(STDIN_FILENO, FIONREAD, &bytesDigitados);

        if(bytesDigitados > 0){
            // Descarta o caracter [ das teclas especiais
            getchar();
            // Le o proximo caracter que vai indicar a seta precionada
            tecla = getchar();
        }
        tcsetattr(STDIN_FILENO, TCSANOW, &antigoTer);
        return tecla;
    }
    tcsetattr(STDIN_FILENO, TCSANOW, &antigoTer);
    return tecla;
}

void opcao_selecionada(int valorRecebido){
    char comando[255];
    switch (valorRecebido){
        // Digitar comando
        case 0:
            printf("\033[H\033[J");
            printf("\nInforme o comando SQL: ");
            
            fgets(comando, sizeof(comando), stdin);

            // Remove o \n no final que o fgets coloca
            comando[strcspn(comando, "\n")] = 0;

            int fd = open(FIFO_NAME, O_WRONLY);

            write(fd, comando, sizeof(comando));

            close(fd);
            break;

        // Sair
        case 1:
            printf("Progrma finalizado!");
            exit(0);
            break;
        
        default:
            break;
    }
}

void *th_console_io(){
    const char* opcoes[] = {"Digitar comando", "Sair"};
    // Pega a quantidade de opcoes dinamicamente
    int numOpcoes = sizeof(opcoes) / sizeof(opcoes[0]);
    int opcSelec = 0;
    int executando = 1;

    while (executando) {
        // Exibe o menu
        printf("\033[H\033[J");
        for (int i = 0; i < numOpcoes; i++) {
            if (i == opcSelec) {
                printf("> %s\n", opcoes[i]);
            } else {
                printf("  %s\n", opcoes[i]);
            }
        }

        int key = le_entrada();
        switch (key){
            case botaoCima:
                opcSelec--;
                // Se der seta pra cima no primeiro item, vai para o ultimo
                if (opcSelec < 0) opcSelec = numOpcoes - 1;
                break;

            case botaoBaixo:
                opcSelec++;
                // Se der seta pra baixo no ultimo item vai para o primeiro
                if (opcSelec >= numOpcoes) opcSelec = 0;
                break;

            case botaoEnter:
                opcao_selecionada(opcSelec);
                break;

            case botaoEsc:
                // Para o programa
                exit(0);
                break;
            
            default:
                break;
        }
    }
    return 0;
}

int main(){
    // Cria o pipe
    if (access(FIFO_NAME, F_OK) != 0){
        mkfifo(FIFO_NAME, 0666);
    }

    // Limpa o terminal
    printf("\033[H\033[J");
    
    pthread_t thId;
    pthread_attr_t thAttr;

    pthread_attr_init(&thAttr);
	
	pthread_create(&thId, &thAttr, th_console_io, 0);
	
	pthread_join(thId, NULL);
}