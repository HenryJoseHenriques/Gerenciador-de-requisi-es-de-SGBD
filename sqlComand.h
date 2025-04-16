#include "listas.h"
//função que determina se é true ou false o where

int condition(WhereCondition *cond, DataItem *items) {
    DataItem *item = items;
    while(item != NULL) {
        if(strcmp(item->coluna, cond->coluna) == 0) {
            if(strcmp(cond->operador, "=") == 0) {
                return strcmp(item->valor, cond->valor) == 0;
            }
            else if(strcmp(cond->operador, "<>") == 0 || strcmp(cond->operador, "!=") == 0) {
                return strcmp(item->valor, cond->valor) != 0;
            }
            else if(strcmp(cond->operador, ">=") == 0) {
                return atoi(item->valor) >= atoi(cond->valor);
            }
            else if(strcmp(cond->operador, "<=") == 0) {
                return atoi(item->valor) <= atoi(cond->valor);
            }
            else if(strcmp(cond->operador, ">") == 0) {
                return atoi(item->valor) > atoi(cond->valor);
            }
            else if(strcmp(cond->operador, "<") == 0) {
                return atoi(item->valor) < atoi(cond->valor);
            }
            return 0;
        }
        item = item->proximo;
    }
    return 0;
}

int Where(WhereCondition *where, DataItem *items) {
    if (where == NULL)
        return 1;  
    
    int current = condition(where, items);

    if (where->proximo == NULL)
        return current;
    
    if (strcmp(where->connector, "AND") == 0)
        return current && Where(where->proximo, items);
    else if (strcmp(where->connector, "OR") == 0)
        return current || Where(where->proximo, items);
    else
        return current;
}

//comanddos puros sql

void update(SQLCommand *comando) {
    char filename[50];
    inputString(filename, comando->tableName);
    addString(filename, ".txt");

    FILE *fp = fopen(filename, "r");
    if (!fp) {
        printf("Tabela '%s' não encontrada\n", comando->tableName);
        return;
    }

    // Lê o cabeçalho e remove o '\n'
    char header[1024];
    if (fgets(header, sizeof(header), fp) == NULL) {
        fclose(fp);
        printf("Arquivo vazio.\n");
        return;
    }
    removeSpacesIniFim(header);
    
    // Obtém os nomes das colunas a partir do cabeçalho
    int numHeaderTokens = 0;
    char **headerTokens = splitString(header, ",", &numHeaderTokens);
    if (numHeaderTokens <= 0) {
        fclose(fp);
        printf("Cabeçalho inválido.\n");
        return;
    }
    
    // Lê as linhas restantes e armazena em um vetor (cada linha com capacidade de 1000)
    int capacity = 1000, totalRows = 0;
    char **rows = malloc(capacity * sizeof(char *));
    if (!rows) { fclose(fp); return; }
    char buffer[1024];
    while (fgets(buffer, sizeof(buffer), fp) != NULL) {
        if (totalRows >= capacity) {
            capacity *= 2;
            char **tmp = realloc(rows, capacity * sizeof(char *));
            if (!tmp) {
                for (int i = 0; i < totalRows; i++) free(rows[i]);
                free(rows);
                fclose(fp);
                return;
            }
            rows = tmp;
        }
        // Aloca 1000 caracteres para cada linha, independentemente do tamanho real lido
        rows[totalRows] = malloc(1000 * sizeof(char));
        if (rows[totalRows] == NULL) {
            for (int i = 0; i < totalRows; i++) free(rows[i]);
            free(rows);
            fclose(fp);
            return;
        }
        strncpy(rows[totalRows], buffer, 1000 - 1);
        rows[totalRows][1000 - 1] = '\0';
        totalRows++;
    }
    fclose(fp);
    
    // Cria vetor para as linhas atualizadas
    int newCount = 0;
    char **newRows = malloc(totalRows * sizeof(char *));
    if (!newRows) {
        for (int i = 0; i < totalRows; i++) free(rows[i]);
        free(rows);
        liberarStringArray(headerTokens, numHeaderTokens);
        return;
    }
    
    // Para cada linha de dados
    for (int i = 0; i < totalRows; i++) {
        strncpy(buffer, rows[i], sizeof(buffer) - 1);
        buffer[sizeof(buffer) - 1] = '\0';
        removeSpacesIniFim(buffer);
        if (buffer[0] == '\0')
            continue;
        
        // Divide a linha em tokens (valores) usando vírgula
        int numTokens = 0;
        char **tokens = splitString(buffer, ",", &numTokens);
        
        // Monta a lista de DataItem para a linha (para a avaliação da cláusula)
        DataItem *lineItems = NULL;
        DataItem *lineItemsUpdate = NULL;
        int j;
        for (j = 0; j < numTokens && j < numHeaderTokens; j++) {
            int numcolunsInfo = 0;
            char **colunsInfo = splitString(headerTokens[j], "(", &numcolunsInfo);

            removeSpaces(colunsInfo[0]);
            colunsInfo[0][strlen(colunsInfo[0]) - 1] = '\0';

            DataItem *d = criarDataItem(colunsInfo[0], tokens[j]);
            DataItem *d1 = NULL;
            int ok = 1;
            for (DataItem *item = comando->setClause; item != NULL; item = item->proximo) {
                if (strcmp(item->coluna, colunsInfo[0]) == 0) {
                    d1 = criarDataItem(colunsInfo[0], item->valor);
                    ok = 0;
                    break;
                }
            }
            if (ok) {
                d1 = criarDataItem(colunsInfo[0], tokens[j]);
            }

            if (!lineItemsUpdate) {
                lineItemsUpdate = d1;
            } else {
                DataItem *temp = lineItemsUpdate;
                while (temp->proximo)
                    temp = temp->proximo;
                temp->proximo = d1;
            }

            if (!lineItems) {
                lineItems = d;
            } else {
                DataItem *temp = lineItems;
                while (temp->proximo)
                    temp = temp->proximo;
                temp->proximo = d;
            }
            liberarStringArray(colunsInfo, numcolunsInfo);
        }
        liberarStringArray(tokens, numTokens);
        
        // Avalia a cláusula WHERE para essa linha
        int removeRow = 0;
        if (comando->whereClause != NULL)
            removeRow = Where(comando->whereClause, lineItems);
        
        // Libera a lista de DataItem original
        DataItem *tmp;
        while (lineItems) {
            tmp = lineItems;
            lineItems = lineItems->proximo;
            free(tmp);
        }
    
        // Se a linha não for atualizada, mantemos a linha original
        if (!removeRow) {
            newRows[newCount++] = rows[i];
            while (lineItemsUpdate) {
                tmp = lineItemsUpdate;
                lineItemsUpdate = lineItemsUpdate->proximo;
                free(tmp);
            }
        } else {
            // Se a linha deve ser atualizada, usamos um buffer local de 1000 e garantimos que caiba tudo
            char updatedLine[1000];
            updatedLine[0] = '\0';
            for (DataItem *di = lineItemsUpdate; di != NULL; di = di->proximo) {
                addString(updatedLine, di->valor);
                if (di->proximo != NULL)
                    addString(updatedLine, ",");
            }
            addString(updatedLine, ",");
            printf("valor1=%s\n", updatedLine);

            newRows[newCount++] = strdup(updatedLine);
            while (lineItemsUpdate) {
                tmp = lineItemsUpdate;
                lineItemsUpdate = lineItemsUpdate->proximo;
                free(tmp);
            }
            free(rows[i]);  // Libera a linha original, pois foi substituída
        }
    }
    free(rows);
    liberarStringArray(headerTokens, numHeaderTokens);
    
    // Reabre o arquivo para escrita (truncando-o) e regrava o cabeçalho e as linhas processadas
    fp = fopen(filename, "w");
    if (!fp) {
        perror("Erro ao abrir o arquivo para escrita");
        for (int i = 0; i < newCount; i++) free(newRows[i]);
        free(newRows);
        return;
    }
    fputs(header, fp);
    fputc('\n', fp);
    for (int i = 0; i < newCount; i++) {
        fputs(newRows[i], fp);
        size_t len = strlen(newRows[i]);
        if (((len == 0 || newRows[i][len - 1] != '\n') && i != newCount - 1) && newCount-1 != i)
            fputc('\n', fp);
        free(newRows[i]);
    }
    fclose(fp);
    free(newRows);
    
    printf("UPDATE realizada com sucesso.\n");
}

DataItem *select_query(SQLCommand *comando) {
    char filename[50];
    inputString(filename, comando->tableName);
    addString(filename, ".txt");

    FILE *fp = fopen(filename, "r");
    if (!fp) {
        printf("Tabela '%s' não encontrada\n", comando->tableName);
        return NULL;
    }

    // Lê o cabeçalho (primeira linha) e remove o '\n'
    char header[1024];
    if (fgets(header, sizeof(header), fp) == NULL) {
        fclose(fp);
        printf("Arquivo vazio.\n");
        return NULL;
    }
    removeSpacesIniFim(header);

    // Separa o cabeçalho em tokens (nomes das colunas) – assumindo que estão separados por vírgula
    int numHeaderTokens = 0;
    char **headerTokens = splitString(header, ",", &numHeaderTokens);
    if (numHeaderTokens <= 0) {
        fclose(fp);
        printf("Cabeçalho inválido.\n");
        return NULL;
    }

    DataItem *resultRow = NULL;
    char buffer[1024];
    // Percorre as linhas de dados
    while (fgets(buffer, sizeof(buffer), fp) != NULL) {
        removeSpacesIniFim(buffer);
        if (buffer[0] == '\0')
            continue;

        // Divide a linha em tokens (valores), assumindo separação por vírgula
        int numTokens = 0;
        char **tokens = splitString(buffer, ",", &numTokens);

        // Cria a lista de DataItem para esta linha, associando cada valor com o nome da coluna
        DataItem *lineItems = NULL;
        for (int i = 0; i < numHeaderTokens && i < numTokens; i++) {
            int numcolunsInfo = 0;
            char **colunsInfo = splitString(headerTokens[i], "(", &numcolunsInfo);

            removeSpaces(colunsInfo[0]);
            
            colunsInfo[0][strlen(colunsInfo[0])-1] = '\0'; 
            DataItem *d = criarDataItem(colunsInfo[0], tokens[i]);
            if (!lineItems) {
                lineItems = d;
            } else {
                DataItem *temp = lineItems;
                while (temp->proximo)
                    temp = temp->proximo;
                temp->proximo = d;
            }
        }
        liberarStringArray(tokens, numTokens);

        // Se há cláusula WHERE, avalia a linha; se não há cláusula, assume que a linha "satisfaz"
        int satisfies = 1;
        if (comando->whereClause != NULL)
            satisfies = Where(comando->whereClause, lineItems);

        if (satisfies) {
            resultRow = lineItems;  // retorna a lista de DataItem desta linha
            break;
        } else {
            // Libera os DataItem desta linha (pois ela não foi selecionada)
            DataItem *tmp;
            while (lineItems) {
                tmp = lineItems;
                lineItems = lineItems->proximo;
                free(tmp);
            }
        }
    }
    liberarStringArray(headerTokens, numHeaderTokens);
    fclose(fp);

    if (resultRow == NULL)
        printf("Nenhuma linha satisfaz a condição WHERE.\n");
    else
        printf("Linha selecionada com sucesso.\n");
    return resultRow;
}

void delet(SQLCommand *comando) {
    char filename[50];
    inputString(filename, comando->tableName);
    addString(filename, ".txt");

    FILE *fp = fopen(filename, "r");
    if (!fp) {
        printf("Tabela '%s' não encontrada\n", comando->tableName);
        return ;
    }

    // Lê o cabeçalho (primeira linha) e remove o '\n'
    char header[1024];
    if (fgets(header, sizeof(header), fp) == NULL) {
        fclose(fp);
        printf("Arquivo vazio.\n");
        return ;
    }
    removeSpacesIniFim(header);
    
    // Obtém os nomes das colunas a partir do cabeçalho
    int numHeaderTokens = 0;
    char **headerTokens = splitString(header, ",", &numHeaderTokens);
    if (numHeaderTokens <= 0) {
        fclose(fp);
        printf("Cabeçalho inválido.\n");
        return ;
    }
    
    // Lê as linhas restantes e armazena em um vetor
    int capacity = 10, totalRows = 0;
    char **rows = malloc(capacity * sizeof(char *));
    if (!rows) { fclose(fp); return ; }
    char buffer[1024];
    while (fgets(buffer, sizeof(buffer), fp) != NULL) {
        if (totalRows >= capacity) {
            capacity *= 2;
            char **tmp = realloc(rows, capacity * sizeof(char *));
            if (!tmp) {
                for (int i = 0; i < totalRows; i++) free(rows[i]);
                free(rows);
                fclose(fp);
                return ;
            }
            rows = tmp;
        }
        rows[totalRows++] = strdup(buffer);
    }
    fclose(fp);
    
    // Processa cada linha de dados para verificar se deve ser removida
    int newCount = 0;
    char **newRows = malloc(totalRows * sizeof(char *));
    if (!newRows) {
        for (int i = 0; i < totalRows; i++) free(rows[i]);
        free(rows);
        return ;
    }
    
    for (int i = 0; i < totalRows; i++) {
        strncpy(buffer, rows[i], sizeof(buffer) - 1);
        buffer[sizeof(buffer) - 1] = '\0';
        removeSpacesIniFim(buffer);
        if (buffer[0] == '\0')
            continue;
        
        // Divide a linha em tokens (valores) usando vírgula
        int numTokens = 0;
        char **tokens = splitString(buffer, ",", &numTokens);
        
        // Monta a lista de DataItem usando os nomes extraídos do cabeçalho
        DataItem *lineItems = NULL;
        int j;
        for (j = 0; j < numTokens && j < numHeaderTokens; j++) {
            int numcolunsInfo = 0;
            char **colunsInfo = splitString(headerTokens[j], "(", &numcolunsInfo);

            removeSpaces(colunsInfo[0]);
            
            colunsInfo[0][strlen(colunsInfo[0])-1] = '\0'; 

            DataItem *d = criarDataItem(colunsInfo[0], tokens[j]);
            if (!lineItems) {
                lineItems = d;
            } else {
                DataItem *temp = lineItems;
                while (temp->proximo)
                    temp = temp->proximo;
                temp->proximo = d;
            }
        }
        liberarStringArray(tokens, numTokens);
        
        // Avalia a cláusula WHERE para essa linha
        int removeRow = 0;
        if (comando->whereClause != NULL)
            removeRow = Where(comando->whereClause, lineItems);
        
        // Libera a lista de DataItem
        DataItem *tmp;
        while (lineItems) {
            tmp = lineItems;
            lineItems = lineItems->proximo;
            free(tmp);
        }
        
        if (!removeRow) {
            newRows[newCount++] = rows[i];  // Mantém a linha
        } else {
            free(rows[i]);  // Libera a linha removida
        }
    }
    free(rows);
    liberarStringArray(headerTokens, numHeaderTokens);
    
    // Reabre o arquivo para escrita (truncando-o) e regrava o cabeçalho e as linhas restantes
    fp = fopen(filename, "w");
    if (!fp) {
        perror("Erro ao abrir o arquivo para escrita");
        for (int i = 0; i < newCount; i++) free(newRows[i]);
        free(newRows);
        return ;
    }
    fputs(header, fp);
    fputc('\n', fp);
    for (int i = 0; i < newCount; i++) {
        fputs(newRows[i], fp);
        size_t len = strlen(newRows[i]);
        if ((len == 0 || newRows[i][len-1] != '\n') && i != newCount-1)
            fputc('\n', fp);
        free(newRows[i]);
    }
    fclose(fp);
    free(newRows);
    
    printf("DELETE realizada com sucesso.\n");
    return ;
}

void inserte(SQLCommand *comando) {
    char filename[50];
    inputString(filename, comando->tableName);
    addString(filename, ".txt");
    
    FILE *fp = fopen(filename, "r");
    if (fp != NULL) {
        char data[1000];
        if (fgets(data, sizeof(data), fp) != NULL) {
            removeSpacesIniFim(data);
        }
        fclose(fp);
       
    
        
        int numcolunsFormat;
        char **colunsFormat = splitString(data, ",", &numcolunsFormat);
        int totalTamanho = 10;
        for (int i = 0; i < numcolunsFormat; i++) {
            if (colunsFormat[i][0] != '\0') {
                int numcolunsInfo;
                char **colunsInfo = splitString(colunsFormat[i], "(", &numcolunsInfo);    
                colunsInfo[2][strlen(colunsInfo[2])-1] = '\0';
                totalTamanho += atoi(colunsInfo[2]);
            }
        }

        int ok2 = 0;
        for (InsertRow *row = comando->insertRows; row != NULL; row = row->proximo) {
            fp = fopen(filename, "a");
            int ok = 1;
            char dado[totalTamanho];
            dado[0] = '\n';
            dado[1] = '\0';
            for (int i = 0; i < numcolunsFormat; i++) {
                
                if (colunsFormat[i][0] != '\0') {
                    int numcolunsInfo;
                    char **colunsInfo = splitString(colunsFormat[i], "(", &numcolunsInfo);
   
                    
               
                    removeSpaces(colunsInfo[0]);
                    removeSpaces(colunsInfo[1]);
                    removeSpaces(colunsInfo[2]);
                    
                    colunsInfo[0][strlen(colunsInfo[0])-1] = '\0'; // remove ')'
                    colunsInfo[1][strlen(colunsInfo[1])-1] = '\0';
                    colunsInfo[2][strlen(colunsInfo[2])-1] = '\0';
                        
                        // Converte o token de tamanho para inteiro
                        
                      

                        
                    ok2 = 0;
                    for (DataItem *item = row->items; item != NULL; item = item->proximo) {

                        
                        removeSpaces(item->coluna);
                        removeSpaces(item->valor);
    
                           
                        if (strcmp(item->coluna, colunsInfo[0]) == 0) {
                                // Verifica se o tipo é compatível. Use strcmp para comparar strings.
                               // printf("%s-%s\n", item->coluna, colunsInfo[0]);
                            if ((strcmp(colunsInfo[1], "varchar") == 0) || (tipoVar(item->valor) == 1 && strcmp(colunsInfo[1], "int") == 0) || (tipoVar(item->valor) == 2 && strcmp(colunsInfo[1], "float") == 0)) {
                                addString(dado, item->valor);
                                addString(dado,",");
                                ok2 = 1;
                                printf("  Coluna: %s, Valor: %s\n", item->coluna, item->valor);
                            }else{
                                printf("Tipo nao compativel");
                                ok = 0;
                                break;
                            }
                        }
                    }
                    if(ok2 == 0){
                        ok = 0;
                        break;
                    }

                    
              
                } else {
                    printf("erro\n");
                    return;
                }
            }
            if(ok){
                fprintf(fp,"%s", dado);
            }
            fclose(fp);
            if(ok2 == 0){
                printf("Coluna nao existe");
                ok = 0;
                break;
            }
        }     
    } else {
        printf("Tabela '%s' não encontrada\n", comando->tableName);
    }
}

void create(SQLCommand *comando){
    
    char filename[50];
    inputString(filename,comando->tableName);
    addString(filename,".txt");
    FILE *fp = fopen(filename, "r");
    if (fp == NULL) {
        char data[1000];
        data[0] = '\0';
        fp = fopen(filename, "wb");
        if (fp == NULL){
            printf("Tabela '%s' erro ao criar tabela\n", comando->tableName);
            return;
        }
        fclose(fp);
        for (Elemento *atual = comando->colunas; atual != NULL; atual = atual->proximo) {
            addString(data,"(");
            addString(data,atual->nome);
            addString(data,")");
            addString(data,"(");
            addString(data,atual->tipo);
            addString(data,")");
            addString(data,"(");

            char quantidadeStr[20];
            snprintf(quantidadeStr, sizeof(quantidadeStr), "%d", atual->quantidade);
            addString(data, quantidadeStr);

            addString(data,"),");
            printf("  Nome: %s, Tipo: %s, Quantidade: %d\n",
                   atual->nome, atual->tipo, atual->quantidade);
        }
        fp = fopen(filename, "w");
        fwrite (data, sizeof (char), strlen(data), fp);
        
        printf("Tabela '%s' criada com sucesso!\n", comando->tableName);
    }else{
        
        printf("Tabela '%s' ja existe\n", comando->tableName);
    }
    fclose(fp);

}

void drop(SQLCommand *comando){
    
    char filename[50];
    inputString(filename,comando->tableName);
    addString(filename,".txt");
    FILE *fp = fopen(filename, "r");
    if (fp != NULL) {
        fclose(fp);
        remove(filename);
        printf("Tabela '%s' apagada com sucesso\n", comando->tableName);
    }else{
        fclose(fp);
        printf("Tabela '%s' nao encontrada (arquivo: %s)\n", comando->tableName, filename);
    }
   

}



//mux
void comandoSQL(SQLCommand *comando){
    switch (comando->tipo) {
        case SQL_CREATE_TABLE:
                create(comando);
            break;
        case SQL_UPDATE:
                update(comando);
            break;
        case SQL_DROP:
                drop(comando);
            break;
        case SQL_INSERT: {
                 inserte(comando);
            break;
        }
        case SQL_SELECT: {
            select_query(comando);
            break;
        }
        case SQL_DELETE: {
                delet(comando);
            break;
        }
        default:
            printf("Comando desconhecido.\n");
            break;
    }

}


//string para struct sql
SQLCommand* leituraSQL(char *sql) {
    SQLCommand *comando = malloc(sizeof(SQLCommand));
    if (!comando) {
        fprintf(stderr, "Erro ao alocar memória para SQLCommand.\n");
        exit(EXIT_FAILURE);
    }
    comando->colunas = NULL;
    comando->setClause = NULL;
    comando->insertRows = NULL;
    comando->whereClause = NULL;
    comando->selectColumns = NULL;
    comando->numSelectColumns = 0;
    comando->tableName[0] = '\0';

    if (strncasecmp(sql, "CREATE TABLE", 12) == 0) {
        comando->tipo = SQL_CREATE_TABLE;
        if (sscanf(sql, "CREATE TABLE %49s", comando->tableName) != 1) {
            fprintf(stderr, "Erro ao ler o nome da tabela.\n");
            comando->tipo = SQL_UNKNOWN;
        }
        char *start = strchr(sql, '(');
        char *end = strrchr(sql, ')');
        if (start && end && start < end) {
            int len = end - start - 1;
            char *columnsStr = malloc(len + 1);
            if (!columnsStr) {
                fprintf(stderr, "Erro ao alocar memória.\n");
                exit(EXIT_FAILURE);
            }
            strncpy(columnsStr, start + 1, len);
            columnsStr[len] = '\0';
            comando->colunas = parseCreateTableColumns(columnsStr);
            free(columnsStr);  
        }
        
    } else if (strncasecmp(sql, "UPDATE", 6) == 0) {
        comando->tipo = SQL_UPDATE;
        if (sscanf(sql, "UPDATE %49s", comando->tableName) != 1) {
            fprintf(stderr, "Erro ao ler o nome da tabela no UPDATE.\n");
            comando->tipo = SQL_UNKNOWN;
        }
        char *setPointer = strcasestr(sql, "SET");
        if (setPointer) {
            setPointer += 3;
            while (isspace((unsigned char)*setPointer)) setPointer++;
            char *wherePtr = strcasestr(setPointer, "WHERE");
            int len = (wherePtr) ? (wherePtr - setPointer) : strlen(setPointer);
            char *setStr = malloc(len + 1);
            if (!setStr) {
                fprintf(stderr, "Erro ao alocar memória para setStr.\n");
                exit(EXIT_FAILURE);
            }
            strncpy(setStr, setPointer, len);
            setStr[len] = '\0';
            comando->setClause = parseUpdateSet(setStr);
            free(setStr);
            if (wherePtr) {
                wherePtr += strlen("WHERE");
                while (isspace((unsigned char)*wherePtr)) wherePtr++;
                char *endPtr = strchr(wherePtr, ';');
                int lenWhere = (endPtr) ? (endPtr - wherePtr) : strlen(wherePtr);
                char *whereStr = malloc(lenWhere + 1);
                if (!whereStr) {
                    fprintf(stderr, "Erro ao alocar memória para whereStr.\n");
                    exit(EXIT_FAILURE);
                }
                strncpy(whereStr, wherePtr, lenWhere);
                whereStr[lenWhere] = '\0';
                comando->whereClause = parseWhereClause(whereStr);
                free(whereStr);
            }
        }
    } else if (strncasecmp(sql, "DROP", 4) == 0) {
        comando->tipo = SQL_DROP;
        if (sscanf(sql, "DROP TABLE %49s", comando->tableName) != 1) {
            fprintf(stderr, "Erro ao ler o nome da tabela no DROP.\n");
            comando->tipo = SQL_UNKNOWN;
        }
    } else if (strncasecmp(sql, "INSERT", 6) == 0) {
        comando->tipo = SQL_INSERT;
        if (strncasecmp(sql, "INSERT INTO", 11) == 0) {
            if (sscanf(sql, "INSERT INTO %49s", comando->tableName) != 1) {
                fprintf(stderr, "Erro ao ler o nome da tabela no INSERT.\n");
                comando->tipo = SQL_UNKNOWN;
            }
            char *firstParen = strchr(sql, '(');
            if (firstParen) {
                char *firstCloseParen = strchr(firstParen, ')');
                if (firstCloseParen && firstCloseParen > firstParen) {
                    int lenColumns = firstCloseParen - firstParen - 1;
                    char *columnsStr = malloc(lenColumns + 1);
                    if (!columnsStr) {
                        fprintf(stderr, "Erro ao alocar memória.\n");
                        exit(EXIT_FAILURE);
                    }
                    strncpy(columnsStr, firstParen + 1, lenColumns);
                    columnsStr[lenColumns] = '\0';
                    char *colNames[50];
                    int numCols = 0;
                    char *token = strtok(columnsStr, ",");
                    while (token && numCols < 50) {
                        removeSpacesIniFim(token);
                        colNames[numCols] = strdup(token);
                        numCols++;
                        token = strtok(NULL, ",");
                    }
                    free(columnsStr);
                    char *valuesPtr = strcasestr(sql, "VALUES");
                    if (valuesPtr) {
                        valuesPtr += strlen("VALUES");
                        while (isspace((unsigned char)*valuesPtr)) valuesPtr++;
                        comando->insertRows = parseInsertRows(valuesPtr, colNames, numCols);
                    }
                    for (int i = 0; i < numCols; i++) {
                        free(colNames[i]);
                    }
                }
            }
        } else {
            comando->tipo = SQL_UNKNOWN;
        }
    } else if (strncasecmp(sql, "SELECT", 6) == 0) {
        comando->tipo = SQL_SELECT;
        char *fromPtr = strcasestr(sql, "FROM");
        if (!fromPtr) {
            fprintf(stderr, "Erro: 'FROM' não encontrado no SELECT.\n");
            comando->tipo = SQL_UNKNOWN;
        } else {
            int lenCols = fromPtr - (sql + 6);
            char *colsPart = malloc(lenCols + 1);
            if (!colsPart) {
                fprintf(stderr, "Erro ao alocar memória para colsPart.\n");
                exit(EXIT_FAILURE);
            }
            strncpy(colsPart, sql + 6, lenCols);
            colsPart[lenCols] = '\0';
            char *colNames[50];
            int count = 0;
            char *token = strtok(colsPart, ",");
            while (token && count < 50) {
                removeSpacesIniFim(token);
                colNames[count] = strdup(token);
                count++;
                token = strtok(NULL, ",");
            }
            free(colsPart);
            comando->selectColumns = malloc(count * sizeof(char*));
            for (int i = 0; i < count; i++) {
                comando->selectColumns[i] = colNames[i];
            }
            comando->numSelectColumns = count;
            char *tablePtr = fromPtr + strlen("FROM");
            while (isspace((unsigned char)*tablePtr)) tablePtr++;
            if (sscanf(tablePtr, " %49s", comando->tableName) != 1) {
                fprintf(stderr, "Erro ao ler o nome da tabela no SELECT.\n");
                comando->tipo = SQL_UNKNOWN;
            }
            char *wherePtr = strcasestr(sql, "WHERE");
            if (wherePtr) {
                wherePtr += strlen("WHERE");
                while (isspace((unsigned char)*wherePtr)) wherePtr++;
                char *endPtr = strchr(wherePtr, ';');
                int lenWhere = (endPtr) ? (endPtr - wherePtr) : strlen(wherePtr);
                char *whereStr = malloc(lenWhere + 1);
                if (!whereStr) {
                    fprintf(stderr, "Erro ao alocar memória para whereStr.\n");
                    exit(EXIT_FAILURE);
                }
                strncpy(whereStr, wherePtr, lenWhere);
                whereStr[lenWhere] = '\0';
                comando->whereClause = parseWhereClause(whereStr);
                free(whereStr);
            }
        }
    } else if (strncasecmp(sql, "DELETE", 6) == 0) {
        comando->tipo = SQL_DELETE;
        char *fromPtr = strcasestr(sql, "FROM");
        if (!fromPtr) {
            fprintf(stderr, "Erro: 'FROM' não encontrado no DELETE.\n");
            comando->tipo = SQL_UNKNOWN;
        } else {
            int lenCols = fromPtr - (sql + 6);
            char *colsPart = malloc(lenCols + 1);
            if (!colsPart) {
                fprintf(stderr, "Erro ao alocar memória para colsPart.\n");
                exit(EXIT_FAILURE);
            }
            strncpy(colsPart, sql + 6, lenCols);

            char *tablePtr = fromPtr + strlen("FROM");
            while (isspace((unsigned char)*tablePtr)) tablePtr++;
            if (sscanf(tablePtr, " %49s", comando->tableName) != 1) {
                fprintf(stderr, "Erro ao ler o nome da tabela no DELETE.\n");
                comando->tipo = SQL_UNKNOWN;
            }
            char *wherePtr = strcasestr(sql, "WHERE");
            if (wherePtr) {
                wherePtr += strlen("WHERE");
                while (isspace((unsigned char)*wherePtr)) wherePtr++;
                char *endPtr = strchr(wherePtr, ';');
                int lenWhere = (endPtr) ? (endPtr - wherePtr) : strlen(wherePtr);
                char *whereStr = malloc(lenWhere + 1);
                if (!whereStr) {
                    fprintf(stderr, "Erro ao alocar memória para whereStr.\n");
                    exit(EXIT_FAILURE);
                }
                strncpy(whereStr, wherePtr, lenWhere);
                whereStr[lenWhere] = '\0';
                comando->whereClause = parseWhereClause(whereStr);
                free(whereStr);
            }
        }
    }else{
        comando->tipo = SQL_UNKNOWN;
    }
    return comando;
}


//printa pora teste
void printSQLCommand(SQLCommand *comando) {
    if (!comando) return;
    switch (comando->tipo) {
        case SQL_CREATE_TABLE:
            printf("Comando: CREATE TABLE\nTabela: %s\nColunas:\n", comando->tableName);
            for (Elemento *atual = comando->colunas; atual != NULL; atual = atual->proximo) {
                printf("  Nome: %s, Tipo: %s, Quantidade: %d\n",
                       atual->nome, atual->tipo, atual->quantidade);
            }
            break;
        case SQL_UPDATE:
            printf("Comando: UPDATE\nTabela: %s\nSET:\n", comando->tableName);
            for (DataItem *item = comando->setClause; item != NULL; item = item->proximo) {
                printf("  Coluna: %s, Valor: %s\n", item->coluna, item->valor);
            }
            if (comando->whereClause) {
                printf("WHERE:\n");
                for (WhereCondition *cond = comando->whereClause; cond != NULL; cond = cond->proximo) {
                    printf("  %s %s %s", cond->coluna, cond->operador, cond->valor);
                    if (strlen(cond->connector) > 0)
                        printf(" %s", cond->connector);
                    printf("\n");
                }
            }
            break;
        case SQL_DROP:
            printf("Comando: DROP\nTabela: %s\n", comando->tableName);
            break;
        case SQL_INSERT: {
            printf("Comando: INSERT\nTabela: %s\n", comando->tableName);
            int rowNum = 1;
            for (InsertRow *row = comando->insertRows; row != NULL; row = row->proximo) {
                printf(" Linha %d:\n", rowNum);
                for (DataItem *item = row->items; item != NULL; item = item->proximo) {
                    printf("  Coluna: %s, Valor: %s\n", item->coluna, item->valor);
                }
                rowNum++;
            }
            break;
        }
        case SQL_SELECT: {
            printf("Comando: SELECT\nTabela: %s\nColunas selecionadas:\n", comando->tableName);
            for (int i = 0; i < comando->numSelectColumns; i++) {
                printf("  %s\n", comando->selectColumns[i]);
            }
            if (comando->whereClause) {
                printf("WHERE:\n");
                for (WhereCondition *cond = comando->whereClause; cond != NULL; cond = cond->proximo) {
                    printf("  %s %s %s", cond->coluna, cond->operador, cond->valor);
                    if (strlen(cond->connector) > 0)
                        printf(" %s", cond->connector);
                    printf("\n");
                }
            }
            break;
        }
        case SQL_DELETE: {
            printf("\nComando: DELETE\nTabela: %s\n", comando->tableName);
            if (comando->whereClause) {
                printf("WHERE:\n");
                for (WhereCondition *cond = comando->whereClause; cond != NULL; cond = cond->proximo) {
                    printf("  %s %s %s", cond->coluna, cond->operador, cond->valor);
                    if (strlen(cond->connector) > 0)
                        printf(" %s", cond->connector);
                    printf("\n");
                }
            }
            break;
        }
        default:
            printf("Comando desconhecido.\n");
            break;
    }
}