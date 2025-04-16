//manipular string
void removeSpacesIniFim(char *str) {
    char *end;
    while (isspace((unsigned char)*str))
        str++;
    if (*str == 0) return;
    end = str + strlen(str) - 1;
    while (end > str && isspace((unsigned char)*end)) {
        *end = '\0';
        end--;
    }
}

void removeSpaces(char *str) {
    char *src = str, *dst = str;
    while (*src != '\0') {
        if (!isspace((unsigned char)*src)) {
            *dst++ = *src;
        }
        src++;
    }
    *dst = '\0';
}


void _inputString(char *dest, size_t destSize, const char *src) {
    if (!dest || destSize == 0)
        return;
    snprintf(dest, destSize, "%s", src);
}

#define inputString(dest, src) _inputString((dest), sizeof(dest), (src))
#define addString(dest, src) strncat((dest), (src), sizeof(dest) - strlen(dest) - 1)




//verifica se existe string na string
char *strcasestr(const char *haystack, const char *needle) {
    if (!*needle)
        return (char *)haystack;

    for (; *haystack; haystack++) {
        if (tolower((unsigned char)*haystack) == tolower((unsigned char)*needle)) {
            const char *h = haystack;
            const char *n = needle;
            while (*h && *n && (tolower((unsigned char)*h) == tolower((unsigned char)*n))) {
                h++;
                n++;
            }
            if (!*n)
                return (char *)haystack;
        }
    }
    return NULL;
}

//criar um array com um termo de separação 
char **splitString(const char *str, const char *delim, int *numTokens) {
    char *copy = strdup(str);
    if (!copy) return NULL;
    int count = 0;
    char *token = strtok(copy, delim);
    while (token) {
         count++;
         token = strtok(NULL, delim);
    }
    free(copy);
    char **result = malloc(count * sizeof(char *));
    if (!result) return NULL;
    copy = strdup(str);
    token = strtok(copy, delim);
    int index = 0;
    while (token) {
         result[index++] = strdup(token);
         token = strtok(NULL, delim);
    }
    free(copy);
    *numTokens = count;
    return result;
}


//se tiver so inteiro retorna 1 float 2 e vachar 0
int tipoVar(char *str) {
    if (str == NULL || *str == '\0')
        return 0;

    int dotCount = 0;
    char *p = str;

    while (*p != '\0') {
        if (*p == '.') {
            dotCount++;
        } else if (!isdigit((unsigned char)*p)) {
            return 0; 
        }
        p++;
    }
    
    if (dotCount == 0)
        return 1;  
    else if (dotCount == 1)
        return 2; 

}


void liberarStringArray(char **arr, int numTokens) {
    for (int i = 0; i < numTokens; i++) {
        free(arr[i]);
    }
    free(arr);
}

/************* struct ****************/

typedef enum {
    SQL_CREATE_TABLE,
    SQL_INSERT,
    SQL_SELECT,
    SQL_UPDATE,
    SQL_DROP,
    SQL_DELETE,
    SQL_UNKNOWN
} SQLCommandType;

typedef struct Elemento {
    char nome[50];
    char tipo[50];
    int quantidade;
    struct Elemento *proximo;
} Elemento;

typedef struct DataItem {
    char coluna[50];
    char valor[50];
    struct DataItem *proximo;
} DataItem;

typedef struct InsertRow {
    DataItem *items;
    struct InsertRow *proximo;
} InsertRow;

typedef struct WhereCondition {
    char coluna[50];
    char operador[10];
    char valor[50];
    char connector[4];
    struct WhereCondition *proximo;
} WhereCondition;

typedef struct SQLCommand {
    SQLCommandType tipo;
    char tableName[50];
    Elemento *colunas;
    DataItem *setClause;
    InsertRow *insertRows;
    WhereCondition *whereClause;
    char **selectColumns;
    int numSelectColumns;
} SQLCommand;





//criar elementos lista encadeada simples
Elemento* criarElemento(const char* nome, const char* tipo, int quantidade) {
    Elemento* novo = malloc(sizeof(Elemento));
    if (!novo) {
        fprintf(stderr, "Erro ao alocar memória para Elemento.\n");
        exit(EXIT_FAILURE);
    }
    inputString(novo->nome, nome);
    inputString(novo->tipo, tipo);
    novo->quantidade = quantidade;
    novo->proximo = NULL;
    return novo;
}

DataItem* criarDataItem(const char* coluna, const char* valor) {
    DataItem* novo = malloc(sizeof(DataItem));
    if (!novo) {
        fprintf(stderr, "Erro ao alocar memória para DataItem.\n");
        exit(EXIT_FAILURE);
    }
    inputString(novo->coluna, coluna);
    inputString(novo->valor, valor);
    novo->proximo = NULL;
    return novo;
}

WhereCondition* criarWhereCondition(const char *coluna, const char *op, const char *valor, const char *connector) {
    WhereCondition *novo = malloc(sizeof(WhereCondition));
    if (!novo) {
        fprintf(stderr, "Erro ao alocar memória para WhereCondition.\n");
        exit(EXIT_FAILURE);
    }
    inputString(novo->coluna, coluna);
    inputString(novo->operador, op);
    inputString(novo->valor, valor);
    if (connector)
        inputString(novo->connector, connector);
    else
        novo->connector[0] = '\0';
    novo->proximo = NULL;
    return novo;
}

// strings para lista encadiada 

void addNo(void **head, void *node, size_t proximoOffset) {
    if (!(*head)) {
        *head = node;
    } else {
        char *cur = *head;
        while (*(void **)(cur + proximoOffset)) {
            cur = *(void **)(cur + proximoOffset);
        }
        *(void **)(cur + proximoOffset) = node;
    }
}

Elemento* parseCreateTableColumns(char *columnsStr) {
    Elemento *head = NULL;
    char *token = strtok(columnsStr, ",");
    while (token != NULL) {
        char colName[50], typeName[50];
        int quantidade = 50, numCampos;
        numCampos = sscanf(token, " %49s %49[^(\n]", colName, typeName);
        if (numCampos < 2) {
            token = strtok(NULL, ",");
            continue;
        }
        char *p = strchr(token, '(');
        if (p != NULL) {
            quantidade = atoi(p + 1);
        }
        Elemento *novo = criarElemento(colName, typeName, quantidade);
        addNo((void**)&head, novo, offsetof(Elemento, proximo));
        token = strtok(NULL, ",");
    }
    return head;
}

DataItem* parseUpdateSet(char *setStr) {
    DataItem *head = NULL;
    char *token = strtok(setStr, ",");
    while (token != NULL) {
        char coluna[50], valor[50];
        if (sscanf(token, " %49[^=]= %49s", coluna, valor) == 2) {
            removeSpacesIniFim(coluna);
            removeSpacesIniFim(valor);
            DataItem *novo = criarDataItem(coluna, valor);
            addNo((void**)&head, novo, offsetof(DataItem, proximo));
        }
        token = strtok(NULL, ",");
    }
    return head;
}

InsertRow* parseInsertRows(char *valuesStr, char **colNames, int numCols) {
    InsertRow *head = NULL;
    char *p = valuesStr;
    while (1) {
        char *start = strchr(p, '(');
        if (!start) break;
        char *end = strchr(start, ')');
        if (!end) break;
        int len = end - start - 1;
        char *rowBuffer = malloc(len + 1);
        if (!rowBuffer) {
            fprintf(stderr, "Erro ao alocar memória para rowBuffer.\n");
            exit(EXIT_FAILURE);
        }
        strncpy(rowBuffer, start + 1, len);
        rowBuffer[len] = '\0';
        DataItem *itemHead = NULL;
        int colIndex = 0;
        char *valueToken = strtok(rowBuffer, ",");
        while (valueToken && colIndex < numCols) {
            removeSpacesIniFim(valueToken);
            DataItem *item = criarDataItem(colNames[colIndex], valueToken);
            addNo((void**)&itemHead, item, offsetof(DataItem, proximo));
            colIndex++;
            valueToken = strtok(NULL, ",");
        }
        free(rowBuffer);
        if (colIndex != numCols) {
            fprintf(stderr, "Erro: número de valores (%d) difere do número de colunas (%d).\n", colIndex, numCols);
        }
        InsertRow *newRow = malloc(sizeof(InsertRow));
        if (!newRow) {
            fprintf(stderr, "Erro ao alocar memória para InsertRow.\n");
            exit(EXIT_FAILURE);
        }
        newRow->items = itemHead;
        newRow->proximo = NULL;
        addNo((void**)&head, newRow, offsetof(InsertRow, proximo));
        p = end + 1;
        while (isspace((unsigned char)*p) || *p == ',') p++;
    }
    return head;
}

WhereCondition* parseWhereClause(char *whereStr) {
    WhereCondition *head = NULL;
    char conditionBuffer[256];
    char *p = whereStr;
    
    while (*p != '\0') {
        char *andPtr = strcasestr(p, " AND ");
        char *orPtr = strcasestr(p, " OR ");
        char *delimiter = NULL;
        if (andPtr && orPtr)
            delimiter = (andPtr < orPtr) ? andPtr : orPtr;
        else if (andPtr)
            delimiter = andPtr;
        else if (orPtr)
            delimiter = orPtr;
        else
            delimiter = NULL;
        int len = (delimiter) ? (delimiter - p) : strlen(p);
        strncpy(conditionBuffer, p, len);
        conditionBuffer[len] = '\0';
        removeSpacesIniFim(conditionBuffer);
        if (strlen(conditionBuffer) > 0) {
            char col[50], op[10], val[50];
            if (sscanf(conditionBuffer, " %49s %9s %49s", col, op, val) == 3) {
                char connector[4] = "";
                if (delimiter) {
                    if (strncasecmp(delimiter, " AND ", 5) == 0)
                        strcpy(connector, "AND");
                    else if (strncasecmp(delimiter, " OR ", 4) == 0)
                        strcpy(connector, "OR");
                }
                WhereCondition *novo = criarWhereCondition(col, op, val, connector);
                addNo((void**)&head, novo, offsetof(WhereCondition, proximo));
            } else {
                fprintf(stderr, "Falha ao parsear condição: %s\n", conditionBuffer);
            }
        }
        if (delimiter)
            p = delimiter + ((strncasecmp(delimiter, " AND ", 5) == 0) ? 5 : 4);
        else
            break;
    }
    return head;
}


// liberação de lista encadeadas memoria
void liberarLista(void *head, size_t proximoOffset, void (*freeNode)(void*)) {
    void *cur = head;
    while (cur) {
        void *next = *(void **)((char*)cur + proximoOffset);
        freeNode(cur);
        cur = next;
    }
}


void liberarInsert(void *node) {
    InsertRow *row = node;
    liberarLista(row->items, offsetof(DataItem, proximo), free);
    free(row);
}

void liberarSQLCommand(SQLCommand *comando) {
    if (comando) {
        if (comando->colunas)
            liberarLista(comando->colunas, offsetof(Elemento, proximo), free);
        if (comando->setClause)
            liberarLista(comando->setClause, offsetof(DataItem, proximo), free);
        if (comando->insertRows)
            liberarLista(comando->insertRows, offsetof(InsertRow, proximo), liberarInsert);
        if (comando->whereClause)
            liberarLista(comando->whereClause, offsetof(WhereCondition, proximo), free);
        if (comando->selectColumns) {
            for (int i = 0; i < comando->numSelectColumns; i++) {
                free(comando->selectColumns[i]);
            }
            free(comando->selectColumns);
        }
        free(comando);
    }
}






