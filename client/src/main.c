#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>

#include "client.h"

/*
 * Sample client program using client.h
 * Also used to run tests
 *
 */

char* load_file(const char* path) {
    FILE* f = fopen(path, "r");

    fseek(f, 0, SEEK_END);
    size_t fsize = ftell(f);
    fseek(f, 0, SEEK_SET);
    char* buf = malloc(sizeof(char) * (fsize)); //will put null terminator on eof character
    if (fread(buf, sizeof(char), fsize, f) != fsize) {
        printf("load file failed\n");
        exit(1);
    }
    buf[fsize - 1] = '\0';

    fclose(f);

    return buf;
}

int main(int argc, char** argv) {
    VDBHANDLE h = vdbclient_connect("127.0.0.1", "3333");

    if (argc > 1) {
        char* queries = load_file(argv[1]);

        struct VdbReader r = vdbclient_execute_query(h, queries);

        if (!r.buf)
            printf("server disconnected\n");

        while (vdbreader_has_unread_bytes(&r)) {
            uint32_t row;
            uint32_t col;
            vdbreader_next_set_dim(&r, &row, &col);
            for (uint32_t i = 0; i < row; i++) {
                for (uint32_t j = 0; j < col; j++) {
                    enum VdbTokenType type = vdbreader_next_type(&r);
                    switch (type) {
                        case VDBT_TYPE_INT:
                            printf("%ld", vdbreader_next_int(&r));
                            break;
                        case VDBT_TYPE_FLOAT:
                            printf("%f", vdbreader_next_float(&r));
                            break;
                        case VDBT_TYPE_STR: {
                            char* s = vdbreader_next_string(&r);
                            printf("%s", s);
                            free(s);
                            break;
                        }
                        case VDBT_TYPE_BOOL:
                            if (vdbreader_next_bool(&r)) {
                                printf("true");
                            } else {
                                printf("false");
                            }
                            break;
                        case VDBT_TYPE_NULL:
                            printf("null");
                            break;
                        default:
                            break;
                    }
                    if (j != col -1)
                        printf(", ");
                }
                printf("\n");
            }
        }

        free(r.buf);

        free(queries);

    } else if (argc == 1) {
        char* line = NULL; //not including this in memory allocation tracker
        size_t len = 0;
        ssize_t nread;
        struct VdbReader r;

        while (true) {
            printf(" > ");
            nread = getline(&line, &len, stdin);
            if (nread == -1)
                break;
            line[strlen(line) - 1] = '\0'; //get rid of newline

            r = vdbclient_execute_query(h, line);

            if (!r.buf) {
                printf("server disconnected\n");
                break;
            }

            if (strncmp(line, "exit;", strlen("exit;")) == 0) { 
                free(r.buf);
                break;
            }

            //printf("%s", buf); //TODO use struct VdbReader to get data to print
            free(r.buf);
        }

        free(line);

    }

    vdbclient_disconnect(h);

    return 0;
}
