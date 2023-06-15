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
    long fsize = ftell(f);
    fseek(f, 0, SEEK_SET);
    char* buf = malloc(sizeof(char) * (fsize)); //will put null terminator on eof character
    fread(buf, fsize, sizeof(char), f);
    buf[fsize - 1] = '\0';

    fclose(f);

    return buf;
}

int main(int argc, char** argv) {
    VDBHANDLE h = vdbclient_connect("127.0.0.1", "3333");
    char* buf;

    if (argc > 1) {
        char* queries = load_file(argv[1]);

        if (!(buf = vdbclient_execute_query(h, queries)))
            printf("server disconnected\n");

        int32_t size = *((int32_t*)buf);
        int32_t off = sizeof(int32_t);
        while (off < size) {
            int row_count = *((int32_t*)(buf + off));
            off += sizeof(int32_t);

            int col_count = *((int32_t*)(buf + off));
            off += sizeof(int32_t);
            for (int row = 0; row < row_count; row++) {
                for (int col = 0; col < col_count; col++) {
                    enum VdbTokenType type = *((uint32_t*)(buf + off));     
                    off += sizeof(uint32_t);
                    switch (type) {
                        case VDBT_TYPE_STR: {
                            int len = *((uint32_t*)(buf + off));
                            off += sizeof(uint32_t);
                            char s[len + 1];
                            memcpy(s, buf + off, len);
                            off += len;
                            s[len] = '\0';
                            printf("%s", s);
                            break;
                        }
                        case VDBT_TYPE_INT: {
                            int64_t i = *((int64_t*)(buf + off));
                            off += sizeof(int64_t);
                            printf("%ld", i);
                            break;
                        }
                        case VDBT_TYPE_BOOL: {
                            bool b = *((bool*)(buf + off));
                            off += sizeof(bool);
                            if (b) {
                                printf("true");
                            } else {
                                printf("false");
                            }
                            break;
                        }
                        case VDBT_TYPE_FLOAT: {
                            double d = *((double*)(buf + off));
                            off += sizeof(double);
                            printf("%f", d);
                            break;
                        }
                        case VDBT_TYPE_NULL: {
                            printf("null");
                            break;
                        }
                        default:
                            break;
                    }
                    if (col != col_count - 1)
                        printf(", ");
                }
                printf("\n");
            }
        }

        free(buf);

        free(queries);

    } else if (argc == 1) {
        char* line = NULL; //not including this in memory allocation tracker
        size_t len = 0;
        ssize_t nread;

        while (true) {
            printf(" > ");
            nread = getline(&line, &len, stdin);
            if (nread == -1)
                break;
            line[strlen(line) - 1] = '\0'; //get rid of newline

            if (!(buf = vdbclient_execute_query(h, line))) {
                printf("server disconnected\n");
                break;
            }

            if (strncmp(line, "exit;", strlen("exit;")) == 0) { 
                free(buf);
                break;
            }

            printf("%s", buf);
            free(buf);
        }

        free(line);

    }

    vdbclient_disconnect(h);

    return 0;
}
