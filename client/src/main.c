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


void vdbreader_sprint_value(char* buf, int maxlen, struct VdbReader* r) {
    enum VdbTokenType type = vdbreader_next_type(r);

    switch (type) {
        case VDBT_TYPE_INT:
            snprintf(buf, maxlen, "%ld", vdbreader_next_int(r));
            break;
        case VDBT_TYPE_FLOAT:
            snprintf(buf, maxlen, "%f", vdbreader_next_float(r));
            break;
        case VDBT_TYPE_STR: {
            char* s = vdbreader_next_string(r);
            snprintf(buf, maxlen, "%s", s);
            free(s);
            break;
        }
        case VDBT_TYPE_BOOL:
            if (vdbreader_next_bool(r)) {
                snprintf(buf, maxlen, "true");
            } else {
                snprintf(buf, maxlen, "false");
            }
            break;
        case VDBT_TYPE_NULL:
            snprintf(buf, maxlen, "null");
            break;
        default:
            break;
    }
}

void vdbreader_find_col_maxlens(struct VdbReader* reader, uint32_t row, uint32_t col, uint32_t* maxlens) {
    uint32_t cached_idx = reader->idx;

    for (uint32_t i = 0; i < col; i++) {
        maxlens[i] = 0;
    }

    for (uint32_t r = 0; r < row; r++) {
        for (uint32_t c = 0; c < col; c++) {
            char buf[1024];
            vdbreader_sprint_value(buf, 1024, reader);
            if (strlen(buf) > maxlens[c])
                maxlens[c] = strlen(buf);
        }
    }

    reader->idx = cached_idx;
}


void vdbreader_print_value(struct VdbReader* r) {
    enum VdbTokenType type = vdbreader_next_type(r);

    switch (type) {
        case VDBT_TYPE_INT:
            printf("%ld", vdbreader_next_int(r));
            break;
        case VDBT_TYPE_FLOAT:
            printf("%f", vdbreader_next_float(r));
            break;
        case VDBT_TYPE_STR: {
            char* s = vdbreader_next_string(r);
            printf("%s", s);
            free(s);
            break;
        }
        case VDBT_TYPE_BOOL:
            if (vdbreader_next_bool(r)) {
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
}

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

        //printing output to match .expected file format for tests
        while (vdbreader_has_unread_bytes(&r)) {
            uint8_t is_tuple = vdbreader_next_is_tuple(&r);
            if (is_tuple) {
                uint32_t row;
                uint32_t col;
                vdbreader_next_set_dim(&r, &row, &col);
                for (uint32_t i = 0; i < row; i++) {
                    for (uint32_t j = 0; j < col; j++) {
                        vdbreader_print_value(&r);
                        if (j != col -1)
                            printf(", ");
                    }
                    printf("\n");
                }
            } else {
                vdbreader_print_value(&r);
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
            printf("vdb> ");
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

            /*
            char tuple_line[18];
            tuple_line[0] = '|';
            tuple_line[1] = ' ';
            tuple_line[14] = ' ';
            tuple_line[15] = '|';
            tuple_line[16] = '\n';
            tuple_line[17] = '\0';*/

            while (vdbreader_has_unread_bytes(&r)) {
                uint8_t is_tuple = vdbreader_next_is_tuple(&r);

                if (is_tuple) {
                    uint32_t row;
                    uint32_t col;
                    vdbreader_next_set_dim(&r, &row, &col);

                    uint32_t maxlens[col];
                    vdbreader_find_col_maxlens(&r, row, col, maxlens);

                    int colwidths = 0;
                    for (uint32_t i = 0; i < col; i++) {
                        colwidths += maxlens[i];
                    }

                    //max column widths + middle " | " + beginning/ending + newline + null-terminator
                    int total_len = colwidths + 3 * (col - 1) + 6;
                    char tuple_line[total_len];
                    memset(tuple_line, ' ', total_len);
                    tuple_line[0] = '|';
                    tuple_line[1] = ' ';
                    tuple_line[total_len - 4] = ' ';
                    tuple_line[total_len - 3] = '|';
                    tuple_line[total_len - 2] = '\n';
                    tuple_line[total_len - 1] = '\0';

                    char separator_line[total_len];
                    memset(separator_line, '-', total_len);
                    separator_line[0] = '+';
                    separator_line[total_len - 3] = '+';
                    separator_line[total_len - 2] = '\n';
                    separator_line[total_len - 1] = '\0';

                    int off = 2 + maxlens[0];
                    for (uint32_t i = 1; i < col; i++) {
                        separator_line[off + 1] = '+'; 
                        tuple_line[off + 1] = '|'; 
                        off += 3 + maxlens[i];
                    }


                    for (uint32_t i = 0; i < row; i++) {
                        if (i == 0) {
                            printf("%s", separator_line);
                        }
                        int off = 2;
                        for (uint32_t j = 0; j < col; j++) {
                            memset(tuple_line + off, ' ', maxlens[j]);
                            vdbreader_sprint_value(tuple_line + off, maxlens[j] + 1, &r);
                            tuple_line[strlen(tuple_line)] = ' ';
                            off += maxlens[j] + 3;
                        }
                        printf("%s", tuple_line);
                        if (i == 0) {
                            printf("%s", separator_line);
                        }
                    }
                    printf("%s", separator_line);
                } else {
                    vdbreader_print_value(&r);
                    printf("\n");
                }
            }

            free(r.buf);
        }

        free(line);

    }

    vdbclient_disconnect(h);

    return 0;
}
