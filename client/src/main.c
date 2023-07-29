#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <threads.h>

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

int fcn(void* args) {
    int thrd_id = *((int*)(args));
    char* port_arg = (char*)((uint8_t*)args + sizeof(int));;
    VDBHANDLE h = vdbclient_connect("127.0.0.1", port_arg);

    char* setup_query = "open sol;";
    struct VdbReader r = vdbclient_execute_query(h, setup_query);
    free(r.buf);

    char update_buf[64];
    sprintf(update_buf, "update planets set aa=%d, bb=%d, cc=%d, dd=%d where id=1;", thrd_id, thrd_id, thrd_id, thrd_id);
    char* select_query = "select aa, bb, cc, dd from planets;";

    for (int i = 0; i < 10000; i++) {
        r = vdbclient_execute_query(h, update_buf);
        free(r.buf);

        r = vdbclient_execute_query(h, select_query);

        while (vdbreader_has_unread_bytes(&r)) {
            uint8_t is_tuple = vdbreader_next_is_tuple(&r);
            if (is_tuple) {
                uint32_t row;
                uint32_t col;
                vdbreader_next_set_dim(&r, &row, &col);
                for (uint32_t i = 0; i < row; i++) {
                    uint64_t v[4];
                    v[0] = 0;
                    v[1] = 0;
                    v[2] = 0;
                    v[3] = 0;
                    for (uint32_t j = 0; j < col; j++) {
                        enum VdbTokenType type = vdbreader_next_type(&r);
                        if (type == VDBT_TYPE_STR) {
                            char* s = vdbreader_next_string(&r);
                            free(s);
                        } else {
                            v[j] = vdbreader_next_int(&r);
                        }
                    }
                    if (!(v[0] == v[1] && v[1] == v[2] && v[2] == v[3])) {
                        printf("inconsistent data: ");
                        printf("%ld, %ld, %ld, %ld\n", v[0], v[1], v[2], v[3]);
                    }
                }
            }
        }

        free(r.buf);
    }

    char* teardown_query = "close sol;";
    r = vdbclient_execute_query(h, teardown_query);
    free(r.buf);

    vdbclient_disconnect(h);
    free(args);
    return 0;
}

int main(int argc, char** argv) {
    int opt;
    bool set_port = false;
    bool multiple_threads = false;
    char* port_arg;
    while ((opt = getopt(argc, argv, "p:t")) != -1) {
        switch (opt) {
            case 'p':
                set_port = true;
                port_arg = optarg;
                break;
            case 't': //testing with multiple threads
                multiple_threads = true;
                break;
            default:
                printf("usage: vdbclient -p [port number] [sql file]\n");
                exit(1);
                break;
        }
    }

    if (!set_port) {
        port_arg = "3333";
    }


    if (multiple_threads) {
        //set up database
        VDBHANDLE h = vdbclient_connect("127.0.0.1", port_arg);

        char* setup_query = "create database sol;"
                            "open sol;"
                            "create table planets (id int key, aa int, bb int, cc int, dd int);"
                            "insert into planets (id, aa, bb, cc, dd) values (1, 0, 0, 0, 0);";

        struct VdbReader r = vdbclient_execute_query(h, setup_query);
        free(r.buf);

        //open 4 threads to read/write
        const int THREAD_COUNT = 8;
        thrd_t threads[THREAD_COUNT];

        for (int i = 0; i < THREAD_COUNT; i++) {
            uint8_t* args = malloc(sizeof(int) + strlen(port_arg) + 1);
            int thrd_id = i + 1;
            memcpy(args, &thrd_id, sizeof(int));
            memcpy(args + sizeof(int), port_arg, strlen(port_arg));
            args[sizeof(int) + strlen(port_arg)] = '\0';
            thrd_create(&threads[i], &fcn, args);
        }

        for (int i = 0; i < THREAD_COUNT; i++) {
            thrd_join(threads[i], NULL);
        }

        char* cleanup_query = "close sol;"
                              "drop database sol;";

        r = vdbclient_execute_query(h, cleanup_query);
        free(r.buf);

        //clean up database
        vdbclient_disconnect(h);
    } else if (optind < argc) {
        VDBHANDLE h = vdbclient_connect("127.0.0.1", port_arg);
        char* queries = load_file(argv[argc - 1]);

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
        vdbclient_disconnect(h);
    } else {
        VDBHANDLE h = vdbclient_connect("127.0.0.1", port_arg);
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
        vdbclient_disconnect(h);
    }


    return 0;
}
