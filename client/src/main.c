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

        printf("%s", buf);
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
