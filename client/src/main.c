#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include "client.h"


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
    if (argc > 1) {
        int sockfd = connect_to_server("localhost", "3333");

        char buf[MAXDATASIZE];

        char* queries = load_file(argv[1]);

        send_request(sockfd, queries, buf);
        printf("%s", buf);

        free(queries);

        disconnect_from_server(sockfd);
    } else if (argc == 1) {
        int sockfd = connect_to_server("localhost", "3333");
        char buf[MAXDATASIZE];

        char* line = NULL; //not including this in memory allocation tracker
        size_t len = 0;
        ssize_t nread;

        while (true) {
            printf(" > ");
            nread = getline(&line, &len, stdin);
            if (nread == -1)
                break;
            line[strlen(line) - 1] = '\0'; //get rid of newline

            send_request(sockfd, line, buf); //this needs to loop
            if (strncmp(buf, "disconnecting", strlen("disconnecting")) == 0) {
                break;
            }
            printf("%s", buf);
        }

        free(line);

        disconnect_from_server(sockfd);
    }



    return 0;
}
