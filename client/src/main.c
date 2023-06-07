#include <stdio.h>
#include <stdlib.h>
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
    } else {
        printf("usage: vdbclient <script>\n");
        return 0;
    }



    return 0;
}
