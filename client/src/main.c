#include <stdio.h>
#include "client.h"

int main() {
    int sockfd = connect_to_server("localhost", "3333");

    char buf[MAXDATASIZE];

    send_request(sockfd, "if exists drop database sol;", buf);
    printf("response 1: %s\n", buf);

    send_request(sockfd, "create database sol;", buf);
    printf("response 1: %s\n", buf);

    send_request(sockfd, "open sol;", buf);
    printf("response 2: %s\n", buf);

    send_request(sockfd, "close sol;", buf);
    printf("response 3: %s\n", buf);

    send_request(sockfd, "exit;", buf);
    printf("response 4: %s\n", buf);

    disconnect_from_server(sockfd);
    return 0;
}
