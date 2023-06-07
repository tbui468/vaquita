#include "client.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <netdb.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>


static void* get_in_addr(struct sockaddr* sa) {
    if (sa->sa_family == AF_INET) {
        return &(((struct sockaddr_in*)sa)->sin_addr);
    }

    return &(((struct sockaddr_in6*)sa)->sin6_addr);
}

void send_request(int sockfd, char* request, char* response) {
    if (send(sockfd, request, strlen(request), 0) == -1) {
        //deal with error
    }
    
    int numbytes;
    if ((numbytes = recv(sockfd, response, MAXDATASIZE -1, 0)) == -1)
        exit(1);

    response[numbytes] = '\0';
}

int connect_to_server(const char* hostname, const char* port) {
    struct addrinfo hints;
    struct addrinfo* servinfo;
    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    if (getaddrinfo(hostname, port, &hints, &servinfo) != 0)
        return -1;

    //connect to first result possible
    struct addrinfo* p;
    int sockfd;

    for (p = servinfo; p != NULL; p = p->ai_next) {
        if ((sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1)
            continue;

        if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
            close(sockfd);
            continue;
        }

        break;
    }

    if (p == NULL)
        return -1;

    freeaddrinfo(servinfo);

    /*
    char buf[MAXDATASIZE];
    send_request(sockfd, "create database sol;", buf);
    printf("received: %s\n", buf);
    if ((numbytes = recv(sockfd, buf, MAXDATASIZE -1, 0)) == -1)
        exit(1);

    buf[numbytes] = '\0';
    printf("received: %s\n", buf);

    char* msg = "select * from planets;\n";
    if (send(sockfd, msg, strlen(msg), 0) == -1) {
        //deal with error
    }
    
    if ((numbytes = recv(sockfd, buf, MAXDATASIZE -1, 0)) == -1)
        exit(1);

    buf[numbytes] = '\0';
    printf("received: %s\n", buf);*/

    return sockfd;
}


int disconnect_from_server(int sockfd) {
    close(sockfd);
    return 0;
}


