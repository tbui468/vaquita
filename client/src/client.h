#ifndef VDB_CLIENT_H
#define VDB_CLIENT_H

typedef void* VDBHANDLE;

#define MAXDATASIZE 1024

#if defined _WIN32 || _WIN64

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>

struct VdbClient {
    SOCKET sockfd;
};

VDBHANDLE vdbclient_connect(const char* hostname, const char* port) {
    struct VdbClient* c = malloc(sizeof(struct VdbClient));

    WSADATA  wsa_data;

    if (WSAStartup(MAKEWORD(2,2),&wsa_data) != 0) {
       exit(1);
    }

    if (LOBYTE(wsa_data.wVersion) < 2 || HIBYTE(wsa_data.wVersion) < 2) {
       exit(1);
    }

    struct addrinfo hints;
    memset(&hints,0,sizeof(hints));

    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo* servinfo;
    if (getaddrinfo(hostname, port, &hints, &servinfo) != 0) {
       exit(1);
    }

    struct addrinfo* p;
    for (p = servinfo; p != NULL; p = p->ai_next) {
      c->sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
      if (c->sockfd == INVALID_SOCKET)
         continue;

      if (connect(c->sockfd, p->ai_addr, (int)p->ai_addrlen) == SOCKET_ERROR) {
         closesocket(c->sockfd);
         continue;
      }

      break;
    }

    if (p == NULL) {
       exit(1);
    }

    freeaddrinfo(servinfo);

    return (VDBHANDLE)c;
}

void vdbclient_disconnect(VDBHANDLE h) {
    struct VdbClient* c = (struct VdbClient*)h;
    closesocket(c->sockfd);
    WSACleanup();
    free(c);
}


#else

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

struct VdbClient {
    int sockfd;
};

VDBHANDLE vdbclient_connect(const char* hostname, const char* port) {
    struct VdbClient* c = malloc(sizeof(struct VdbClient));

    struct addrinfo hints;
    struct addrinfo* servinfo;
    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    if (getaddrinfo(hostname, port, &hints, &servinfo) != 0)
        return NULL;

    //connect to first result possible
    struct addrinfo* p;

    for (p = servinfo; p != NULL; p = p->ai_next) {
        if ((c->sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1)
            continue;

        if (connect(c->sockfd, p->ai_addr, p->ai_addrlen) == -1) {
            close(c->sockfd);
            continue;
        }

        break;
    }

    if (p == NULL)
        return NULL;

    freeaddrinfo(servinfo);

    return (VDBHANDLE)c;
}

void vdbclient_disconnect(VDBHANDLE h) {
    struct VdbClient* c = (struct VdbClient*)h;
    close(c->sockfd);
    free(c);
}

#endif // defined (__WIN32__)

void vdbclient_execute_query(VDBHANDLE h, char* request, char* response) {
    struct VdbClient* c = (struct VdbClient*)h;

    if (send(c->sockfd, request, strlen(request), 0) == -1) {
        //deal with error
    }
    
    int numbytes;
    if ((numbytes = recv(c->sockfd, response, MAXDATASIZE -1, 0)) == -1)
        exit(1);

    response[numbytes] = '\0';
}


#endif //VDB_CLIENT_H
