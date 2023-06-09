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
#include <stdint.h>
#include <stdbool.h>
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <BaseTsd.h>
typedef SSIZE_T ssize_t;

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

void vdbclient_send(struct VdbClient* c, char* buf, int len) {
    ssize_t written = 0;
    ssize_t n;

    while (written < len) {
        if ((n = send(c->sockfd, buf + written, len - written, 0)) <= 0) {
            if (n < 0 && errno == EINTR) //interrupted but not error, so we need to try again
                n = 0;
            else {
                exit(1); //real error
            }
        }

        written += n;
    }
}

bool vdbclient_recv(struct VdbClient* c, char* buf, int len) {
    ssize_t nread = 0;
    ssize_t n;
    while (nread < len) {
        if ((n = recv(c->sockfd, buf + nread, len - nread, 0)) < 0) {
            if (n < 0 && errno == EINTR)
                n = 0;
            else
                exit(1);
        } else if (n == 0) {
            //connection ended
            return false;
        }

        nread += n;
    }

    return true;
}

char* vdbclient_execute_query(VDBHANDLE h, char* request) {
    struct VdbClient* c = (struct VdbClient*)h;

    int32_t l = strlen(request);
    vdbclient_send(c, (char*)&l, sizeof(int32_t));
    vdbclient_send(c, request, l);

    int32_t recv_len;
    if (!vdbclient_recv(c, (char*)&recv_len, sizeof(int32_t)))
        return NULL;

    char* buf = malloc(sizeof(char) * (recv_len + 1));
    if (!buf) {
        printf("malloc failed\n");
        exit(1);
    }

    if (!vdbclient_recv(c, buf, recv_len)) {
        free(buf);
        return false;
    }
    
    buf[recv_len] = '\0';

    return buf;
}


#endif //VDB_CLIENT_H
