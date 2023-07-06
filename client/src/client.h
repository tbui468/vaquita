#ifndef VDB_CLIENT_H
#define VDB_CLIENT_H

typedef void* VDBHANDLE;

//TODO: shouldn't require us to copy this over from server token.h
//      this is error prone
enum VdbTokenType {
    VDBT_TYPE_STR,
    VDBT_TYPE_INT,
    VDBT_TYPE_BOOL,
    VDBT_TYPE_FLOAT,
    VDBT_TYPE_NULL
};

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


struct VdbReader {
    char* buf;
    uint32_t idx;
};


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

struct VdbReader vdbclient_execute_query(VDBHANDLE h, char* request) {
    struct VdbClient* c = (struct VdbClient*)h;

    int32_t l = strlen(request);
    vdbclient_send(c, (char*)&l, sizeof(int32_t));
    vdbclient_send(c, request, l);

    int32_t recv_len;
    if (!vdbclient_recv(c, (char*)&recv_len, sizeof(int32_t))) {
        printf("failed to recv data length\n");
        struct VdbReader r = {NULL, 0};
        return r;
    }

    char* buf = malloc(sizeof(int32_t) + sizeof(char) * recv_len);
    if (!buf) {
        printf("malloc failed\n");
        exit(1);
    }

    *((int32_t*)buf) = recv_len;

    if (!vdbclient_recv(c, buf + sizeof(int32_t), recv_len - sizeof(int32_t))) {
        printf("failed to recv data\n");
        free(buf);
        struct VdbReader r = {NULL, 0};
        return r;
    }

    struct VdbReader r = {buf, sizeof(uint32_t)}; //skip size 
    return r;
}

bool vdbreader_has_unread_bytes(struct VdbReader* r) {
    return r->idx < *((uint32_t*)(r->buf));
}

uint8_t vdbreader_next_is_tuple(struct VdbReader* r) {
    uint8_t is_tuple = *((uint8_t*)(r->buf + r->idx));
    r->idx += sizeof(uint8_t);

    return is_tuple;
}

void vdbreader_next_set_dim(struct VdbReader* r, uint32_t* row, uint32_t* col) {
    *row = *((int32_t*)(r->buf + r->idx));
    r->idx += sizeof(int32_t);

    *col = *((int32_t*)(r->buf + r->idx));
    r->idx += sizeof(int32_t);
}

enum VdbTokenType vdbreader_next_type(struct VdbReader* r) {
    enum VdbTokenType type = *((uint8_t*)(r->buf + r->idx));
    r->idx += sizeof(uint8_t);
    return type;
}

int64_t vdbreader_next_int(struct VdbReader* r) {
    int64_t i = *((int64_t*)(r->buf + r->idx));
    r->idx += sizeof(int64_t);
    return i;
}

double vdbreader_next_float(struct VdbReader* r) {
    double d = *((double*)(r->buf + r->idx));
    r->idx += sizeof(double);
    return d;
}

char* vdbreader_next_string(struct VdbReader* r) {
    int len = *((uint32_t*)(r->buf + r->idx));
    r->idx += sizeof(uint32_t);
    char* s = malloc(sizeof(char) * (len + 1));
    memcpy(s, r->buf + r->idx, len);
    s[len] = '\0';
    r->idx += len;
    return s;
}

bool vdbreader_next_bool(struct VdbReader* r) {
    bool b = *((bool*)(r->buf + r->idx));
    r->idx += sizeof(bool);
    return b;
}


#endif //VDB_CLIENT_H
