#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <threads.h>

//network includes
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <errno.h>

#define BACKLOG 10
//end network stuff

#include "lexer.h"
#include "parser.h"
#include "vm.h"

VDBHANDLE h;

void vdbserver_send(int sockfd, char* buf, int len) {
    ssize_t written = 0;
    ssize_t n;

    while (written < len) {
        if ((n = send(sockfd, buf + written, len - written, 0)) <= 0) {
            if (n < 0 && errno == EINTR) //interrupted but not error, so we need to try again
                n = 0;
            else {
                exit(1); //real error
            }
        }

        written += n;
    }
}

bool vdbserver_recv(int sockfd, char* buf, int len) {
    ssize_t nread = 0;
    ssize_t n;
    while (nread < len) {
        if ((n = recv(sockfd, buf + nread, len - nread, 0)) < 0) {
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

//TODO: this should be in the vm
bool execute_query(VDBHANDLE* h, char* query, struct VdbByteList* output) {
    struct VdbTokenList* tokens;
    struct VdbErrorList* lex_errors;

    time_t start_time, end_time;
    time(&start_time);

    if (vdblexer_lex(query, &tokens, &lex_errors) == VDBRC_ERROR) {
        for (int i = 0; i < 1; i++) {
            //struct VdbError e = lex_errors->errors[i];
            //vdbstring_concat(output, "error [%d]: %s\n", e.line, e.msg);
        }
        vdbtokenlist_free(tokens);
        vdberrorlist_free(lex_errors);
        printf("lex error\n");
        return false;
    }

    time(&end_time);
    //printf("tokenizing time: %.2lf\n", difftime(end_time, start_time));
    time(&start_time);

    struct VdbStmtList* stmts;
    struct VdbErrorList* parse_errors;

    if (vdbparser_parse(tokens, &stmts, &parse_errors) == VDBRC_ERROR) {
        for (int i = 0; i < 1; i++) {
            //struct VdbError e = parse_errors->errors[i];
            //vdbstring_concat(output, "error [%d]: %s\n", e.line, e.msg);
        }
        vdbtokenlist_free(tokens);
        vdberrorlist_free(lex_errors);
        vdbstmtlist_free(stmts);
        vdberrorlist_free(parse_errors);
        printf("parse error\n");
        return false;
    }

    time(&end_time);
    //printf("parsing time: %.2lf\n", difftime(end_time, start_time));
    time(&start_time);

    bool end = vdb_execute(stmts, h, output);

    time(&end_time);
    //printf("executing time: %.2lf\n", difftime(end_time, start_time));

    vdbtokenlist_free(tokens);
    vdberrorlist_free(lex_errors);
    vdbstmtlist_free(stmts);
    vdberrorlist_free(parse_errors);

    if (end) {
        return true;
    }

    return false;
}


void sigchld_handler(int s) {
    s = s; //silence warning

    int saved_errno = errno;

    while (waitpid(-1, NULL, WNOHANG) > 0);

    errno = saved_errno;
}

void* get_in_addr(struct sockaddr* sa) {
    //ipv4
    if (sa->sa_family == AF_INET) {
        return &(((struct sockaddr_in*)sa)->sin_addr);
    } 

    //ipv6
    return &(((struct sockaddr_in6*)sa)->sin6_addr);
}

int vdbtcp_listen(const char* port) {
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE; //use local ip address

    struct addrinfo* servinfo;
    if (getaddrinfo(NULL, port, &hints, &servinfo) != 0) {
        return 1;
    }

    //loop through results for getaddrinfo and bind to first one we can
    struct addrinfo* p;
    int sockfd;
    int yes = 1;
    for (p = servinfo; p != NULL; p = p->ai_next) {
        if ((sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1)
            continue;

        if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1)
            exit(1);

        if (bind(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
            close(sockfd);
            continue;
        }

        break;
    }

    freeaddrinfo(servinfo);

    //failed to bind
    if (p == NULL) {
        exit(1);
    }

    if (listen(sockfd, BACKLOG) == -1) {
        exit(1);
    }

    //what was this for again?
    struct sigaction sa;
    sa.sa_handler = sigchld_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    if (sigaction(SIGCHLD, &sa, NULL) == -1) {
        exit(1);
    }

    return sockfd;
}

int vdbtcp_accept(int listenerfd) {
    socklen_t sin_size = sizeof(struct sockaddr_storage);
    struct sockaddr_storage their_addr;
    int new_fd = accept(listenerfd, (struct sockaddr*)&their_addr, &sin_size);

    return new_fd;
}

void vdbtcp_handle_client(int conn_fd) {

    while (true) {
        int32_t request_len;
        if (!vdbserver_recv(conn_fd, (char*)&request_len, sizeof(int32_t))) {
            printf("client disconnected\n");
            break;
        }
        char buf[request_len + 1];
        if (!vdbserver_recv(conn_fd, buf, request_len)) {
            printf("client disconnected\n");
            break;
        }
        buf[request_len] = '\0';

        struct VdbByteList* response_buf = vdbbytelist_init();
        uint32_t count = response_buf->count;
        vdbbytelist_append_bytes(response_buf, (uint8_t*)&count, sizeof(uint32_t)); //saving space for length

        bool end = execute_query(&h, buf, response_buf);
        *((uint32_t*)(response_buf->values)) = (uint32_t)(response_buf->count); //filling in bytelist length

        vdbserver_send(conn_fd, (char*)(response_buf->values), sizeof(uint32_t));
        vdbserver_send(conn_fd, (char*)(response_buf->values + sizeof(uint32_t)), response_buf->count - sizeof(uint32_t));

        if (end) {
            //printf("client released database handle\n");
            break;
        }
    }
}

int vdbtcp_handle_client_thread(void* args) {
    thrd_t t = thrd_current();
    thrd_detach(t);
    int fd = *((int*)args);
    free_w(args, sizeof(int));

    vdbtcp_handle_client(fd);

    close(fd);

    return 0;
}

int vdbtcp_serve(const char* port) {

    int listener_fd = vdbtcp_listen(port);

    h = NULL;

    //main accept loop
    while (true) {
        int conn_fd = vdbtcp_accept(listener_fd);
        if (conn_fd == -1)
            continue;


        int* ptr = malloc_w(sizeof(int));
        *ptr = conn_fd;

        thrd_t t;
        thrd_create(&t, &vdbtcp_handle_client_thread, ptr);
    } 

    return 0;
}

int main(int argc, char** argv) {
    int opt;
    bool set_port = false;
    char* port_arg;
    while ((opt = getopt(argc, argv, "p:")) != -1) {
        switch (opt) {
            case 'p':
                set_port = true;
                port_arg = optarg;
                break;
            default:
                printf("usage: vdb -p [port number]\n");
                exit(1);
                break;
        }
    }

    if (!set_port) {
        port_arg = "3333";
    }
    vdbtcp_serve(port_arg);

    return 0;
}
