#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <stdlib.h>

//network includes
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <errno.h>

#define PORT "3333"
#define BACKLOG 10
//end network stuff

#include "vdb.h"
#include "lexer.h"
#include "parser.h"
#include "vm.h"


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

bool execute_query(VDBHANDLE* h, char* query, struct VdbString* output) {
    struct VdbTokenList* tokens;
    struct VdbErrorList* lex_errors;

    if (vdblexer_lex(query, &tokens, &lex_errors) == VDBRC_ERROR) {
        for (int i = 0; i < 1; i++) {
            struct VdbError e = lex_errors->errors[i];
            vdbstring_concat(output, "error [%d]: %s\n", e.line, e.msg);
        }
        vdbtokenlist_free(tokens);
        vdberrorlist_free(lex_errors);
        return true;
    }

    struct VdbStmtList* stmts;
    struct VdbErrorList* parse_errors;

    if (vdbparser_parse(tokens, &stmts, &parse_errors) == VDBRC_ERROR) {
        for (int i = 0; i < 1; i++) {
            struct VdbError e = parse_errors->errors[i];
            vdbstring_concat(output, "error [%d]: %s\n", e.line, e.msg);
        }
        vdbtokenlist_free(tokens);
        vdberrorlist_free(lex_errors);
        vdbstmtlist_free(stmts);
        vdberrorlist_free(parse_errors);
        return true;
    }

    bool end = vdb_execute(stmts, h, output);

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

int serve() {
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE; //use local ip address

    struct addrinfo* servinfo;
    if (getaddrinfo(NULL, PORT, &hints, &servinfo) != 0) {
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

    struct sigaction sa;
    sa.sa_handler = sigchld_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    if (sigaction(SIGCHLD, &sa, NULL) == -1) {
        exit(1);
    }

    //main accept loop
    socklen_t sin_size;
    struct sockaddr_storage their_addr;
    int new_fd;
    char s[INET6_ADDRSTRLEN];

    while (true) {
        sin_size = sizeof(struct sockaddr_storage);
        new_fd = accept(sockfd, (struct sockaddr*)&their_addr, &sin_size);
        if (new_fd == -1)
            continue;

        //inet_ntop(their_addr.ss_family, get_in_addr((struct sockaddr*)&their_addr), s, sizeof(s));

        if (!fork()) {
            //this is child process
            close(sockfd); //child doesn't need listener

            char buf[1000];
            VDBHANDLE h = NULL;

            //struct VdbString s = vdbstring_init("connected to localhost:3333\n");
            struct VdbString s;
            s.start = NULL;
            s.len = 0;
            while (true) {
                int32_t request_len;
                if (!vdbserver_recv(new_fd, (char*)&request_len, sizeof(int32_t))) {
                    printf("client disconnected\n");
                    break;
                }
                if (!vdbserver_recv(new_fd, buf, request_len)) {
                    printf("client disconnected\n");
                    break;
                }
                buf[request_len] = '\0';

                bool end = execute_query(&h, buf, &s);

                int32_t res_len = s.len;
                vdbserver_send(new_fd, (char*)&res_len, sizeof(int32_t));
                vdbserver_send(new_fd, s.start, res_len);

                free_w(s.start, s.len);
                s.start = NULL;
                s.len = 0;

                if (end) {
                    printf("client released database handle\n");
                    break;
                }
            }
            free_w(s.start, s.len);

            close(new_fd);
            exit(0);
        }

        //parent doesn't need this
        close(new_fd);
    } 

    return 0;
}

int main(int argc, char** argv) {
    if (argc > 1) {
        printf("usage: vdb\n");
    } else {
        serve();
    }

    return 0;
}
