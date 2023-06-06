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
            if (send(new_fd, "Hello, world!\n", 14, 0) == -1) {
                //deal with error
            }
            close(new_fd);
            exit(0);
        }

        //parent doesn't need this
        close(new_fd);
    } 

    return 0;
}

void run_cli() {
    char* line = NULL; //not including this in memory allocation tracker
    size_t len = 0;
    ssize_t nread;
    VDBHANDLE h = NULL;

    while (true) {
        printf("vdb");
        if (h) {
            printf(":%s", vdb_dbname(h));
        }
        printf(" > ");
        nread = getline(&line, &len, stdin);
        if (nread == -1)
            break;
        line[strlen(line) - 1] = '\0'; //get rid of newline

        struct VdbTokenList* tokens;
        struct VdbErrorList* lex_errors;

        if (vdblexer_lex(line, &tokens, &lex_errors) == VDBRC_ERROR) {
            for (int i = 0; i < 1; i++) {
                struct VdbError e = lex_errors->errors[i];
                printf("error [%d]: %s\n", e.line, e.msg);
            }
            vdbtokenlist_free(tokens);
            vdberrorlist_free(lex_errors);
            continue;
        }

        struct VdbStmtList* stmts;
        struct VdbErrorList* parse_errors;

        if (vdbparser_parse(tokens, &stmts, &parse_errors) == VDBRC_ERROR) {
            for (int i = 0; i < 1; i++) {
                struct VdbError e = parse_errors->errors[i];
                printf("error [%d]: %s\n", e.line, e.msg);
            }
            vdbtokenlist_free(tokens);
            vdberrorlist_free(lex_errors);
            vdbstmtlist_free(stmts);
            vdberrorlist_free(parse_errors);
            continue;
        }

        bool end = vdb_execute(stmts, &h);

        vdbtokenlist_free(tokens);
        vdberrorlist_free(lex_errors);
        vdbstmtlist_free(stmts);
        vdberrorlist_free(parse_errors);

        if (end) {
            break;
        }
    }

    free(line); //not including this in memory allocation tracker
}

int run_script(const char* path) {
    FILE* f = fopen_w(path, "r");
    VDBHANDLE h = NULL;

    fseek_w(f, 0, SEEK_END);
    long fsize = ftell_w(f);
    fseek_w(f, 0, SEEK_SET);
    char* buf = malloc_w(sizeof(char) * (fsize)); //will put null terminator on eof character
    fread_w(buf, fsize, sizeof(char), f);
    buf[fsize - 1] = '\0';

    fclose_w(f);

    struct VdbTokenList* tokens;
    struct VdbErrorList* lex_errors;

    if (vdblexer_lex(buf, &tokens, &lex_errors) == VDBRC_ERROR) {
        for (int i = 0; i < 1; i++) {
            struct VdbError e = lex_errors->errors[i];
            printf("error [%d]: %s\n", e.line, e.msg);
        }
        vdbtokenlist_free(tokens);
        vdberrorlist_free(lex_errors);
        free_w(buf, sizeof(char) * fsize);
        return -1;
    }

//    vdbtokenlist_print(tokens);

    struct VdbStmtList* stmts;
    struct VdbErrorList* parse_errors;

    if (vdbparser_parse(tokens, &stmts, &parse_errors) == VDBRC_ERROR) {
        for (int i = 0; i < 1; i++) {
            struct VdbError e = parse_errors->errors[i];
            printf("error [%d]: %s\n", e.line, e.msg);
        }
        vdbtokenlist_free(tokens);
        vdberrorlist_free(lex_errors);
        vdbstmtlist_free(stmts);
        vdberrorlist_free(parse_errors);
        free_w(buf, sizeof(char) * fsize);
        return -1;
    }

    //TODO: uncomment later 
    /*
    for (int i = 0; i < stmts->count; i++) {
        vdbstmt_print(&stmts->stmts[i]);
    }*/

    vdb_execute(stmts, &h);

    vdbtokenlist_free(tokens);
    vdberrorlist_free(lex_errors);
    vdbstmtlist_free(stmts);
    vdberrorlist_free(parse_errors);
    free_w(buf, sizeof(char) * fsize);

    return 0;
}

int main(int argc, char** argv) {
    if (argc > 1) {
        int result = run_script(argv[1]);
//        printf("allocated memory: %ld\n", allocated_memory);
        return result;
    } else {
        serve();
        //run_cli();
    }



    return 0;
}
