#ifndef VDB_CLIENT_H
#define VDB_CLIENT_H

#define MAXDATASIZE 1024
int connect_to_server(const char* hostname, const char* port);
int disconnect_from_server(int sockfd);
void send_request(int sockfd, char* request, char* response);

#endif //VDB_CLIENT_H
