#ifndef VDB_UTIL_H
#define VDB_UTIL_H

#include <stddef.h>
#include <stdio.h>
#include <dirent.h>

void err_quit(const char* msg);

int get_filename(FILE* f, char* buf, ssize_t max_len);
int get_pathname(FILE* f, char* buf, ssize_t max_len);
int fileno_w(FILE* f);
int read_lock(FILE* f, short whence, off_t start, off_t len);
int write_lock(FILE* f, short whence, off_t start, off_t len);
int unlock(FILE* f, short whence, off_t start, off_t len);
size_t fread_w(void* ptr, size_t size, size_t count, FILE* f);
size_t fwrite_w(void* ptr, size_t size, size_t count, FILE* f);
int fseek_w(FILE* f, long offset, int whence);
long ftell_w(FILE* f);
FILE* fopen_w(const char* filename, const char* mode);
int fclose_w(FILE* f);
void* calloc_w(size_t count, size_t size);
void* malloc_w(size_t size);
int remove_w(const char* pathname);
int mkdir_w(const char* pathname, int mode);

DIR* opendir_w(const char* name);
int closedir_w(DIR* d);
struct dirent* readdir_w(DIR* d);

int join_path(char* result, ...);

#endif //VDB_UTIL_H
