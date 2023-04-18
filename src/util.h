#ifndef VDB_UTIL_H
#define VDB_UTIL_H

#include <stddef.h>
#include <stdio.h>
#include <dirent.h>
#include <stdint.h>

#define flow(msg) printf("%s - %d\n", __func__, msg)

struct U32List {
    uint32_t* values;
    uint32_t count;
    uint32_t capacity;
};
struct U32List* u32l_alloc();
void u32l_append(struct U32List* list, uint32_t v);
void u32l_free(struct U32List* list);

//wrappers
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
void* realloc_w(void* ptr, size_t size);
int remove_w(const char* pathname);
int mkdir_w(const char* pathname, int mode);
DIR* opendir_w(const char* name);
int closedir_w(DIR* d);
struct dirent* readdir_w(DIR* d);
char* strdup_w(const char* s);

//error handling
void err_quit(const char* msg);

//utility
int join_path(char* result, ...);
void read_u32(uint32_t* dst, uint8_t* buf, int* off);
void write_u32(uint8_t* dst, uint32_t v, int* off);

#endif //VDB_UTIL_H
