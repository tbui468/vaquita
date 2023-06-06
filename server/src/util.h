#ifndef VDB_UTIL_H
#define VDB_UTIL_H

#include <stddef.h>
#include <stdio.h>
#include <dirent.h>
#include <stdint.h>

extern uint64_t allocated_memory;

struct VdbIntList {
    int* values;
    int count;
    int capacity;
};

struct VdbByteList {
    uint8_t* values;
    int count;
    int capacity;
};

struct VdbIntList* vdbintlist_init();
void vdbintlist_free(struct VdbIntList* il);
void vdbintlist_append_int(struct VdbIntList* il, int value);

struct VdbByteList* vdbbytelist_init();
void vdbbytelist_free(struct VdbByteList* bl);
void vdbbytelist_append_byte(struct VdbByteList* bl, uint8_t byte);

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
void* realloc_w(void* ptr, size_t new_size, size_t prev_size);
void free_w(void* ptr, size_t size);
int remove_w(const char* pathname);
int rmdir_w(const char* pathname);
int mkdir_w(const char* pathname, int mode);
DIR* opendir_w(const char* name);
int closedir_w(DIR* d);
struct dirent* readdir_w(DIR* d);
char* strdup_w(const char* s);
int vsnprintf_w(char* s, size_t size, const char* fmt, va_list ap);

//error handling
void err_quit(const char* msg);

//utility
void read_u32(uint32_t* dst, uint8_t* buf, int* off);
void write_u32(uint8_t* dst, uint32_t v, int* off);

#endif //VDB_UTIL_H
