#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdarg.h>
#include <sys/stat.h>

#include "util.h"

uint64_t allocated_memory = 0;

struct VdbIntList* vdbintlist_init() {
    struct VdbIntList* il = malloc_w(sizeof(struct VdbIntList));
    il->count = 0;
    il->capacity = 8;
    il->values = malloc_w(sizeof(int) * il->capacity);

    return il;
}

void vdbintlist_free(struct VdbIntList* il) {
    free_w(il->values, sizeof(int) * il->capacity);
    free_w(il, sizeof(struct VdbIntList));
}

void vdbintlist_append_int(struct VdbIntList* il, int value) {
    if (il->count + 1 > il->capacity) {
        int old_cap = il->capacity;
        il->capacity *= 2;
        il->values = realloc_w(il->values, sizeof(int) * il->capacity, sizeof(int) * old_cap);
    }

    il->values[il->count++] = value;
}

int get_filename(FILE* f, char* buf, ssize_t max_len) {
    get_pathname(f, buf, max_len);
    char* end = strrchr(buf, '/');
    int len = strlen(end + 1);
    memmove(buf, end + 1, len);
    buf[len] = '\0';

    return len;
}

int get_pathname(FILE* f, char* buf, ssize_t max_len) {
    int fd = fileno_w(f);
    int res;
    char filepath[FILENAME_MAX];
    if ((res = sprintf(filepath, "/proc/self/fd/%d", fd)) < 0)
        err_quit("sprintf failed");

    ssize_t len = readlink(filepath, buf, max_len);
    buf[len] = '\0';

    return len;
}

void err_quit(const char* msg) {
    perror(msg);
    exit(1);
}

int fileno_w(FILE* f) {
    int res;
    if ((res = fileno(f)) < 0)
        err_quit("fileno failed");
    return res;
}

int read_lock(FILE* f, short whence, off_t start, off_t len) {
    struct flock fl;
    fl.l_type = F_RDLCK;
    fl.l_whence = whence;
    fl.l_start = start;
    fl.l_len = len;

    int res;
    if ((res = fcntl(fileno_w(f), F_SETLKW, &fl)) < 0)
        err_quit("fcntl failed");
    return res;
}

int write_lock(FILE* f, short whence, off_t start, off_t len) {
    struct flock fl;
    fl.l_type = F_WRLCK;
    fl.l_whence = whence;
    fl.l_start = start;
    fl.l_len = len;

    int res;
    if ((res = fcntl(fileno_w(f), F_SETLKW, &fl)) < 0)
        err_quit("fcntl failed");
    return res;
}

int unlock(FILE* f, short whence, off_t start, off_t len) {
    struct flock fl;
    fl.l_type = F_UNLCK;
    fl.l_whence = whence;
    fl.l_start = start;
    fl.l_len = len;

    int res;
    if ((res = fcntl(fileno_w(f), F_SETLKW, &fl)) < 0)
        err_quit("fcntl failed");
    return res;
}

size_t fread_w(void* ptr, size_t size, size_t count, FILE* f) {
    size_t res;
    if ((res = fread(ptr, size, count, f)) != count) {
        err_quit("fread failed");
    }

    return res;
}

size_t fwrite_w(void* ptr, size_t size, size_t count, FILE* f) {
    size_t res;
    if ((res = fwrite(ptr, size, count, f)) != count)
        err_quit("fwrite failed");

    return res;
}

int fseek_w(FILE* f, long offset, int whence) {
    int res;
    if ((res = fseek(f, offset, whence)) != 0)
        err_quit("fseek failed");

    return res;
}

long ftell_w(FILE* f) {
    long res;
    if ((res = ftell(f)) == -1)
        err_quit("ftell failed");
    return res;
}

FILE* fopen_w(const char* filename, const char* mode) {
    FILE* f;
    if (!(f = fopen(filename, mode)))
        err_quit("fopen failed");
    return f;
}

int fclose_w(FILE* f) {
    int res;
    if ((res = fclose(f)) == EOF)
        err_quit("fclose failed");
    return res;
}

void* calloc_w(size_t count, size_t size) {
    allocated_memory += count * size;
    void* ptr;
    if (!(ptr = calloc(count, size)))
        err_quit("calloc failed");

    return ptr;
}

void* malloc_w(size_t size) {
    allocated_memory += size;
    void* ptr;
    if (!(ptr = malloc(size)))
        err_quit("malloc failed");
    return ptr;
}

void* realloc_w(void* ptr, size_t new_size, size_t prev_size) {
    allocated_memory += (new_size - prev_size);
    void* ret;
    if (!(ret = realloc(ptr, new_size)))
        err_quit("realloc failed");
    return ret;
}

void free_w(void* ptr, size_t size) {
    free(ptr);
    allocated_memory -= size;
}

int remove_w(const char* pathname) {
    int res;
    if ((res = remove(pathname)) == -1)
        err_quit("remove failed");
    return res;
}

int rmdir_w(const char* pathname) {
    int res;
    if ((res = rmdir(pathname)) == -1)
        err_quit("rmdir failed");
    return res;
}

int mkdir_w(const char* pathname, int mode) {
    int res;
    if ((res = mkdir(pathname, mode)) == -1)
        err_quit("mkdir failed");
    return res;
}

DIR* opendir_w(const char* name) {
    DIR* d;
    if (!(d = opendir(name)))
        err_quit("opendir failed");
    return d;
}

int closedir_w(DIR* d) {
    int res;
    if ((res = closedir(d)) == -1)
        err_quit("closedir failed");
    return res;
}

struct dirent* readdir_w(DIR* d) {
    struct dirent* dir;
    if (!(dir = readdir(d)))
        err_quit("readdir failed");
    return dir;
}

char* strdup_w(const char* s) {
    char* res;
    if (!(res = strdup(s)))
        err_quit("strdup failed");
    return res;
}

int vsnprintf_w(char* s, size_t size, const char* fmt, va_list ap) {
    int res;
    if ((res = vsnprintf(s, size, fmt, ap)) < 0)
        err_quit("vsnprintf failed");

    return res;
}

void read_u32(uint32_t* dst, uint8_t* buf, int* off) {
    *dst = *((uint32_t*)(buf + *off));
    *off += sizeof(uint32_t);
}

void write_u32(uint8_t* dst, uint32_t v, int* off) {
    *((uint32_t*)(dst + *off)) = v;
    *off += sizeof(uint32_t);
}
