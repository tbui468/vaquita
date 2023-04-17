#ifndef VDB_SCHEMA_H
#define VDB_SCHEMA_H

#include <stdint.h>
#include <stdbool.h>
#include <stdarg.h>

enum VdbField {
    VDBF_INT,
    VDBF_STR,
    VDBF_BOOL
};

struct VdbSchema {
    enum VdbField* fields;
    uint32_t count;
};

struct VdbString {
    char* start;
    uint32_t len;
};

struct VdbDatum {
    enum VdbField type;
    union {
        uint64_t Int;
        struct VdbString* Str;
        bool Bool;
    } as;
};

struct VdbSchema* vdb_schema_alloc(int count, va_list args);
void vdb_schema_free(struct VdbSchema* schema);
struct VdbSchema* vdb_schema_copy(struct VdbSchema* schema);

#endif //VDB_SCHEMA_H
