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
    uint32_t block_idx;
    uint32_t offset_idx;
};

struct VdbSchema* vdb_schema_alloc(int count, va_list args);
void vdb_schema_free(struct VdbSchema* schema);
struct VdbSchema* vdb_schema_copy(struct VdbSchema* schema);
void vdb_schema_serialize(uint8_t* buf, struct VdbSchema* schema, int* off);
struct VdbSchema* vdb_schema_deserialize(uint8_t* buf, int* off);

#endif //VDB_SCHEMA_H
