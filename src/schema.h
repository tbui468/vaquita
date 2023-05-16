#ifndef VDB_SCHEMA_H
#define VDB_SCHEMA_H

#include <stdint.h>
#include <stdbool.h>
#include <stdarg.h>

#include "token.h"

struct VdbSchema {
    enum VdbTokenType* types;
    char** names;
    uint32_t count;
};

struct VdbString {
    char* start;
    uint32_t len;
};

struct VdbDatum {
    enum VdbTokenType type;
    bool is_null;
    union {
        uint64_t Int;
        struct VdbString* Str;
        bool Bool;
    } as;
    uint32_t block_idx;
    uint32_t idxcell_idx;
};

struct VdbSchema* vdb_schema_alloc(int count, va_list args);
struct VdbSchema* vdbschema_alloc(int count, struct VdbTokenList* attributes, struct VdbTokenList* types);
void vdb_schema_free(struct VdbSchema* schema);
struct VdbSchema* vdb_schema_copy(struct VdbSchema* schema);
void vdb_schema_serialize(uint8_t* buf, struct VdbSchema* schema, int* off);
struct VdbSchema* vdb_schema_deserialize(uint8_t* buf, int* off);

#endif //VDB_SCHEMA_H
