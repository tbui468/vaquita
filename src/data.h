#ifndef VDB_data_H
#define VDB_data_H

#include <stdint.h>
#include <stdbool.h>

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

struct VdbRecord {
    struct VdbDatum* data;
    uint32_t count;
    uint32_t key;
};

struct VdbSchema* vdbdata_alloc_schema(int count, ...);
void vdbdata_free_schema(struct VdbSchema* schema);
struct VdbSchema* vdbdata_copy_schema(struct VdbSchema* schema);

struct VdbRecord* vdbdata_alloc_record(struct VdbSchema* schema, ...);
void vdbdata_free_record(struct VdbRecord* rec);

#endif //VDB_RECORD_H
