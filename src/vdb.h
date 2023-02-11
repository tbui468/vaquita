#ifndef VDB_H
#define VDB_H

#include <stdbool.h>
#include <stdint.h>

typedef void* VDBHANDLE;

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

struct VdbData {
    struct VdbDatum* data;
    uint32_t count;
};

VDBHANDLE vdb_open(const char* name);
void vdb_close(VDBHANDLE);
int vdb_create_table(VDBHANDLE h, const char* table_name, struct VdbSchema schema);
int vdb_drop_table(VDBHANDLE h, const char* table_name);
int vdb_insert_record(VDBHANDLE h, const char* table, struct VdbData data);
struct VdbData* vdb_fetch_record(VDBHANDLE h, const char* table, uint32_t key);

#endif //VDB_H
