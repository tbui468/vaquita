#ifndef VDB_VALUE_H
#define VDB_VALUE_H

#include <stdbool.h>
#include <stdint.h>

#include "token.h"
#include "util.h"

struct VdbString {
    char* start;
    uint32_t len;
};

struct VdbValue {
    enum VdbTokenType type;
    union {
        int64_t Int;
        struct VdbString Str;
        bool Bool;
        double Float;
    } as;
    uint32_t block_idx;
    uint32_t idxcell_idx;
};

struct VdbValueList {
    struct VdbValue* values;
    int capacity;
    int count;
};


struct VdbValue vdbvalue_deserialize_string(uint8_t* buf);
bool vdbvalue_is_null(struct VdbValue* d);
struct VdbValue vdbvalue_init_string(char* start, int len);
struct VdbValue vdbvalue_copy(struct VdbValue v);
int vdbvalue_compare(struct VdbValue v1, struct VdbValue v2);
void vdbvalue_free(struct VdbValue v);
void vdbvalue_to_bytes(struct VdbByteList* bl, struct VdbValue v);

struct VdbValueList * vdbvaluelist_init();
void vdbvaluelist_free(struct VdbValueList* vl);
void vdbvaluelist_append_value(struct VdbValueList* vl, struct VdbValue v);

#endif //VDB_VALUE_H
