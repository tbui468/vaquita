#ifndef VDB_VALUE_H
#define VDB_VALUE_H

#include <stdbool.h>
#include <stdint.h>

#include "token.h"

struct VdbString {
    char* start;
    uint32_t len;
};

struct VdbValue {
    enum VdbTokenType type;
    union {
        uint64_t Int;
        struct VdbString Str;
        bool Bool;
        double Float;
    } as;
    uint32_t block_idx;
    uint32_t idxcell_idx;
};


struct VdbValue vdbvalue_deserialize_string(uint8_t* buf);
bool vdbvalue_is_null(struct VdbValue* d);

#endif //VDB_VALUE_H
