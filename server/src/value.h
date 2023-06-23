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
};

struct VdbValueList {
    struct VdbValue* values;
    int capacity;
    int count;
};

int vdbvalue_serialized_size(struct VdbValue v);
int vdbvalue_serialize(uint8_t* buf, struct VdbValue v);
int vdbvalue_deserialize(struct VdbValue* v, uint8_t* buf);

bool vdbvalue_is_null(struct VdbValue* d);
struct VdbValue vdbvalue_copy(struct VdbValue v);
int vdbvalue_compare(struct VdbValue v1, struct VdbValue v2);
void vdbvalue_free(struct VdbValue v);
struct VdbValue vdbint(int64_t i);
struct VdbValue vdbfloat(double d);
struct VdbValue vdbstring(char* s, int len);
struct VdbValue vdbbool(bool b);

struct VdbValueList * vdbvaluelist_init();
void vdbvaluelist_free(struct VdbValueList* vl);
void vdbvaluelist_append_value(struct VdbValueList* vl, struct VdbValue v);

#endif //VDB_VALUE_H
