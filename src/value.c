#include <string.h>

#include "value.h"
#include "util.h"

struct VdbValue vdbvalue_deserialize_string(uint8_t* buf) {
    struct VdbValue d;
    d.type = VDBT_TYPE_STR;
    d.block_idx = *((uint32_t*)(buf + sizeof(uint32_t) * 0));
    d.idxcell_idx = *((uint32_t*)(buf + sizeof(uint32_t) * 1));

    d.as.Str.len = *((uint32_t*)(buf + sizeof(uint32_t) * 2));
    d.as.Str.start = malloc_w(sizeof(char) * d.as.Str.len);
    memcpy(d.as.Str.start, buf + sizeof(uint32_t) * 3, d.as.Str.len);
    return d;
}

bool vdbvalue_is_null(struct VdbValue* d) {
    return d->type == VDBT_TYPE_NULL;
}

struct VdbValue vdbvalue_init_string(char* start, int len) {
    struct VdbValue v;
    v.type = VDBT_TYPE_STR;
    v.as.Str.len = len;
    v.as.Str.start = malloc_w(sizeof(char) * len);
    memcpy(v.as.Str.start, start, len);
    return v;
}

struct VdbValue vdbvalue_copy(struct VdbValue v) {
    if (v.type == VDBT_TYPE_STR) {
        return vdbvalue_init_string(v.as.Str.start, v.as.Str.len);
    }

    return v;
}

void vdbvalue_free(struct VdbValue v) {
    if (v.type == VDBT_TYPE_STR) {
        free_w(v.as.Str.start, sizeof(char) * v.as.Str.len);
    }
}

struct VdbValueList * vdbvaluelist_init() {
    struct VdbValueList* vl = malloc_w(sizeof(struct VdbValueList));
    vl->count = 0;
    vl->capacity = 8;
    vl->values = malloc_w(sizeof(struct VdbValue) * vl->capacity);
    return vl;
}

void vdbvaluelist_free(struct VdbValueList* vl) {
    for (int i = 0; i < vl->count; i++) {
        if (vl->values[i].type == VDBT_TYPE_STR) {
            free_w(vl->values[i].as.Str.start, vl->values[i].as.Str.len * sizeof(char));
        }
    }

    free_w(vl->values, sizeof(struct VdbValue) * vl->capacity);
    free_w(vl, sizeof(struct VdbValueList));
}

void vdbvaluelist_append_value(struct VdbValueList* vl, struct VdbValue v) {
    if (vl->count == vl->capacity) {
        uint32_t old_cap = vl->capacity;
        vl->capacity *= 2;
        vl->values = realloc_w(vl->values, sizeof(struct VdbValue) * vl->capacity, sizeof(struct VdbValue) * old_cap);
    }

    vl->values[vl->count++] = v;
}
