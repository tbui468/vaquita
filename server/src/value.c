#include <string.h>
#include <assert.h>
#include <stdarg.h>

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

struct VdbString vdbstring_init(char* value) {
    struct VdbString s;
    s.start = malloc_w(sizeof(char) * strlen(value));
    s.len = strlen(value);
    memcpy(s.start, value, s.len);
    return s;
}

void vdbstring_concat(struct VdbString* s, const char* fmt, ...) {
    char buf[1024]; 
    va_list ap;
    va_start(ap, fmt);
    vsnprintf_w(buf, 1024, fmt, ap);
    va_end(ap);

    int new_len = s->len + strlen(buf);
    s->start = realloc_w(s->start, sizeof(char) * new_len, sizeof(char) * s->len);
    memcpy(s->start + s->len, buf, strlen(buf));
    s->len = new_len;
}

struct VdbValue vdbvalue_copy(struct VdbValue v) {
    if (v.type == VDBT_TYPE_STR) {
        return vdbvalue_init_string(v.as.Str.start, v.as.Str.len);
    }

    return v;
}

int vdbvalue_compare(struct VdbValue v1, struct VdbValue v2) {
    assert(v1.type == v2.type && "value types must be the same to compared");
    switch (v1.type) {
        case VDBT_TYPE_STR:
            return strncmp(v1.as.Str.start, v2.as.Str.start, v1.as.Str.len > v2.as.Str.len ? v1.as.Str.len : v2.as.Str.len);
        case VDBT_TYPE_INT:
            if (v1.as.Int < v2.as.Int)
                return -1;
            if (v1.as.Int > v2.as.Int)
                return 1;
            break;
        case VDBT_TYPE_FLOAT:
            if (v1.as.Float < v2.as.Float)
                return -1;
            if (v1.as.Float > v2.as.Float)
                return 1;
            break;
        case VDBT_TYPE_BOOL:
            if (v1.as.Bool < v2.as.Bool)
                return -1;
            if (v1.as.Bool > v2.as.Bool)
                return 1;
            break;
        case VDBT_TYPE_NULL:
            break;
        default:
            assert(false && "invalid value type");
    }

    return 0;
}

void vdbvalue_free(struct VdbValue v) {
    if (v.type == VDBT_TYPE_STR) {
        free_w(v.as.Str.start, sizeof(char) * v.as.Str.len);
    }
}

void vdbvalue_to_bytes(struct VdbByteList* bl, struct VdbValue v) {
    switch (v.type) {
        case VDBT_TYPE_STR: {
            for (uint32_t j = 0; j < v.as.Str.len; j++) {
                vdbbytelist_append_byte(bl, *(v.as.Str.start + j));
            }
            break;
        }
        case VDBT_TYPE_INT: {
            uint8_t* ptr = (uint8_t*)(&v.as.Int);
            for (uint32_t j = 0; j < sizeof(uint64_t); j++) {
                vdbbytelist_append_byte(bl, *(ptr + j));
            }
            break;
        }
        case VDBT_TYPE_FLOAT: {
            uint8_t* ptr = (uint8_t*)(&v.as.Float);
            for (uint32_t j = 0; j < sizeof(double); j++) {
                vdbbytelist_append_byte(bl, *(ptr + j));
            }
            break;
        }
        case VDBT_TYPE_BOOL: {
            uint8_t* ptr = (uint8_t*)(&v.as.Bool);
            for (uint32_t j = 0; j < sizeof(bool); j++) {
                vdbbytelist_append_byte(bl, *(ptr + j));
            }
            break;
        }
        case VDBT_TYPE_NULL:
            vdbbytelist_append_byte(bl, 0);
            break;
        default:
            assert(false && "invalid data type");
            break;
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
