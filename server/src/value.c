#include <string.h>
#include <assert.h>
#include <stdarg.h>

#include "value.h"
#include "util.h"


int vdbvalue_serialized_size(struct VdbValue v) {
    int size = 0;
    size += sizeof(uint8_t);

    switch (v.type) {
        case VDBT_TYPE_STR: {
            size += sizeof(uint32_t);
            size += v.as.Str.len;
            break;
        }
        case VDBT_TYPE_INT: {
            size += sizeof(int64_t);
            break;
        }
        case VDBT_TYPE_FLOAT: {
            size += sizeof(double);
            break;
        }
        case VDBT_TYPE_BOOL: {
            size += sizeof(bool);
            break;
        }
        case VDBT_TYPE_NULL:
            break;
        default:
            assert(false && "invalid data type");
            break;
    }

    return size;
}

int vdbvalue_serialize(uint8_t* buf, struct VdbValue v) {
    int off = 0;

    *((uint8_t*)(buf + off)) = (uint8_t)(v.type);
    off += sizeof(uint8_t);

    switch (v.type) {
        case VDBT_TYPE_STR: {
            *((uint32_t*)(buf + off)) = (uint32_t)(v.as.Str.len);
            off += sizeof(uint32_t);
            memcpy(buf + off, v.as.Str.start, v.as.Str.len);
            off += v.as.Str.len;
            break;
        }
        case VDBT_TYPE_INT: {
            memcpy(buf + off, &v.as.Int, sizeof(int64_t));
            off += sizeof(int64_t);
            break;
        }
        case VDBT_TYPE_FLOAT: {
            memcpy(buf + off, &v.as.Float, sizeof(double));
            off += sizeof(double);
            break;
        }
        case VDBT_TYPE_BOOL: {
            memcpy(buf + off, &v.as.Bool, sizeof(bool));
            off += sizeof(bool);
            break;
        }
        case VDBT_TYPE_NULL:
            break;
        default:
            assert(false && "invalid data type");
            break;
    }

    return off;
}

int vdbvalue_deserialize(struct VdbValue* v, uint8_t* buf) {
    int off = 0;
    v->type = (enum VdbTokenType)(*((uint8_t*)(buf + off)));
    off += sizeof(uint8_t);

    switch (v->type) {
        case VDBT_TYPE_STR: {
            v->as.Str.len = *((uint32_t*)(buf + off));
            off += sizeof(uint32_t);
            v->as.Str.start = malloc_w(sizeof(char) * v->as.Str.len);
            memcpy(v->as.Str.start, buf + off, v->as.Str.len);
            off += v->as.Str.len;
            break;
        }
        case VDBT_TYPE_INT: {
            memcpy(&v->as.Int, buf + off, sizeof(int64_t));
            off += sizeof(int64_t);
            break;
        }
        case VDBT_TYPE_FLOAT: {
            memcpy(&v->as.Float, buf + off, sizeof(double));
            off += sizeof(double);
            break;
        }
        case VDBT_TYPE_BOOL: {
            memcpy(&v->as.Bool, buf + off, sizeof(bool));
            off += sizeof(bool);
            break;
        }
        case VDBT_TYPE_NULL:
            break;
        default:
            assert(false && "invalid data type");
            break;
    }

    return off;
}

struct VdbValue vdbvalue_deserialize_string(uint8_t* buf) {
    struct VdbValue d;
    d.type = VDBT_TYPE_STR;

    d.as.Str.len = *((uint32_t*)(buf));
    d.as.Str.start = malloc_w(sizeof(char) * d.as.Str.len);
    memcpy(d.as.Str.start, buf + sizeof(uint32_t), d.as.Str.len);
    return d;
}

bool vdbvalue_is_null(struct VdbValue* d) {
    return d->type == VDBT_TYPE_NULL;
}

struct VdbValue vdbint(int64_t i) {
    struct VdbValue v;
    v.type = VDBT_TYPE_INT;
    v.as.Int = i;
    return v;
}

struct VdbValue vdbfloat(double d) {
    struct VdbValue v;
    v.type = VDBT_TYPE_FLOAT;
    v.as.Float = d;
    return v;
}

struct VdbValue vdbstring(char* s, int len) {
    struct VdbValue v;
    v.type = VDBT_TYPE_STR;
    v.as.Str.start = malloc_w(sizeof(char) * len);
    v.as.Str.len = len;
    memcpy(v.as.Str.start, s, v.as.Str.len);
    return v;
}

struct VdbValue vdbbool(bool b) {
    struct VdbValue v;
    v.type = VDBT_TYPE_BOOL;
    v.as.Bool = b;
    return v;
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
        return vdbstring(v.as.Str.start, v.as.Str.len);
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
