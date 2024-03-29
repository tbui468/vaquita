#include <string.h>
#include <assert.h>
#include <stdarg.h>

#include "value.h"
#include "util.h"


int vdbvalue_serialized_size(struct VdbValue v) {
    int size = 0;
    size += sizeof(uint8_t);

    switch (v.type) {
        case VDBT_TYPE_TEXT: {
            size += sizeof(uint32_t) * 2;
            break;
        }
        case VDBT_TYPE_INT8: {
            size += sizeof(int64_t);
            break;
        }
        case VDBT_TYPE_FLOAT8: {
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

int vdbvalue_serialize_string(uint8_t* buf, struct VdbValue* v) {
    assert(v->type == VDBT_TYPE_TEXT && "must be string");
    int off = 0;

    *((uint8_t*)(buf + off)) = (uint8_t)(v->type);
    off += sizeof(uint8_t);

    *((uint32_t*)(buf + off)) = (uint32_t)(v->as.Str.len);
    off += sizeof(uint32_t);
    memcpy(buf + off, v->as.Str.start, v->as.Str.len);
    off += v->as.Str.len;

    return off;
}

int vdbvalue_serialized_string_size(struct VdbValue v) {
    assert(v.type == VDBT_TYPE_TEXT && "must be string");
    int off = 0;
    off += sizeof(uint8_t);
    off += sizeof(uint32_t);
    off += v.as.Str.len;

    return off;
}

int vdbvalue_serialize(uint8_t* buf, struct VdbValue v) {
    int off = 0;

    *((uint8_t*)(buf + off)) = (uint8_t)(v.type);
    off += sizeof(uint8_t);

    switch (v.type) {
        case VDBT_TYPE_TEXT: {
            assert(v.as.Str.block_idx != 0 && "block idx not set");
            *((uint32_t*)(buf + off)) = v.as.Str.block_idx;
            off += sizeof(uint32_t);
            *((uint32_t*)(buf + off)) = v.as.Str.idxcell_idx;
            off += sizeof(uint32_t);
            break;
        }
        case VDBT_TYPE_INT8: {
            memcpy(buf + off, &v.as.Int, sizeof(int64_t));
            off += sizeof(int64_t);
            break;
        }
        case VDBT_TYPE_FLOAT8: {
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

void vdbvalue_deserialize_string(struct VdbValue* v, uint8_t* buf) {
    int off = 0;
    v->type = (enum VdbTokenType)(*((uint8_t*)(buf + off)));
    off += sizeof(uint8_t);

    v->as.Str.len = *((uint32_t*)(buf + off));
    off += sizeof(uint32_t);
    v->as.Str.start = malloc_w(sizeof(char) * v->as.Str.len);
    memcpy(v->as.Str.start, buf + off, v->as.Str.len);
    off += v->as.Str.len;
}

int vdbvalue_deserialize(struct VdbValue* v, uint8_t* buf) {
    int off = 0;
    v->type = (enum VdbTokenType)(*((uint8_t*)(buf + off)));
    off += sizeof(uint8_t);

    switch (v->type) {
        case VDBT_TYPE_TEXT: {
            v->as.Str.block_idx = *((uint32_t*)(buf + off));
            off += sizeof(uint32_t);
            v->as.Str.idxcell_idx = *((uint32_t*)(buf + off));
            off += sizeof(uint32_t);
            break;
        }
        case VDBT_TYPE_INT8: {
            memcpy(&v->as.Int, buf + off, sizeof(int64_t));
            off += sizeof(int64_t);
            break;
        }
        case VDBT_TYPE_FLOAT8: {
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

bool vdbvalue_is_null(struct VdbValue* d) {
    return d->type == VDBT_TYPE_NULL;
}

struct VdbValue vdbint(int64_t i) {
    struct VdbValue v;
    v.type = VDBT_TYPE_INT8;
    v.as.Int = i;
    return v;
}

struct VdbValue vdbfloat(double d) {
    struct VdbValue v;
    v.type = VDBT_TYPE_FLOAT8;
    v.as.Float = d;
    return v;
}

struct VdbValue vdbstring(char* s, int len) {
    struct VdbValue v;
    v.type = VDBT_TYPE_TEXT;
    v.as.Str.start = malloc_w(sizeof(char) * len);
    v.as.Str.len = len;
    v.as.Str.block_idx = 0;
    v.as.Str.idxcell_idx = 0;
    memcpy(v.as.Str.start, s, v.as.Str.len);
    return v;
}

struct VdbValue vdbbool(bool b) {
    struct VdbValue v;
    v.type = VDBT_TYPE_BOOL;
    v.as.Bool = b;
    return v;
}

struct VdbValue vdbvalue_copy(struct VdbValue v) {
    if (v.type == VDBT_TYPE_TEXT) {
        return vdbstring(v.as.Str.start, v.as.Str.len);
    }

    return v;
}

int vdbvalue_compare(struct VdbValue v1, struct VdbValue v2) {
    assert(v1.type == v2.type && "value types must be the same to compared");
    switch (v1.type) {
        case VDBT_TYPE_TEXT:
            return strncmp(v1.as.Str.start, v2.as.Str.start, v1.as.Str.len > v2.as.Str.len ? v1.as.Str.len : v2.as.Str.len);
        case VDBT_TYPE_INT8:
            if (v1.as.Int < v2.as.Int)
                return -1;
            if (v1.as.Int > v2.as.Int)
                return 1;
            break;
        case VDBT_TYPE_FLOAT8:
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
    if (v.type == VDBT_TYPE_TEXT) {
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
        if (vl->values[i].type == VDBT_TYPE_TEXT) {
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
