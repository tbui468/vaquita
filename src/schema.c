#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include "schema.h"
#include "util.h"

//TODO: should remove this function since we have vdbschema_alloc now
struct VdbSchema* vdb_schema_alloc(int count, va_list args) {
    struct VdbSchema* schema = malloc_w(sizeof(struct VdbSchema));
    schema->count = count;
    schema->types = malloc_w(sizeof(enum VdbTokenType) * count);
    schema->names = malloc_w(sizeof(char*) * count);

    for (int i = 0; i < count; i++) {
        schema->types[i] = va_arg(args, int);
        schema->names[i] = strdup_w(va_arg(args, const char*));
    }

    return schema;
}

struct VdbSchema* vdbschema_alloc(int count, struct VdbTokenList* attributes, struct VdbTokenList* types) {
    struct VdbSchema* schema = malloc_w(sizeof(struct VdbSchema));
    schema->count = count;
    schema->types = malloc_w(sizeof(enum VdbTokenType) * count);
    schema->names = malloc_w(sizeof(char*) * count);

    for (int i = 0; i < count; i++) {
        schema->types[i] = types->tokens[i].type;
        int len = attributes->tokens[i].len;
        schema->names[i] = malloc_w(sizeof(char) * (len + 1));
        memcpy(schema->names[i], attributes->tokens[i].lexeme, len);
        schema->names[i][len] = '\0';
    }

    return schema;
}

void vdb_schema_free(struct VdbSchema* schema) {
    for (uint32_t i = 0; i < schema->count; i++) {
        free(schema->names[i]);
    }
    free(schema->names);
    free(schema->types);
    free(schema);
}

struct VdbSchema* vdb_schema_copy(struct VdbSchema* schema) {
    struct VdbSchema* s = malloc_w(sizeof(struct VdbSchema));
    s->count = schema->count;
    s->types = malloc_w(sizeof(enum VdbTokenType) * s->count);
    s->names = malloc_w(sizeof(char*) * s->count);

    memcpy(s->types, schema->types, sizeof(enum VdbTokenType) * s->count);

    for (uint32_t i = 0; i < s->count; i++) {
        s->names[i] = strdup_w(schema->names[i]);
    }

    return s;
}

void vdbschema_serialize(uint8_t* buf, struct VdbSchema* schema) {
    //TODO: remove these two
    int i = 0;
    int* off = &i;

    write_u32(buf, schema->count, off);
    for (uint32_t i = 0; i < schema->count; i++) {
        enum VdbTokenType f = schema->types[i];
        write_u32(buf, f, off);
        uint32_t len = strlen(schema->names[i]);
        write_u32(buf, len, off);
        memcpy(buf + *off, schema->names[i], len);
        *off += len;
    }
}

struct VdbSchema* vdbschema_deserialize(uint8_t* buf) {
    //TODO: remove these two
    int i = 0;
    int* off = &i;

    struct VdbSchema* schema = malloc_w(sizeof(struct VdbSchema));
    read_u32(&schema->count, buf, off);
    schema->types = malloc_w(sizeof(enum VdbTokenType) * schema->count);
    schema->names = malloc_w(sizeof(char*) * schema->count);
    for (uint32_t i = 0; i < schema->count; i++) {
        uint32_t type;
        read_u32(&type, buf, off);
        schema->types[i] = type;
        uint32_t len;
        read_u32(&len, buf, off);
        schema->names[i] = malloc_w(sizeof(char) * len + 1);
        memcpy(schema->names[i], buf + *off, len);
        schema->names[i][len] = '\0';
        *off += len;
    }
    return schema;
}

uint32_t vdbschema_fixedlen_record_size(struct VdbSchema* schema) {
    uint32_t size = 0;
    size += sizeof(uint32_t); //record key - TODO: should remove since key is now part of data
    for (uint32_t i = 0; i < schema->count; i++) {
        size += sizeof(bool); //is_null flag
        switch (schema->types[i]) {
            case VDBT_TYPE_INT:
                size += sizeof(uint64_t);
                break;
            case VDBT_TYPE_FLOAT:
                size += sizeof(double);
                break;
            case VDBT_TYPE_STR:
                size += sizeof(uint32_t) * 2;
                break;
            case VDBT_TYPE_BOOL:
                size += sizeof(bool);
                break;
            default:
                assert(false && "invalid data type");
                break;
        }
    }

    return size;
}
