#include "schema.h"
#include "util.h"

#include <stdlib.h>
#include <string.h>

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

void vdb_schema_serialize(uint8_t* buf, struct VdbSchema* schema, int* off) {
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

struct VdbSchema* vdb_schema_deserialize(uint8_t* buf, int* off) {
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
