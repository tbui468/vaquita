#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include "schema.h"
#include "util.h"


struct VdbSchema* vdbschema_alloc(int count, struct VdbTokenList* attributes, struct VdbTokenList* types, int key_idx) {
    struct VdbSchema* schema = malloc_w(sizeof(struct VdbSchema));
    schema->count = count;
    schema->types = malloc_w(sizeof(enum VdbTokenType) * count);
    schema->names = malloc_w(sizeof(char*) * count);
    schema->key_idx = key_idx;

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
        free_w(schema->names[i], sizeof(char) * (strlen(schema->names[i]) + 1));
    }
    free_w(schema->names, sizeof(char*) * schema->count);
    free_w(schema->types, sizeof(enum VdbTokenType) * schema->count);
    free_w(schema, sizeof(struct VdbSchema));
}

struct VdbSchema* vdb_schema_copy(struct VdbSchema* schema) {
    struct VdbSchema* s = malloc_w(sizeof(struct VdbSchema));
    s->count = schema->count;
    s->types = malloc_w(sizeof(enum VdbTokenType) * s->count);
    s->names = malloc_w(sizeof(char*) * s->count);
    s->key_idx = schema->key_idx;

    memcpy(s->types, schema->types, sizeof(enum VdbTokenType) * s->count);

    for (uint32_t i = 0; i < s->count; i++) {
        s->names[i] = strdup_w(schema->names[i]);
    }

    return s;
}

void vdbschema_serialize(uint8_t* buf, struct VdbSchema* schema) {
    int off = 0;

    *((uint32_t*)(buf + off)) = schema->key_idx;
    off += sizeof(uint32_t);

    *((uint32_t*)(buf + off)) = schema->count;
    off += sizeof(uint32_t);

    for (uint32_t i = 0; i < schema->count; i++) {
        *((uint32_t*)(buf + off)) = (uint32_t)schema->types[i];
        off += sizeof(uint32_t);

        *((uint32_t*)(buf + off)) = (uint32_t)strlen(schema->names[i]);
        off += sizeof(uint32_t);

        int len = strlen(schema->names[i]);
        memcpy(buf + off, schema->names[i], len);
        off += len;
    }
}

struct VdbSchema* vdbschema_deserialize(uint8_t* buf) {
    int off = 0;

    struct VdbSchema* schema = malloc_w(sizeof(struct VdbSchema));

    schema->key_idx = *((uint32_t*)(buf + off));
    off += sizeof(uint32_t);

    schema->count = *((uint32_t*)(buf + off));
    off += sizeof(uint32_t);

    schema->types = malloc_w(sizeof(enum VdbTokenType) * schema->count);
    schema->names = malloc_w(sizeof(char*) * schema->count);

    for (uint32_t i = 0; i < schema->count; i++) {
        schema->types[i] = *((uint32_t*)(buf + off));
        off += sizeof(uint32_t);

        int len = *((uint32_t*)(buf + off));
        off += sizeof(uint32_t);

        schema->names[i] = malloc_w(sizeof(char) * len + 1);
        memcpy(schema->names[i], buf + off, len);
        schema->names[i][len] = '\0';
        off += len;
    }

    return schema;
}


