#ifndef VDB_SCHEMA_H
#define VDB_SCHEMA_H

#include <stdint.h>
#include <stdbool.h>
#include <stdarg.h>

#include "token.h"
#include "value.h"

struct VdbSchema {
    enum VdbTokenType* types;
    char** names;
    uint32_t count;
    uint32_t key_idx;
};

struct VdbSchema* vdbschema_alloc(int count, struct VdbTokenList* attributes, struct VdbTokenList* types, int key_idx);
void vdb_schema_free(struct VdbSchema* schema);
struct VdbSchema* vdb_schema_copy(struct VdbSchema* schema);
void vdbschema_serialize(uint8_t* buf, struct VdbSchema* schema);
struct VdbSchema* vdbschema_deserialize(uint8_t* buf);
uint32_t vdbschema_fixedlen_record_size(struct VdbSchema* schema);

#endif //VDB_SCHEMA_H
