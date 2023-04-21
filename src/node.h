#ifndef VDB_NODE_H
#define VDB_NODE_H

#include <stdint.h>
#include <stdbool.h>

#include "record.h"

enum VdbNodeType {
    VDBN_META,
    VDBN_INTERN,
    VDBN_LEAF,
    VDBN_DATA
};

struct VdbPtr {
    uint32_t idx;
    uint32_t key;
};

struct VdbPtrList {
    struct VdbPtr* pointers;
    uint32_t count;
    uint32_t capacity;
};

struct VdbNode {
    enum VdbNodeType type;
    uint32_t parent_idx;
    bool dirty;
    union {
        struct {
            struct VdbSchema* schema;
            uint32_t pk_counter;
            uint32_t root_idx;
        } meta;
        struct {
            struct VdbPtrList* pointers;
            uint32_t right_idx;
        } intern;
        struct {
            struct VdbRecordList* records;
            uint32_t data_idx;
        } leaf;
        struct {
            uint32_t next_idx;
        } data;
    } as; 
};

struct VdbNodeList {
    struct VdbNode** nodes;
    uint32_t count;
    uint32_t capacity;
};

struct VdbNode* vdb_node_init(enum VdbNodeType type, uint32_t parent_idx);
void vdb_node_free(struct VdbNode* node);
void vdb_node_serialize(uint8_t* buf, struct VdbNode* node);
void vdb_node_deserialize(struct VdbNode* node, uint8_t* buf);

bool vdb_node_leaf_full(struct VdbNode* node, struct VdbRecord* rec);
bool vdb_node_intern_full(struct VdbNode* node);

struct VdbPtrList* vdb_ptrlist_alloc();
void vdb_ptrlist_append_ptr(struct VdbPtrList* pl, struct VdbPtr ptr);
void vdb_ptrlist_free(struct VdbPtrList* pl);

#endif //VDB_NODE_H
