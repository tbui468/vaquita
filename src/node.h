#ifndef VDB_NODE_H
#define VDB_NODE_H

#include <stdint.h>
#include <stdbool.h>

#include "record.h"

enum VdbNodeType {
    VDBN_INTERN,
    VDBN_LEAF,
    VDBN_DATA
};

struct VdbNode {
    enum VdbNodeType type;
    uint32_t idx;
    struct VdbNode* parent;
    union {
        struct {
            struct VdbNodeList* nodes;
            struct VdbNode* right;
        } intern;
        struct {
            struct VdbRecordList* records;
            struct VdbNode* data;
        } leaf;
        struct {
            struct VdbNode* next;
        } data;
    } as; 
};

struct VdbNodeList {
    struct VdbNode** nodes;
    uint32_t count;
    uint32_t capacity;
};

struct VdbNode* vdb_node_init_intern(uint32_t idx, struct VdbNode* parent);
struct VdbNode* vdb_node_init_leaf(uint32_t idx, struct VdbNode* parent);
struct VdbNode* vdb_node_init_data(uint32_t idx, struct VdbNode* parent);
void vdb_node_free(struct VdbNode* node);

bool vdb_node_leaf_full(struct VdbNode* node, struct VdbRecord* rec);
bool vdb_node_intern_full(struct VdbNode* node);

struct VdbNodeList* vdb_nodelist_alloc();
void vdb_nodelist_append_node(struct VdbNodeList* nl, struct VdbNode* node);
struct VdbNode* vdb_nodelist_get_node(struct VdbNodeList* nl, uint32_t idx);
void vdb_nodelist_remove_node(struct VdbNodeList* nl, uint32_t idx);
void vdb_nodelist_free(struct VdbNodeList* nl);

#endif //VDB_NODE_H
