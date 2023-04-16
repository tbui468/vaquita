#include <stddef.h>
#include <stdlib.h>

#include "node.h"
#include "util.h"
#include "record.h"

struct VdbNode* vdb_node_init_intern(uint32_t idx) {
    struct VdbNode* node = malloc_w(sizeof(struct VdbNode));
    node->type = VDBN_INTERN;
    node->idx = idx;
    node->as.intern.right = NULL;
    node->as.intern.nodes = vdb_nodelist_alloc();
    return node;
}

struct VdbNode* vdb_node_init_leaf(uint32_t idx) {
    struct VdbNode* node = malloc_w(sizeof(struct VdbNode));
    node->type = VDBN_LEAF;
    node->idx = idx;
    node->as.leaf.data = NULL;
    node->as.leaf.records = vdb_recordlist_alloc();
    return node;
}

struct VdbNode* vdb_node_init_data(uint32_t idx) {
    struct VdbNode* node = malloc_w(sizeof(struct VdbNode));
    node->type = VDBN_DATA;
    node->idx = idx;
    node->as.data.next = NULL;
    return node;
}

void vdb_node_free(struct VdbNode* node) {
    switch (node->type) {
        case VDBN_INTERN:
            vdb_nodelist_free(node->as.intern.nodes);
            break;
        case VDBN_LEAF:
            vdb_recordlist_free(node->as.leaf.records);
            break;
        case VDBN_DATA:
            break;
    }
    free(node);
}

struct VdbNodeList* vdb_nodelist_alloc() {
    struct VdbNodeList* rl = malloc_w(sizeof(struct VdbNodeList));
    rl->count = 0;
    rl->capacity = 8; 
    rl->nodes = malloc_w(sizeof(struct VdbNode*) * rl->capacity);
    return rl;
}

void vdb_nodelist_append_node(struct VdbNodeList* nl, struct VdbNode* node) {
    if (nl->count == nl->capacity) {
        nl->capacity *= 2;
        nl->nodes = realloc_w(nl->nodes, sizeof(struct VdbNode*) * nl->capacity);
    }

    nl->nodes[nl->count++] = node;
}

struct VdbNode* vdb_nodelist_get_node(struct VdbNodeList* nl, uint32_t idx) {
    struct VdbNode* node;
    uint32_t i;
    for (i = 0; i < nl->count; i++) {
        node = nl->nodes[i];
        if (node->idx == idx)
            return node;
    }

    return NULL;
}

void vdb_nodelist_remove_node(struct VdbNodeList* nl, uint32_t idx) {
    struct VdbNode* node = NULL;
    uint32_t i;
    for (i = 0; i < nl->count; i++) {
        node = nl->nodes[i];
        if (node->idx == idx)
            break;
    }

    if (node) {
        for (uint32_t j = i + 1; j < nl->count; j++) {
            nl->nodes[j - 1] = nl->nodes[j];
        }
        nl->count--;
        vdb_node_free(node);
    }
}

void vdb_nodelist_free(struct VdbNodeList* nl) {
    for (uint32_t i = 0; i < nl->count; i++) {
        struct VdbNode* node = nl->nodes[i];
        vdb_node_free(node);
    }

    free(nl->nodes);
    free(nl);
}

