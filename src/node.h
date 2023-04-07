#ifndef VDB_NODE_H
#define VDB_NODE_H

#include <stdint.h>

struct VdbNodePtr {
    uint32_t key;
    uint32_t idx;
};

struct VdbNodePtrList {
    uint32_t count;
    uint32_t capacity;
    struct VdbNodePtr* pointers;
};

struct VdbRecordList {
    uint32_t count;
    uint32_t capacity;
    struct VdbRecord* records;
};

struct VdbStringList {
    uint32_t count;
    uint32_t capacity;
    struct VdbString** strings;
};

enum VdbNodeType {
    VDBN_META,
    VDBN_INTERN,
    VDBN_LEAF,
    VDBN_DATA
};

struct VdbNode {
    enum VdbNodeType type;
    uint32_t idx;
    union {
        struct {
            uint32_t pk_counter;
            struct VdbSchema* schema;
        } meta;
        struct {
            struct VdbNodePtrList* pl;
        } intern;
        struct {
            struct VdbRecordList* rl;
            uint32_t data_idx;
        } leaf;
        struct {
            uint32_t next;
        } data;
    } as;
};

struct VdbNode node_deserialize_meta(uint8_t* buf);
struct VdbNode node_deserialize_intern(uint8_t* buf);
struct VdbNode node_deserialize_leaf(uint8_t* buf, uint8_t* data_buf, struct VdbSchema* schema);
struct VdbNode node_init_intern(uint32_t idx);
struct VdbNode node_init_leaf(uint32_t idx);
struct VdbNode node_init_data(uint32_t idx);
void node_serialize(uint8_t* buf, uint8_t* data_buf, struct VdbNode* node);
void node_append_nodeptr(struct VdbNode* node, struct VdbNodePtr ptr);
void node_clear_nodeptrs(struct VdbNode* node);
void node_append_record(struct VdbNode* node, struct VdbRecord rec);
void node_free(struct VdbNode* node);
bool node_is_full(struct VdbNode* node, uint32_t insert_size);
uint32_t node_nodeptr_size();
bool node_is_root(struct VdbNode* node);
uint32_t node_read_data_idx(uint8_t* buf);

#endif //VDB_NODE_H
