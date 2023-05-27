#ifndef VDB_BINARYTREE_H
#define VDB_BINARYTREE_H

#include "record.h"
#include "token.h"

struct VdbBinaryTree {
    struct VdbBinaryNode* root;
    struct VdbIntList* idxs;
};

struct VdbBinaryNode {
    struct VdbRecord* rec;
    struct VdbBinaryNode* left;
    struct VdbBinaryNode* right;
};

struct VdbBinaryTree* vdbbinarytree_init(struct VdbIntList* idxs);
void vdbbinarytree_insert_node(struct VdbBinaryTree* bt, struct VdbRecord* rec);
void vdbbinarytree_free(struct VdbBinaryTree* bt);
struct VdbRecordSet* vdbbinarytree_flatten_asc(struct VdbBinaryTree* bt);
struct VdbRecordSet* vdbbinarytree_flatten_desc(struct VdbBinaryTree* bt);

#endif //VDB_BINARYTREE_H
