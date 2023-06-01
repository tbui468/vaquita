#ifndef VDB_BINARYTREE_H
#define VDB_BINARYTREE_H

#include "record.h"
#include "token.h"

struct VdbBinaryTree {
    struct VdbBinaryNode* root;
};

struct VdbBinaryNode {
    struct VdbRecordSet* rs;
    struct VdbBinaryNode* left;
    struct VdbBinaryNode* right;
};

struct VdbBinaryTree* vdbbinarytree_init();
void vdbbinarytree_insert_node(struct VdbBinaryTree* bt, struct VdbRecordSet* rs);
void vdbbinarytree_free(struct VdbBinaryTree* bt);
struct VdbRecordSet* vdbbinarytree_flatten_asc(struct VdbBinaryTree* bt);
struct VdbRecordSet* vdbbinarytree_flatten_desc(struct VdbBinaryTree* bt);

#endif //VDB_BINARYTREE_H
