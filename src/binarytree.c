#include <assert.h>

#include "binarytree.h"
#include "util.h"

static void vdbbinarynode_free(struct VdbBinaryNode* n) {
    if (n->left) 
        vdbbinarynode_free(n->left);
    if (n->right)
        vdbbinarynode_free(n->right);
    free_w(n, sizeof(struct VdbBinaryNode));
}

static struct VdbBinaryNode* vdbbinarynode_init(struct VdbRecord* rec, struct VdbBinaryNode* left, struct VdbBinaryNode* right) {
    struct VdbBinaryNode* n = malloc_w(sizeof(struct VdbBinaryNode*));
    n->rec = rec;
    n->left = left;
    n->right = right;
    return n; 
}

struct VdbBinaryTree* vdbbinarytree_init(struct VdbIntList* idxs) {
    struct VdbBinaryTree* bt = malloc_w(sizeof(struct VdbBinaryTree));
    bt->root = NULL;
    bt->idxs = idxs;
    return bt;
}

static void vdbbinarytree_do_insertion(struct VdbBinaryTree* bt, struct VdbBinaryNode* n, struct VdbRecord* rec) {
    int result = vdbrecord_compare(rec, n->rec, bt->idxs);

    if (result < 0) {
        if (!n->left)
            n->left = vdbbinarynode_init(rec, NULL, NULL);
        else
            vdbbinarytree_do_insertion(bt, n->left, rec);
    } else if (result > 0) {
        if (!n->right)
            n->right = vdbbinarynode_init(rec, NULL, NULL);
        else
            vdbbinarytree_do_insertion(bt, n->right, rec);
    } else {
        assert(false && "records cannot be equal when inserting into binary tree");
    }

}

void vdbbinarytree_insert_node(struct VdbBinaryTree* bt, struct VdbRecord* rec) {
    if (!bt->root) {
        bt->root = vdbbinarynode_init(rec, NULL, NULL);
        return;
    }

    vdbbinarytree_do_insertion(bt, bt->root, rec);
}

void vdbbinarytree_free(struct VdbBinaryTree* bt) {
    vdbintlist_free(bt->idxs);
    if (bt->root)
        vdbbinarynode_free(bt->root);
    free_w(bt, sizeof(struct VdbBinaryTree));
}

static void vdbbinarynode_do_traverse_asc(struct VdbBinaryNode* n, struct VdbRecordSet* rs) {
    if (n->left)
        vdbbinarynode_do_traverse_asc(n->left, rs);
    
    vdbrecordset_append_record(rs, n->rec);

    if (n->right)
        vdbbinarynode_do_traverse_asc(n->right, rs);
}

struct VdbRecordSet* vdbbinarytree_flatten_asc(struct VdbBinaryTree* bt) {
    struct VdbRecordSet* rs = vdbrecordset_init();
    if (bt->root)
        vdbbinarynode_do_traverse_asc(bt->root, rs);

    return rs;
}

static void vdbbinarynode_do_traverse_desc(struct VdbBinaryNode* n, struct VdbRecordSet* rs) {
    if (n->right)
        vdbbinarynode_do_traverse_desc(n->right, rs);
    
    vdbrecordset_append_record(rs, n->rec);

    if (n->left)
        vdbbinarynode_do_traverse_desc(n->left, rs);
}
struct VdbRecordSet* vdbbinarytree_flatten_desc(struct VdbBinaryTree* bt) {
    struct VdbRecordSet* rs = vdbrecordset_init();
    if (bt->root)
        vdbbinarynode_do_traverse_desc(bt->root, rs);

    return rs;
}

