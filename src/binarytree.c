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

static struct VdbBinaryNode* vdbbinarynode_init(struct VdbRecordSet* rs, struct VdbBinaryNode* left, struct VdbBinaryNode* right) {
    struct VdbBinaryNode* n = malloc_w(sizeof(struct VdbBinaryNode*));
    n->rs = rs;
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

static void vdbbinarytree_do_insertion(struct VdbBinaryTree* bt, struct VdbBinaryNode* n, struct VdbRecordSet* rs) {
    int result = vdbrecord_compare(rs->records[0], n->rs->records[0], bt->idxs);

    if (result < 0) {
        if (!n->left)
            n->left = vdbbinarynode_init(rs, NULL, NULL);
        else
            vdbbinarytree_do_insertion(bt, n->left, rs);
    } else if (result > 0) {
        if (!n->right)
            n->right = vdbbinarynode_init(rs, NULL, NULL);
        else
            vdbbinarytree_do_insertion(bt, n->right, rs);
    } else {
        assert(false && "records cannot be equal when inserting into binary tree");
    }

}

void vdbbinarytree_insert_node(struct VdbBinaryTree* bt, struct VdbRecordSet* rs) {
    if (!bt->root) {
        bt->root = vdbbinarynode_init(rs, NULL, NULL);
        return;
    }

    vdbbinarytree_do_insertion(bt, bt->root, rs);
}

void vdbbinarytree_free(struct VdbBinaryTree* bt) {
    vdbintlist_free(bt->idxs);
    if (bt->root)
        vdbbinarynode_free(bt->root);
    free_w(bt, sizeof(struct VdbBinaryTree));
}

static void vdbbinarynode_do_traverse_desc(struct VdbBinaryNode* n, struct VdbRecordSet** head) {
    if (n->left)
        vdbbinarynode_do_traverse_desc(n->left, head);
    
    n->rs->next = *head;
    *head = n->rs;

    if (n->right)
        vdbbinarynode_do_traverse_desc(n->right, head);
}

struct VdbRecordSet* vdbbinarytree_flatten_desc(struct VdbBinaryTree* bt) {
    struct VdbRecordSet* head = NULL;
    if (bt->root)
        vdbbinarynode_do_traverse_desc(bt->root, &head);

    return head;
}

static void vdbbinarynode_do_traverse_asc(struct VdbBinaryNode* n, struct VdbRecordSet** head) {
    if (n->right)
        vdbbinarynode_do_traverse_asc(n->right, head);
    
    n->rs->next = *head;
    *head = n->rs;

    if (n->left)
        vdbbinarynode_do_traverse_asc(n->left, head);
}
struct VdbRecordSet* vdbbinarytree_flatten_asc(struct VdbBinaryTree* bt) {
    struct VdbRecordSet* head = NULL;
    if (bt->root)
        vdbbinarynode_do_traverse_asc(bt->root, &head);

    return head;
}

