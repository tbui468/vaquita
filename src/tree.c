#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "util.h"
#include "tree.h"


struct VdbSchema* vdbtree_meta_read_schema(struct VdbTree* tree) {
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, 0);
    struct VdbSchema* schema = vdbnode_meta_read_schema(page->buf);
    vdb_pager_unpin_page(page);
    return schema;
}

uint32_t vdbtree_meta_increment_primary_key_counter(struct VdbTree* tree) {
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, 0);
    page->dirty = true;
    uint32_t pk_counter = vdbnode_meta_read_primary_key_counter(page->buf);
    pk_counter++;
    vdbnode_meta_write_primary_key_counter(page->buf, pk_counter);
    vdb_pager_unpin_page(page);
    return pk_counter;
}

struct VdbChunkList* vdb_chunklist_init() {
    struct VdbChunkList* cl = malloc_w(sizeof(struct VdbChunkList));
    cl->count = 0;
    cl->capacity = 8;
    cl->chunks = malloc_w(sizeof(struct VdbChunk) * cl->capacity);
    return cl;
}

void vdb_chunklist_append_chunk(struct VdbChunkList* cl, struct VdbChunk chunk) {
    if (cl->count == cl->capacity) {
        cl->capacity *= 2;
        cl->chunks = realloc_w(cl->chunks, sizeof(struct VdbChunk) * cl->capacity);
    }

    cl->chunks[cl->count++] = chunk;
}

void vdb_chunklist_free(struct VdbChunkList* cl) {
    free(cl->chunks);
    free(cl);
}

struct VdbChunk vdb_tree_init_chunk(struct VdbTree* tree, uint32_t parent_idx, enum VdbNodeType type) {
    struct VdbNode* node = vdb_node_init(type, parent_idx);

    uint32_t idx = vdb_pager_fresh_page(tree->f);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;

    struct VdbChunk c = {node, page};
    return c;
}

uint32_t vdbtree_meta_init(struct VdbTree* tree, struct VdbSchema* schema) {
    uint32_t idx = vdb_pager_fresh_page(tree->f);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;

    vdbnode_meta_write_type(page->buf);
    vdbnode_meta_write_parent(page->buf, 0);
    vdbnode_meta_write_primary_key_counter(page->buf, 0);
    vdbnode_meta_write_root(page->buf, 0);
    vdbnode_meta_write_schema(page->buf, schema);

    vdb_pager_unpin_page(page);

    return idx;
}

uint32_t vdbtree_intern_init(struct VdbTree* tree, uint32_t parent_idx) {
    uint32_t idx = vdb_pager_fresh_page(tree->f);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;

    vdbnode_intern_write_type(page->buf);
    vdbnode_intern_write_parent(page->buf, 0);
    struct VdbPtr right_ptr = {0, 0};
    vdbnode_intern_write_right_ptr(page->buf, right_ptr);
    vdbnode_intern_write_ptr_count(page->buf, 0);
    vdbnode_intern_write_datacells_size(page->buf, 0);

    vdb_pager_unpin_page(page);

    return idx;
}

uint32_t vdbtree_leaf_init(struct VdbTree* tree, uint32_t parent_idx) {
    uint32_t idx = vdb_pager_fresh_page(tree->f);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;

    vdbnode_leaf_write_type(page->buf);
    vdbnode_leaf_write_parent(page->buf, 0);
    vdbnode_leaf_write_data_block_idx(page->buf, 0);
    vdbnode_leaf_write_record_count(page->buf, 0);
    vdbnode_leaf_write_datacells_size(page->buf, 0);

    vdb_pager_unpin_page(page);

    return idx;
}

void vdbtree_meta_write_root(struct VdbTree* tree, uint32_t meta_idx, uint32_t root_idx) {
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, meta_idx);
    page->dirty = true;
    vdbnode_meta_write_root(page->buf, root_idx);
    vdb_pager_unpin_page(page);
}

void vdbtree_intern_write_right_ptr(struct VdbTree* tree, uint32_t idx, struct VdbPtr ptr) {
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;
    vdbnode_intern_write_right_ptr(page->buf, ptr);
    vdb_pager_unpin_page(page);
}

struct VdbTree* vdb_tree_init(const char* name, struct VdbSchema* schema, struct VdbPager* pager, FILE* f) {
    struct VdbTree* tree = malloc_w(sizeof(struct VdbTree));
    tree->name = strdup_w(name);
    tree->pager = pager;
    tree->f = f;
    tree->chunks = vdb_chunklist_init();

    uint32_t meta_idx = vdbtree_meta_init(tree, schema);
    tree->meta_idx = meta_idx;
    uint32_t root_idx = vdbtree_intern_init(tree, meta_idx);
    vdbtree_meta_write_root(tree, meta_idx, root_idx);
    uint32_t leaf_idx = vdbtree_leaf_init(tree, root_idx);
    struct VdbPtr right_ptr = {leaf_idx, 0};
    vdbtree_intern_write_right_ptr(tree, root_idx, right_ptr);

    /*
    struct VdbChunk meta = vdb_tree_init_chunk(tree, 0, VDBN_META);
    meta.node->as.meta.schema = vdb_schema_copy(schema);
    tree->meta_idx = 0;

    struct VdbChunk root = vdb_tree_init_chunk(tree, 0, VDBN_INTERN);
    meta.node->as.meta.root_idx = root.page->idx;

    struct VdbChunk leaf = vdb_tree_init_chunk(tree, root.page->idx, VDBN_LEAF);
    root.node->as.intern.right_idx = leaf.page->idx;

    vdb_tree_release_chunk(tree, meta);
    vdb_tree_release_chunk(tree, leaf);
    vdb_tree_release_chunk(tree, root);*/
   
    return tree;
}

struct VdbTree* vdb_tree_catch(const char* name, FILE* f, struct VdbPager* pager) {
    struct VdbTree* tree = malloc_w(sizeof(struct VdbTree));
    tree->name = strdup(name);
    tree->f = f;
    tree->pager = pager;
    tree->meta_idx = 0;
    tree->chunks = vdb_chunklist_init();
    return tree;
}

struct VdbChunk vdb_tree_catch_chunk(struct VdbTree* tree, uint32_t idx) {
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    struct VdbNode* node = vdb_node_deserialize(page->buf);
    node->dirty = false;

    struct VdbChunk c = {node, page};
    return c;
}


void vdb_tree_release_chunk(struct VdbTree* tree, struct VdbChunk chunk) {
    if (chunk.node->dirty) {
        chunk.page->dirty = true;
        vdb_node_serialize(chunk.page->buf, chunk.node);
    }

    vdb_pager_unpin_page(chunk.page);
    vdb_node_free(chunk.node);
}

void vdb_tree_release(struct VdbTree* tree) {
    free(tree->name);
    vdb_chunklist_free(tree->chunks);
    free(tree);
}

/*
struct VdbNode* _vdb_tree_traverse_to(struct VdbNode* node, uint32_t key) {
    assert(node->type == VDBN_INTERN);

    //TODO: should do binary search instead of linear
    for (uint32_t i = 0; i < node->as.intern.nodes->count; i++) {
        struct VdbNode* n = node->as.intern.nodes->nodes[i];
        if (n->type == VDBN_LEAF && n->as.leaf.records->count > 0) {
            uint32_t last_rec_key = n->as.leaf.records->records[n->as.leaf.records->count - 1]->key;
            if (key <= last_rec_key)
                return n;
        } else if (n->type == VDBN_INTERN) {
            //get largest key for a given internal node, and compare to key
            struct VdbNode* l = n->as.intern.right;
            while (l->type != VDBN_LEAF) {
                l = l->as.intern.right;
            }
            uint32_t largest_rec_key = l->as.leaf.records->records[l->as.leaf.records->count - 1]->key;
            if (key <= largest_rec_key) {
                return _vdb_tree_traverse_to(n, key);
            }
        }
    }

    if (node->as.intern.right->type == VDBN_LEAF) {
        return node->as.intern.right;
    }

    return _vdb_tree_traverse_to(node->as.intern.right, key);
}

struct VdbNode* _vdb_tree_split_intern(struct VdbTree* tree, struct VdbNode* node) {
    assert(node->type == VDBN_INTERN);

    struct VdbNode* new_intern = NULL;

    if (!node->parent) { //node is root
        struct VdbNode* old_root = node;
        tree->root = _vdb_tree_init_node(tree, NULL, VDBN_INTERN);
        old_root->parent = tree->root;
        new_intern = _vdb_tree_init_node(tree, tree->root, VDBN_INTERN);
        tree->root->as.intern.right = new_intern;
        vdb_nodelist_append_node(tree->root->as.intern.nodes, old_root);
    } else if (!vdb_node_intern_full(node->parent)) {
        new_intern = _vdb_tree_init_node(tree, node->parent, VDBN_INTERN);
        vdb_nodelist_append_node(node->parent->as.intern.nodes, node->parent->as.intern.right);
        node->parent->as.intern.right = new_intern;
    } else {
        struct VdbNode* new_parent = _vdb_tree_split_intern(tree, node->parent);
        new_intern = _vdb_tree_init_node(tree, new_parent, VDBN_INTERN);
        new_parent->as.intern.right = new_intern;
    }

    return new_intern;
}

struct VdbNode* _vdb_tree_split_leaf(struct VdbTree* tree, struct VdbNode* node) {
    assert(node->type == VDBN_LEAF);

    struct VdbNode* parent = node->parent;

    if (vdb_node_intern_full(parent)) {
        struct VdbNode* new_intern = _vdb_tree_split_intern(tree, parent);
        new_intern->as.intern.right = _vdb_tree_init_node(tree, new_intern, VDBN_LEAF);
        return new_intern->as.intern.right;
    }
    
    vdb_nodelist_append_node(parent->as.intern.nodes, parent->as.intern.right);
    parent->as.intern.right = _vdb_tree_init_node(tree, parent, VDBN_LEAF);

    return parent->as.intern.right;
}*/


enum VdbNodeType vdbtree_node_type(struct VdbTree* tree, uint32_t idx) {
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    enum VdbNodeType type = vdbnode_type(page->buf);
    vdb_pager_unpin_page(page);
    return type;
}

uint32_t vdbtree_meta_read_root(struct VdbTree* tree) {
    assert(vdbtree_node_type(tree, 0) == VDBN_META);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, 0);
    uint32_t root_idx = vdb_node_meta_read_root(page->buf);
    vdb_pager_unpin_page(page);
    return root_idx;
}

void vdbtree_leaf_write_record(struct VdbTree* tree, uint32_t idx, struct VdbRecord* rec) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    page->dirty = true;
    vdbnode_leaf_write_record(page->buf, rec); //TODO: integrate variable length data using data blocks
    vdb_pager_unpin_page(page);
}

uint32_t vdbtree_intern_read_ptr_count(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    uint32_t count = vdbnode_intern_read_ptr_count(page->buf);
    vdb_pager_unpin_page(page);
    return count;
}

struct VdbPtr vdbtree_intern_read_ptr(struct VdbTree* tree, uint32_t idx, uint32_t ptr_idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    struct VdbPtr ptr = vdbnode_intern_read_ptr(page->buf, ptr_idx);
    vdb_pager_unpin_page(page);
    return ptr;
}

struct VdbPtr vdbtree_intern_read_right_ptr(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    struct VdbPtr right = vdbnode_intern_read_right_ptr(page->buf);
    vdb_pager_unpin_page(page);
    return right;
}

uint32_t vdbtree_leaf_read_record_count(struct VdbTree* tree, uint32_t idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    uint32_t count = vdbnode_leaf_read_record_count(page->buf);
    vdb_pager_unpin_page(page);
    return count;
}

uint32_t vdbtree_leaf_read_record_key(struct VdbTree* tree, uint32_t idx, uint32_t rec_idx) {
    assert(vdbtree_node_type(tree, idx) == VDBN_LEAF);
    struct VdbPage* page = vdb_pager_pin_page(tree->pager, tree->name, tree->f, idx);
    uint32_t key = vdbnode_leaf_read_record_key(page->buf, rec_idx);
    vdb_pager_unpin_page(page);
    return key;
}

uint32_t vdb_tree_traverse_to(struct VdbTree* tree, uint32_t idx, uint32_t key) {
    assert(vdbtree_node_type(tree, idx) == VDBN_INTERN);

    //Check to see if any ptrs have correct key
    /*
    for (uint32_t i = 0; i < vdbtree_intern_read_ptr_count(tree, idx); i++) {
        struct VdbPtr p = vdbtree_intern_read_ptr(tree, idx, i);
        if (vdbtree_node_type(tree, p.idx) == VDBN_LEAF && vdbtree_leaf_read_record_count(tree, p.idx) > 0) {
            uint32_t last_rec_key = vdbtree_leaf_read_record_key(tree, p.idx, vdbtree_leaf_read_record_count(tree, p.idx) - 1);
            if (key <= last_rec_key) {
                return p.idx;
            }
        } else if (vdbtree_node_type(tree, p.idx) == VDBN_INTERN) {
            //get largest key for a given internal node, and compare to key
            struct VdbPtr right = vdbtree_intern_read_right_ptr(tree, p.idx);
            while (vdbtree_node_type(tree, right.idx) != VDBN_LEAF) {
                right = vdbtree_intern_read_right_ptr(tree, right.idx);
            }
            uint32_t last_rec_key = vdbtree_leaf_read_record_key(tree, right.idx, vdbtree_leaf_read_record_count(tree, right.idx) - 1);
            if (key <= last_rec_key) {
                return vdb_tree_traverse_to(tree, p.idx, key);
            }
        }
    }*/

    //if not found, check right pointer
    struct VdbPtr p = vdbtree_intern_read_right_ptr(tree, idx);

    if (vdbtree_node_type(tree, p.idx) == VDBN_LEAF) {
        return p.idx;
    } else {
        return vdb_tree_traverse_to(tree, p.idx, key);
    }

}

void vdb_tree_insert_record(struct VdbTree* tree, struct VdbRecord* rec) {
    uint32_t root_idx = vdbtree_meta_read_root(tree);
    uint32_t leaf_idx = vdb_tree_traverse_to(tree, root_idx, rec->key);

    /*
    if (vdb_node_leaf_full(leaf, rec)) {
        leaf = _vdb_tree_split_leaf(tree, leaf);
    }*/

    vdbtree_leaf_write_record(tree, leaf_idx, rec);
}

/*
struct VdbRecord* vdb_tree_fetch_record(struct VdbTree* tree, uint32_t key) {
    if (key > tree->pk_counter) {
        return NULL;
    }

    struct VdbNode* root = tree->root;
    struct VdbNode* leaf = _vdb_tree_traverse_to(root, key);

    return vdb_recordlist_get_record(leaf->as.leaf.records, key);
}

bool vdb_tree_update_record(struct VdbTree* tree, struct VdbRecord* rec) {
    struct VdbNode* root = tree->root;
    struct VdbNode* leaf = _vdb_tree_traverse_to(root, rec->key);

    if (!leaf) {
        return false;
    }

    for (uint32_t i = 0; i < leaf->as.leaf.records->count; i++) {
        struct VdbRecord* r = leaf->as.leaf.records->records[i];
        if (r->key == rec->key) {
            leaf->as.leaf.records->records[i] = rec;
            vdb_record_free(r);
            return true;
        }
    }
    
    return false;
}

bool vdb_tree_delete_record(struct VdbTree* tree, uint32_t key) {
    struct VdbNode* root = tree->root;
    struct VdbNode* leaf = _vdb_tree_traverse_to(root, key);

    if (!leaf) {
        return false;
    }

    //TODO: need a way to reuse deleted records
    for (uint32_t i = 0; i < leaf->as.leaf.records->count; i++) {
        struct VdbRecord* r = leaf->as.leaf.records->records[i];
        if (r->key == key) {
            r->key = 0;
            return true;
        }
    }
    
    return false;
}*/

struct VdbTreeList* vdb_treelist_init() {
    struct VdbTreeList* tl = malloc_w(sizeof(struct VdbTreeList));
    tl->count = 0;
    tl->capacity = 8;
    tl->trees = malloc_w(sizeof(struct VdbTree*) * tl->capacity);

    return tl;
}

void vdb_treelist_append_tree(struct VdbTreeList* tl, struct VdbTree* tree) {
    if (tl->count == tl->capacity) {
        tl->capacity *= 2;
        tl->trees = realloc_w(tl->trees, sizeof(struct VdbTree*) * tl->capacity);
    }

    tl->trees[tl->count++] = tree;
}

struct VdbTree* vdb_treelist_get_tree(struct VdbTreeList* tl, const char* name) {
    struct VdbTree* t;
    uint32_t i;
    for (i = 0; i < tl->count; i++) {
        t = tl->trees[i];
        if (strncmp(name, t->name, strlen(name)) == 0)
            return t;
    }

    return NULL;
}

struct VdbTree* vdb_treelist_remove_tree(struct VdbTreeList* tl, const char* name) {
    struct VdbTree* t = NULL;
    uint32_t i;
    for (i = 0; i < tl->count; i++) {
        t = tl->trees[i];
        if (strncmp(name, t->name, strlen(name)) == 0)
            break;
    }

    if (t) {
        tl->trees[i] = tl->trees[--tl->count];
    }

    return t;
}

void vdb_treelist_free(struct VdbTreeList* tl) {
    for (uint32_t i = 0; i < tl->count; i++) {
        struct VdbTree* t = tl->trees[i];
        vdb_tree_release(t);
    }

    free(tl->trees);
    free(tl);
}
