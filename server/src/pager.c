#include <stdbool.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include "pager.h"
#include "util.h"

uint32_t vdb_pager_fresh_page(FILE* f) {
    fseek_w(f, 0, SEEK_END);
    uint32_t idx = ftell_w(f) / VDB_PAGE_SIZE;

    uint8_t* buf = calloc_w(VDB_PAGE_SIZE, sizeof(uint8_t));
    fwrite_w(buf, sizeof(uint8_t), VDB_PAGE_SIZE, f); 
    free_w(buf, sizeof(uint8_t) * VDB_PAGE_SIZE);
    return idx;
}

void _vdb_pager_free_page(struct VdbPage* page) {
    free_w(page->name, sizeof(char) * (strlen(page->name) + 1)); //include null terminator
    free_w(page->buf, sizeof(uint8_t) * VDB_PAGE_SIZE);
    free_w(page, sizeof(struct VdbPage));
}

struct VdbPage* _vdb_pager_load_page(char* name, FILE* f, uint32_t idx) {
    struct VdbPage* page = malloc_w(sizeof(struct VdbPage));

    page->dirty = false;
    page->pin_count = 0;
    page->idx = idx;
    page->buf = malloc_w(sizeof(uint8_t) * VDB_PAGE_SIZE);
    fseek_w(f, page->idx * VDB_PAGE_SIZE, SEEK_SET);
    fread_w(page->buf, sizeof(uint8_t), VDB_PAGE_SIZE, f);
    page->f = f;
    page->name = strdup_w(name);

    return page;
}

struct VdbPageList* _vdb_pagelist_alloc() {
    struct VdbPageList* pl = malloc_w(sizeof(struct VdbPageList));
    pl->count = 0;
    pl->capacity = 8;
    pl->pages = malloc_w(sizeof(struct VdbPage*) * pl->capacity);

    return pl;
}

void _vdb_pagelist_append_page(struct VdbPageList* pl, struct VdbPage* page) {
    if (pl->count == pl->capacity) {
        int old_cap = pl->capacity;
        pl->capacity *= 2;
        pl->pages = realloc_w(pl->pages, sizeof(struct VdbPage*) * pl->capacity, sizeof(struct VdbPage*) * old_cap);
    }

    pl->pages[pl->count++] = page;
}

void _vdb_pagelist_free(struct VdbPageList* pl) {
    for (uint32_t i = 0; i < pl->count; i++) {
        struct VdbPage* p = pl->pages[i];
        _vdb_pager_free_page(p);
    }

    free_w(pl->pages, sizeof(struct VdbPage*) * pl->capacity);
    free_w(pl, sizeof(struct VdbPageList));
}

void _vdb_pager_flush_page(struct VdbPage* page) {
    fseek_w(page->f, page->idx * VDB_PAGE_SIZE, SEEK_SET);
    fwrite_w(page->buf, sizeof(uint8_t), VDB_PAGE_SIZE, page->f);
}

struct VdbPager* vdb_pager_alloc() {
    struct VdbPager* pager = malloc_w(sizeof(struct VdbPager));
    pager->pages = _vdb_pagelist_alloc();
    return pager;
}

void vdb_pager_free(struct VdbPager* pager) {
    for (uint32_t i = 0; i < pager->pages->count; i++) {
        struct VdbPage* p = pager->pages->pages[i];
        if (p->dirty) {
            _vdb_pager_flush_page(p);
        }
    }
    _vdb_pagelist_free(pager->pages);
    free_w(pager, sizeof(struct VdbPager));
}

struct VdbPage* vdb_pager_pin_page(struct VdbPager* pager, char* name, FILE* f, uint32_t idx) {
    //search for page in buffer cache
    struct VdbPage* page = NULL;
    for (uint32_t i = 0; i < pager->pages->count; i++) {
        struct VdbPage* p = pager->pages->pages[i];
        if (p->idx == idx && !strncmp(p->name, name, strlen(name))) {
            page = p;
            break;
        }
    }

    //not cached, so read from disk
    if (!page) {
        //TODO: evict here if necessary before loading page.  Only evict a page if pin_count == 0
        page = _vdb_pager_load_page(name, f, idx);
        _vdb_pagelist_append_page(pager->pages, page);
    }

    page->pin_count++;

    return page;
}

void vdb_pager_unpin_page(struct VdbPage* page) {
    page->pin_count--;
}

void vdb_pager_evict_pages(struct VdbPager* pager, const char* name) {
    for (uint32_t i = 0; i < pager->pages->count; i++) {
        struct VdbPage* p = pager->pages->pages[i];
        if (!strncmp(name, p->name, strlen(name))) {
            _vdb_pager_free_page(p);
            pager->pages->pages[i] = pager->pages->pages[pager->pages->count - 1];
            pager->pages->count--;
            i--;
        }
    }
}
