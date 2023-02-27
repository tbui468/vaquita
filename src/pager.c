#include <string.h>
#include <stdlib.h>

#include "pager.h"
#include "util.h"

struct VdbPager* pager_init() {
    struct VdbPager* p = malloc_w(sizeof(struct VdbPager));
    p->pages = NULL;
    p->page_count = 0;
    return p;
}

struct VdbPage* _pager_load_page(FILE* f, uint32_t idx) {
    struct VdbPage* page = malloc_w(sizeof(struct VdbPage));
    page->file = f;
    page->idx = idx;
    page->buf = malloc_w(VDB_PAGE_SIZE);
    page->dirty = false;
    page->next = NULL;

    fseek_w(f, idx * VDB_PAGE_SIZE, SEEK_SET);
    fread_w(page->buf, sizeof(uint8_t), VDB_PAGE_SIZE, f);

    return page;
}

struct VdbPage* pager_get_page(struct VdbPager* pager, FILE* f, uint32_t idx) {
    struct VdbPage* cur = pager->pages;
    while (cur) {
        if (cur->idx == idx && f == cur->file)
            return cur;
        else
            cur = cur->next;
    }

    struct VdbPage* page = _pager_load_page(f, idx);
    page->next = pager->pages;
    pager->pages = page;
    pager->page_count++;

    return page;
}

uint32_t pager_allocate_page(FILE* f) {
    fseek_w(f, 0, SEEK_END);
    uint32_t idx = ftell_w(f) / VDB_PAGE_SIZE;
    uint8_t* buf = calloc_w(VDB_PAGE_SIZE, sizeof(uint8_t));
    fwrite_w(buf, sizeof(uint8_t), VDB_PAGE_SIZE, f);
    free(buf);
    return idx;
}
