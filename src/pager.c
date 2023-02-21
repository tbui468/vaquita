#include <string.h>

#include "pager.h"
#include "util.h"

struct VdbPager* pager_init() {
    struct VdbPager* p = malloc_w(sizeof(struct VdbPager));
    p->pages = NULL;
    p->count = 0;
    return p;
}

struct VdbPage* _pager_load_page(FILE* f, const char* table, uint32_t idx) {
    struct VdbPage* page = malloc_w(sizeof(struct VdbPage));
    page->table = strdup(table);
    page->idx = idx;
    page->buf = malloc_w(VDB_PAGE_SIZE);
    page->dirty = false;

    fseek_w(f, idx * VDB_PAGE_SIZE, SEEK_SET);
    fread_w(page->buf, sizeof(uint8_t), VDB_PAGE_SIZE, f);

    return page;
}

struct VdbPage* pager_get_page(struct VdbPager* pager, FILE* f, const char* table, uint32_t idx) {
    struct VdbPage* cur = pager->pages;
    while (cur) {
        if (cur->idx == idx && strcmp(cur->table, table) == 0)
            return cur;
        else
            cur = cur->next;
    }

    struct VdbPage* page = _pager_load_page(f, table, idx);
    page->next = pager->pages;
    pager->pages = page;

    return page;
}

void pager_write_page(struct VdbPage* p, uint8_t* buf, int off, int len) {
    memcpy(p->buf + off, buf, len);
    p->dirty = true;
}

