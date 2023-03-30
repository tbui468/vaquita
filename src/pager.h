#ifndef VDB_PAGER_H
#define VDB_PAGER_H

#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>

#define VDB_PAGE_SIZE 512
#define VDB_OFFSETS_START 256

struct VdbPager {
    struct VdbPage* pages; 
    uint32_t page_count;
};

struct VdbPage {
    uint8_t* buf;
    uint32_t idx;
    FILE* f;
    struct VdbPage* next;
    bool locked;
};

struct VdbPager* pager_init();
struct VdbPage* pager_get_page(struct VdbPager* pager, FILE* f, uint32_t idx);
uint32_t pager_allocate_page(FILE* f);
void pager_flush_page(struct VdbPage* page);


#endif //VDB_PAGER_H
