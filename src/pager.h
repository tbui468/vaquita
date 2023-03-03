#ifndef VDB_PAGER_H
#define VDB_PAGER_H

#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>

#define VDB_PAGE_SIZE 512

struct VdbPager {
    struct VdbPage* pages; 
    uint32_t page_count;
};

struct VdbPage {
    FILE* file;
    uint32_t idx;
    uint8_t* buf;
    bool dirty;
    struct VdbPage* next;
};

struct VdbPager* pager_init();
struct VdbPage* pager_get_page(struct VdbPager* pager, FILE* f, uint32_t idx);
uint32_t pager_allocate_page(FILE* f);


#endif //VDB_PAGER_H
