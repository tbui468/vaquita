#include <string.h>


#include "value.h"
#include "util.h"

struct VdbValue vdbvalue_deserialize_string(uint8_t* buf) {
    struct VdbValue d;
    d.type = VDBT_TYPE_STR;
    d.block_idx = *((uint32_t*)(buf + sizeof(uint32_t) * 0));
    d.idxcell_idx = *((uint32_t*)(buf + sizeof(uint32_t) * 1));

    d.as.Str.len = *((uint32_t*)(buf + sizeof(uint32_t) * 2));
    d.as.Str.start = malloc_w(sizeof(char) * d.as.Str.len);
    memcpy(d.as.Str.start, buf + sizeof(uint32_t) * 3, d.as.Str.len);
    return d;
}

bool vdbvalue_is_null(struct VdbValue* d) {
    return d->type == VDBT_TYPE_NULL;
}
