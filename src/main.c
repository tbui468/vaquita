#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "vdb.h"

void make_data(struct VdbData* d, bool a, uint64_t b, char* c) {
    struct VdbString* str = malloc(sizeof(struct VdbString));
    str->start = c;
    str->len = strlen(c);

    struct VdbDatum* data = malloc(sizeof(struct VdbDatum) * 3);
    data[0].type = VDBF_INT;
    data[0].as.Int = b;
    data[1].type = VDBF_STR;
    data[1].as.Str = str;
    data[2].type = VDBF_BOOL;
    data[2].as.Bool = a;
    d->data = data;
    d->count = 3;
}

int main(int argc, char** argv) {
    argc = argc;
    argv = argv;

    VDBHANDLE h = vdb_open("test");

    enum VdbField fields[] = { VDBF_INT, VDBF_STR, VDBF_BOOL };
    struct VdbSchema s = { fields, 3 };
    if (vdb_create_table(h, "students", s) != 0)
        printf("failed to create table\n");
//    if (vdb_create_table(h, "teachers", s) != 0)
//        printf("failed to create table\n");

//    if(vdb_drop_table(h, "students") != 0)
//        printf("failed to drop table\n");
//    if (vdb_drop_table(h, "teachers") != 0)
//        printf("failed to drop table\n");

    struct VdbData d1;
    make_data(&d1, true, 42, "dog");
    struct VdbData d2;
    make_data(&d2, false, 9, "cat");

    if (vdb_insert_record(h, "students", d2) != 0)
        printf("failed to insert record\n");
    if (vdb_insert_record(h, "students", d1) != 0)
        printf("failed to insert record\n");

    vdb_close(h);

    return 0;
}
