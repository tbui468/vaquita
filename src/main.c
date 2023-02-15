#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "vdb.h"

void free_data(struct VdbData* data) {
    for (uint32_t i = 0; i < data->count; i++) {
        struct VdbDatum d = data->data[i];
        if (d.type == VDBF_STR) {
            free(d.as.Str);
        }
    }

    free(data->data);
}

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

void print_data(struct VdbData* result) {
    printf("%ld, %.*s, %d\n", result->data[0].as.Int, result->data[1].as.Str->len, result->data[1].as.Str->start, result->data[2].as.Bool);
    free_data(result);
}

int main(int argc, char** argv) {
    argc = argc;
    argv = argv;

    VDBHANDLE h = vdb_create("test");

    enum VdbField fields[] = { VDBF_INT, VDBF_STR, VDBF_BOOL };
    struct VdbSchema s = { fields, 3 };
    if (vdb_create_table(h, "students", &s) != 0)
        printf("failed to create table\n");

    if (vdb_create_table(h, "teachers", &s) != 0)
        printf("failed to create table\n");

//    if(vdb_drop_table(h, "students") != 0)
//        printf("failed to drop table\n");
    if (vdb_drop_table(h, "teachers") != 0)
        printf("failed to drop table\n");
/*
    struct VdbData d1;
    make_data(&d1, true, 42, "dog");
    struct VdbData d2;
    make_data(&d2, false, 9, "catsss");
    struct VdbData d3;
    make_data(&d3, true, 11, "birds");

    if (vdb_insert_record(h, "students", d1) != 0)
        printf("failed to insert record\n");
    if (vdb_insert_record(h, "students", d2) != 0)
        printf("failed to insert record\n");
    if (vdb_insert_record(h, "students", d3) != 0)
        printf("failed to insert record\n");

    free_data(&d1);
    free_data(&d2);
    free_data(&d3);

    struct VdbData* result;
    if ((result = vdb_fetch_record(h, "students", 0)))
        print_data(result);
    if ((result = vdb_fetch_record(h, "students", 1)))
        print_data(result);
    if ((result = vdb_fetch_record(h, "students", 2)))
        print_data(result);

    if (vdb_delete_record(h, "students", 0) != 0)
        printf("failed to delete record\n");
    if (vdb_delete_record(h, "students", 2) != 0)
        printf("failed to delete record\n");

    if ((result = vdb_fetch_record(h, "students", 0)))
        print_data(result);
    if ((result = vdb_fetch_record(h, "students", 1)))
        print_data(result);
    if ((result = vdb_fetch_record(h, "students", 2)))
        print_data(result);*/

    vdb_close(h);

    return 0;
}
