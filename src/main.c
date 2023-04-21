#include <stdio.h>
#include <stdbool.h>

#include "vdb.h"

int main(int argc, char** argv) {
    argc = argc;
    argv = argv;

    VDBHANDLE h;
    if ((h = vdb_open("school"))) {
        printf("Opened 'school' database\n");    
    } else {
        printf("Failed to open 'school' database\n");
    }

    struct VdbSchema* schema = vdb_alloc_schema(3, VDBF_INT, VDBF_STR, VDBF_BOOL);

    if (vdb_create_table(h, "students", schema)) {
        printf("Created 'students' table\n");
    } else {
        printf("Failed to create 'students' table\n");
    }

    /*
    const char* words[] = {"cat", "dogs", "turtles"};
    for (int i = 1; i <= 100; i++) {
        vdb_insert_record(h, "students", i, words[i % 3], i % 2 == 0);
    }

    vdb_update_record(h, "students", 100, 0, "lions", true);
    vdb_delete_record(h, "students", 2);
    vdb_delete_record(h, "students", 3);

    int keys[] = {1, 2, 3, 49, 100, 101};

    vdb_debug_print_tree(h, "students");
    for (int i = 0; i < 6; i++) {
        struct VdbRecord* r = vdb_fetch_record(h, "students", keys[i]);
        if (r) {
            printf("key: %d, %ld, %.*s, %d\n", r->key, r->data[0].as.Int, r->data[1].as.Str->len, r->data[1].as.Str->start, r->data[2].as.Bool);
        } else {
            printf("not found!\n");
        }
    }*/

    /*
    if (vdb_drop_table(h, "students")) {
        printf("Dropped students table\n");
    } else {
        printf("Error: No students table\n");
    }*/

    vdb_free_schema(schema);

    vdb_close(h);

    return 0;
}
