#include <stdio.h>
#include <stdbool.h>

#include "vdb.h"

int main(int argc, char** argv) {
    argc = argc;
    argv = argv;

    VDBHANDLE h = vdb_open("school");

    struct VdbSchema* schema = vdb_alloc_schema(3, VDBF_INT, VDBF_STR, VDBF_BOOL);
    vdb_create_table(h, "students", schema);
    vdb_insert_record(h, "students", 1, "dogs", true);
    vdb_insert_record(h, "students", 2, "dogs", false);
    vdb_insert_record(h, "students", 3, "dogs", true);

    vdb_debug_print_tree(h, "students");
    //vdb_drop_table(h, "students");
    vdb_free_schema(schema);
    vdb_close(h);

    return 0;
}
