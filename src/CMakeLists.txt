set(Sources
    main.c
    vdb.c
    util.c
    tree.c
    pager.c
    )

set(Headers
    vdb.h
    util.h
    tree.h
    pager.h
    )

add_executable(
    vdb
    ${Headers}
    ${Sources}
    )
