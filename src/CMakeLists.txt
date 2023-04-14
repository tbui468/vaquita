set(Sources
    main.c
    vdb.c
    util.c
    data.c
    tree.c
    )

set(Headers
    vdb.h
    util.h
    data.h
    tree.h
    )

add_executable(
    vdb
    ${Headers}
    ${Sources}
    )
