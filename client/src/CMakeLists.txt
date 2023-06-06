set(Sources
    main.c
    client.c
    )

set(Headers
    client.h
    )

add_executable(
    vdbclient
    ${Headers}
    ${Sources}
    )
