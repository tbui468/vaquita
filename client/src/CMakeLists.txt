set(Sources
    main.c
    )

set(Headers
    client.h
    )

add_executable(
    vdbclient
    ${Headers}
    ${Sources}
    )
