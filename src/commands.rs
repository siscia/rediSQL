use std::ffi::{CStr, CString};
use std::sync::mpsc::channel;
use std::thread;

use redisql_lib::redis::{
    get_dbkey_from_name, reply_with_error_from_key_type,
    with_ch_and_loopdata, RedisReply,
};
use redisql_lib::redis_type::ReplicateVerbatim;
use redisql_lib::sqlite::{get_arc_connection, SQLite3Error};
use redisql_lib::virtual_tables as vtab;

use redisql_lib::redis as r;

#[cfg(feature = "pro")]
use engine_pro::Replicate;
#[cfg(not(feature = "pro"))]
use redisql_lib::redis::Replicate;

extern "C" fn reply_exec(
    ctx: *mut r::rm::ffi::RedisModuleCtx,
    _argv: *mut *mut r::rm::ffi::RedisModuleString,
    _argc: ::std::os::raw::c_int,
) -> i32 {
    let context = r::rm::Context::new(ctx);
    let result = unsafe {
        r::rm::ffi::RedisModule_GetBlockedClientPrivateData.unwrap()(
            context.as_ptr(),
        ) as *mut Result<r::QueryResult, SQLite3Error>
    };
    let result: Box<Result<r::QueryResult, SQLite3Error>> =
        unsafe { Box::from_raw(result) };
    match *result {
        Ok(query_result) => query_result.reply(&context),
        Err(error) => error.reply(&context),
    }
}

extern "C" fn reply_exec_statement(
    ctx: *mut r::rm::ffi::RedisModuleCtx,
    _argv: *mut *mut r::rm::ffi::RedisModuleString,
    _argc: ::std::os::raw::c_int,
) -> i32 {
    let context = r::rm::Context::new(ctx);
    let result = unsafe {
        r::rm::ffi::RedisModule_GetBlockedClientPrivateData.unwrap()(
            context.as_ptr(),
        ) as *mut Result<r::QueryResult, SQLite3Error>
    };
    let result: Box<Result<r::QueryResult, SQLite3Error>> =
        unsafe { Box::from_raw(result) };
    match *result {
        Ok(query_result) => query_result.reply(&context),
        Err(error) => error.reply(&context),
    }
}

extern "C" fn reply_create_statement(
    ctx: *mut r::rm::ffi::RedisModuleCtx,
    _argv: *mut *mut r::rm::ffi::RedisModuleString,
    _argc: ::std::os::raw::c_int,
) -> i32 {
    let context = r::rm::Context::new(ctx);
    let result = unsafe {
        r::rm::ffi::RedisModule_GetBlockedClientPrivateData.unwrap()(
            context.as_ptr(),
        ) as *mut Result<r::QueryResult, SQLite3Error>
    };
    let result: Box<Result<r::QueryResult, SQLite3Error>> =
        unsafe { Box::from_raw(result) };
    match *result {
        Ok(query_result) => query_result.reply(&context),
        Err(error) => error.reply(&context),
    }
}

extern "C" fn timeout(
    ctx: *mut r::rm::ffi::RedisModuleCtx,
    _argv: *mut *mut r::rm::ffi::RedisModuleString,
    _argc: ::std::os::raw::c_int,
) -> i32 {
    unsafe { r::rm::ffi::RedisModule_ReplyWithNull.unwrap()(ctx) }
}

extern "C" fn free_privdata(_arg: *mut ::std::os::raw::c_void) {}

#[allow(non_snake_case)]
pub extern "C" fn ExecStatement(
    ctx: *mut r::rm::ffi::RedisModuleCtx,
    argv: *mut *mut r::rm::ffi::RedisModuleString,
    argc: ::std::os::raw::c_int,
) -> i32 {
    let (context, argvector) = r::create_argument(ctx, argv, argc);

    match argvector.len() {
        0...2 => {
            let error = CString::new(
                "Wrong number of arguments, it \
                 needs at least 3",
            )
            .unwrap();
            unsafe {
                r::rm::ffi::RedisModule_ReplyWithError.unwrap()(
                    context.as_ptr(),
                    error.as_ptr(),
                )
            }
        }
        _ => with_ch_and_loopdata(
            context.as_ptr(),
            argvector[1],
            |ch_loopdata| match ch_loopdata {
                Err(key_type) => reply_with_error_from_key_type(
                    context.as_ptr(),
                    key_type,
                ),
                Ok((ch, _loopdata)) => {
                    let blocked_client = r::rm::BlockedClient {
                        client: unsafe {
                            r::rm::ffi::RedisModule_BlockClient
                                .unwrap()(
                                context.as_ptr(),
                                Some(reply_exec_statement),
                                Some(timeout),
                                Some(free_privdata),
                                10000,
                            )
                        },
                    };

                    let cmd = r::Command::ExecStatement {
                        identifier: argvector[2],
                        arguments: argvector[3..].to_vec(),
                        client: blocked_client,
                    };
                    match ch.send(cmd) {
                        Ok(()) => {
                            unsafe {
                                Replicate(
                                    &context,
                                    "REDISQL.EXEC_STATEMENT.NOW",
                                    argv,
                                    argc,
                                );
                            }
                            r::rm::ffi::REDISMODULE_OK
                        }
                        Err(_) => r::rm::ffi::REDISMODULE_OK,
                    }
                }
            },
        ),
    }
}

#[allow(non_snake_case)]
pub extern "C" fn QueryStatement(
    ctx: *mut r::rm::ffi::RedisModuleCtx,
    argv: *mut *mut r::rm::ffi::RedisModuleString,
    argc: ::std::os::raw::c_int,
) -> i32 {
    let (context, argvector) = r::create_argument(ctx, argv, argc);

    match argvector.len() {
        0...2 => {
            let error = CString::new(
                "Wrong number of arguments, it \
                 needs at least 3",
            )
            .unwrap();
            unsafe {
                r::rm::ffi::RedisModule_ReplyWithError.unwrap()(
                    context.as_ptr(),
                    error.as_ptr(),
                )
            }
        }
        _ => with_ch_and_loopdata(
            context.as_ptr(),
            argvector[1],
            |ch_loopdata| match ch_loopdata {
                Err(key_type) => reply_with_error_from_key_type(
                    context.as_ptr(),
                    key_type,
                ),
                Ok((ch, _loopdata)) => {
                    let blocked_client = r::rm::BlockedClient {
                        client: unsafe {
                            r::rm::ffi::RedisModule_BlockClient
                                .unwrap()(
                                context.as_ptr(),
                                Some(reply_exec_statement),
                                Some(timeout),
                                Some(free_privdata),
                                10000,
                            )
                        },
                    };

                    let cmd = r::Command::QueryStatement {
                        identifier: argvector[2],
                        arguments: argvector[3..].to_vec(),
                        return_method: r::ReturnMethod::Reply,
                        client: blocked_client,
                    };

                    match ch.send(cmd) {
                        Ok(()) => r::rm::ffi::REDISMODULE_OK,
                        Err(_) => r::rm::ffi::REDISMODULE_OK,
                    }
                }
            },
        ),
    }
}

#[allow(non_snake_case)]
pub extern "C" fn QueryStatementInto(
    ctx: *mut r::rm::ffi::RedisModuleCtx,
    argv: *mut *mut r::rm::ffi::RedisModuleString,
    argc: ::std::os::raw::c_int,
) -> i32 {
    let (context, argvector) = r::create_argument(ctx, argv, argc);

    match argvector.len() {
        0...3 => {
            let error = CString::new(
                "Wrong number of arguments, it \
                 needs at least 4",
            )
            .unwrap();
            unsafe {
                r::rm::ffi::RedisModule_ReplyWithError.unwrap()(
                    context.as_ptr(),
                    error.as_ptr(),
                )
            }
        }
        _ => {
            let stream_name = argvector[1];
            let db = argvector[2];

            with_ch_and_loopdata(
                context.as_ptr(),
                db,
                |ch_loopdata| match ch_loopdata {
                    Err(key_type) => reply_with_error_from_key_type(
                        context.as_ptr(),
                        key_type,
                    ),
                    Ok((ch, _loopdata)) => {
                        let blocked_client = r::rm::BlockedClient {
                            client: unsafe {
                                r::rm::ffi::RedisModule_BlockClient
                                    .unwrap()(
                                    context.as_ptr(),
                                    Some(reply_exec_statement),
                                    Some(timeout),
                                    Some(free_privdata),
                                    10000,
                                )
                            },
                        };

                        let cmd = r::Command::QueryStatement {
                            identifier: argvector[3],
                            arguments: argvector[4..].to_vec(),
                            return_method: r::ReturnMethod::Stream {
                                name: stream_name,
                            },
                            client: blocked_client,
                        };

                        match ch.send(cmd) {
                            Ok(()) => r::rm::ffi::REDISMODULE_OK,
                            Err(_) => r::rm::ffi::REDISMODULE_OK,
                        }
                    }
                },
            )
        }
    }
}

#[allow(non_snake_case)]
pub extern "C" fn Exec(
    ctx: *mut r::rm::ffi::RedisModuleCtx,
    argv: *mut *mut r::rm::ffi::RedisModuleString,
    argc: ::std::os::raw::c_int,
) -> i32 {
    let (context, argvector) = r::create_argument(ctx, argv, argc);
    match argvector.len() {
        3 => with_ch_and_loopdata(
            context.as_ptr(),
            argvector[1],
            |leaky_db| match leaky_db {
                Err(key_type) => reply_with_error_from_key_type(
                    context.as_ptr(),
                    key_type,
                ),
                Ok((ch, _loopdata)) => {
                    debug!("Exec | GotDB");
                    let blocked_client = r::rm::BlockedClient {
                        client: unsafe {
                            r::rm::ffi::RedisModule_BlockClient
                                .unwrap()(
                                context.as_ptr(),
                                Some(reply_exec),
                                Some(timeout),
                                Some(free_privdata),
                                10000,
                            )
                        },
                    };
                    debug!("Exec | BlockedClient");

                    let cmd = r::Command::Exec {
                        query: argvector[2],
                        client: blocked_client,
                    };
                    debug!("Exec | Create Command");
                    match ch.send(cmd) {
                        Ok(()) => {
                            unsafe {
                                Replicate(
                                    &context,
                                    "REDISQL.EXEC.NOW",
                                    argv,
                                    argc,
                                );
                            }
                            r::rm::ffi::REDISMODULE_OK
                        }
                        Err(_) => r::rm::ffi::REDISMODULE_OK,
                    }
                }
            },
        ),
        n => {
            let error = CString::new(format!(
                "Wrong number of arguments, it \
                 accepts 3, you provide {}",
                n
            ))
            .unwrap();
            unsafe {
                r::rm::ffi::RedisModule_ReplyWithError.unwrap()(
                    context.as_ptr(),
                    error.as_ptr(),
                )
            }
        }
    }
}

#[allow(non_snake_case)]
pub extern "C" fn Query(
    ctx: *mut r::rm::ffi::RedisModuleCtx,
    argv: *mut *mut r::rm::ffi::RedisModuleString,
    argc: ::std::os::raw::c_int,
) -> i32 {
    let (context, argvector) = r::create_argument(ctx, argv, argc);
    match argvector.len() {
        3 => with_ch_and_loopdata(
            context.as_ptr(),
            argvector[1],
            |ch_loopdata| match ch_loopdata {
                Err(key_type) => reply_with_error_from_key_type(
                    context.as_ptr(),
                    key_type,
                ),
                Ok((ch, _loopdata)) => {
                    let blocked_client = r::rm::BlockedClient {
                        client: unsafe {
                            r::rm::ffi::RedisModule_BlockClient
                                .unwrap()(
                                context.as_ptr(),
                                Some(reply_exec),
                                Some(timeout),
                                Some(free_privdata),
                                10000,
                            )
                        },
                    };

                    let cmd = r::Command::Query {
                        query: argvector[2],
                        return_method: r::ReturnMethod::Reply,
                        client: blocked_client,
                    };
                    match ch.send(cmd) {
                        Ok(()) => r::rm::ffi::REDISMODULE_OK,
                        Err(_) => r::rm::ffi::REDISMODULE_OK,
                    }
                }
            },
        ),
        n => {
            let error = CString::new(format!(
                "Wrong number of arguments, it \
                 accepts 3, you provide {}",
                n
            ))
            .unwrap();
            unsafe {
                r::rm::ffi::RedisModule_ReplyWithError.unwrap()(
                    context.as_ptr(),
                    error.as_ptr(),
                )
            }
        }
    }
}

#[allow(non_snake_case)]
pub extern "C" fn QueryInto(
    ctx: *mut r::rm::ffi::RedisModuleCtx,
    argv: *mut *mut r::rm::ffi::RedisModuleString,
    argc: ::std::os::raw::c_int,
) -> i32 {
    let (context, argvector) = r::create_argument(ctx, argv, argc);
    match argvector.len() {
        4 => {
            let stream_name = argvector[1];
            let db = argvector[2];
            with_ch_and_loopdata(
                context.as_ptr(),
                db,
                |ch_loopdata| match ch_loopdata {
                    Err(key_type) => reply_with_error_from_key_type(
                        context.as_ptr(),
                        key_type,
                    ),
                    Ok((ch, _loopdata)) => {
                        let blocked_client = r::rm::BlockedClient {
                            client: unsafe {
                                r::rm::ffi::RedisModule_BlockClient
                                    .unwrap()(
                                    context.as_ptr(),
                                    Some(reply_exec),
                                    Some(timeout),
                                    Some(free_privdata),
                                    10000,
                                )
                            },
                        };

                        let cmd = r::Command::Query {
                            query: argvector[3],
                            return_method: r::ReturnMethod::Stream {
                                name: stream_name,
                            },
                            client: blocked_client,
                        };
                        match ch.send(cmd) {
                            Ok(()) => r::rm::ffi::REDISMODULE_OK,
                            Err(_) => r::rm::ffi::REDISMODULE_OK,
                        }
                    }
                },
            )
        }
        n => {
            let error = CString::new(format!(
                "Wrong number of arguments, it \
                 accepts 4, you provide {}",
                n
            ))
            .unwrap();
            unsafe {
                r::rm::ffi::RedisModule_ReplyWithError.unwrap()(
                    context.as_ptr(),
                    error.as_ptr(),
                )
            }
        }
    }
}

#[allow(non_snake_case)]
pub extern "C" fn CreateStatement(
    ctx: *mut r::rm::ffi::RedisModuleCtx,
    argv: *mut *mut r::rm::ffi::RedisModuleString,
    argc: ::std::os::raw::c_int,
) -> i32 {
    let (context, argvector) = r::create_argument(ctx, argv, argc);
    match argvector.len() {
        4 => {
            with_ch_and_loopdata(
                context.as_ptr(),
                argvector[1],
                |ch_loopdata| match ch_loopdata {
                    Err(key_type) => reply_with_error_from_key_type(
                        context.as_ptr(),
                        key_type,
                    ),
                    Ok((ch, _loopdata)) => {
                        let blocked_client = r::rm::BlockedClient {
                            client: unsafe {
                                r::rm::ffi::RedisModule_BlockClient
                                    .unwrap()(
                                    context.as_ptr(),
                                    Some(reply_create_statement),
                                    Some(timeout),
                                    Some(free_privdata),
                                    10000,
                                )
                            },
                        };
                        let cmd = r::Command::CompileStatement {
                            identifier: argvector[2],
                            statement: argvector[3],
                            client: blocked_client,
                        };
                        match ch.send(cmd) {
                            Ok(()) => {
                                unsafe {
                                    Replicate(&context, "REDISQL.CREATE_STATEMENT.NOW", argv, argc);
                                }
                                r::rm::ffi::REDISMODULE_OK
                            }
                            Err(_) => r::rm::ffi::REDISMODULE_OK,
                        }
                    }
                },
            )
        }

        _ => {
            let error = CString::new(
                "Wrong number of arguments, it \
                 accepts 4",
            )
            .unwrap();
            unsafe {
                r::rm::ffi::RedisModule_ReplyWithError.unwrap()(
                    context.as_ptr(),
                    error.as_ptr(),
                )
            }
        }
    }
}

#[allow(non_snake_case)]
pub extern "C" fn UpdateStatement(
    ctx: *mut r::rm::ffi::RedisModuleCtx,
    argv: *mut *mut r::rm::ffi::RedisModuleString,
    argc: ::std::os::raw::c_int,
) -> i32 {
    let (context, argvector) = r::create_argument(ctx, argv, argc);
    match argvector.len() {
        4 => {
            with_ch_and_loopdata(
                context.as_ptr(),
                argvector[1],
                |ch_loopdata| match ch_loopdata {
                    Err(key_type) => reply_with_error_from_key_type(
                        context.as_ptr(),
                        key_type,
                    ),
                    Ok((ch, _loopdata)) => {
                        let blocked_client = r::rm::BlockedClient {
                            client: unsafe {
                                r::rm::ffi::RedisModule_BlockClient
                                    .unwrap()(
                                    context.as_ptr(),
                                    Some(reply_create_statement),
                                    Some(timeout),
                                    Some(free_privdata),
                                    10000,
                                )
                            },
                        };

                        let cmd = r::Command::UpdateStatement {
                            identifier: argvector[2],
                            statement: argvector[3],
                            client: blocked_client,
                        };

                        match ch.send(cmd) {
                            Ok(()) => {
                                unsafe {
                                    Replicate(&context, "REDISQL.UPDATE_STATEMENT.NOW", argv, argc);
                                }
                                r::rm::ffi::REDISMODULE_OK
                            }
                            Err(_) => r::rm::ffi::REDISMODULE_OK,
                        }
                    }
                },
            )
        }

        _ => {
            let error = CString::new(
                "Wrong number of arguments, it \
                 accepts 4",
            )
            .unwrap();
            unsafe {
                r::rm::ffi::RedisModule_ReplyWithError.unwrap()(
                    context.as_ptr(),
                    error.as_ptr(),
                )
            }
        }
    }
}

#[allow(non_snake_case)]
pub extern "C" fn DeleteStatement(
    ctx: *mut r::rm::ffi::RedisModuleCtx,
    argv: *mut *mut r::rm::ffi::RedisModuleString,
    argc: ::std::os::raw::c_int,
) -> i32 {
    let (context, argvector) = r::create_argument(ctx, argv, argc);
    match argvector.len() {
        3 => with_ch_and_loopdata(
            context.as_ptr(),
            argvector[1],
            |ch_loopdata| {
                match ch_loopdata {
                    Err(key_type) => reply_with_error_from_key_type(
                        context.as_ptr(),
                        key_type,
                    ),
                    Ok((ch, _loopdata)) => {
                        //let ch = &db.tx;
                        //let _loopdata = &db.loop_data;
                        let blocked_client = r::rm::BlockedClient {
                            client: unsafe {
                                r::rm::ffi::RedisModule_BlockClient
                                    .unwrap()(
                                    context.as_ptr(),
                                    Some(reply_create_statement),
                                    Some(timeout),
                                    Some(free_privdata),
                                    10000,
                                )
                            },
                        };

                        let cmd = r::Command::DeleteStatement {
                            identifier: argvector[2],
                            client: blocked_client,
                        };
                        match ch.send(cmd) {
                            Ok(()) => {
                                unsafe {
                                    Replicate(&context, "REDISQL.DELETE_STATEMENT.NOW", argv, argc);
                                }
                                r::rm::ffi::REDISMODULE_OK
                            }
                            Err(_) => r::rm::ffi::REDISMODULE_OK,
                        }
                    }
                }
            },
        ),
        _ => {
            let error = CString::new(
                "Wrong number of arguments, it \
                 accepts 3",
            )
            .unwrap();
            unsafe {
                r::rm::ffi::RedisModule_ReplyWithError.unwrap()(
                    context.as_ptr(),
                    error.as_ptr(),
                )
            }
        }
    }
}

#[allow(non_snake_case)]
pub extern "C" fn CreateDB(
    ctx: *mut r::rm::ffi::RedisModuleCtx,
    argv: *mut *mut r::rm::ffi::RedisModuleString,
    argc: ::std::os::raw::c_int,
) -> i32 {
    let (context, argvector) = r::create_argument(ctx, argv, argc);

    match argvector.len() {
        2 | 3 => {
            let key_name =
                r::rm::RMString::new(&context, argvector[1]);
            let key = unsafe {
                r::rm::ffi::Export_RedisModule_OpenKey(
                    context.as_ptr(),
                    key_name.as_ptr(),
                    r::rm::ffi::REDISMODULE_WRITE,
                )
            };
            let safe_key = r::RedisKey { key };
            match unsafe {
                r::rm::ffi::RedisModule_KeyType.unwrap()(safe_key.key)
            } {
                r::rm::ffi::REDISMODULE_KEYTYPE_EMPTY => {
                    let (path, in_memory): (&str, bool) =
                        match argvector.len() {
                            3 => (argvector[2], false),
                            _ => (":memory:", true),
                        };
                    match get_arc_connection(path) {
                        Ok(rc) => {
                            match r::create_metadata_table(rc.clone())
                                .and_then(|_| {
                                    r::enable_foreign_key(rc.clone())
                                })
                                .and_then(|_| {
                                    vtab::register_modules(&rc)
                                }) {
                                Err(e) => e.reply(&context),
                                Ok(mut vtab_context) => {
                                    let (tx, rx) = channel();
                                    let db = r::DBKey::new_from_arc(
                                        tx,
                                        rc,
                                        in_memory,
                                        vtab_context,
                                    );
                                    let mut loop_data =
                                        db.loop_data.clone();
                                    thread::spawn(move || {
                                        r::listen_and_execute(
                                            &mut loop_data,
                                            &rx,
                                        );
                                    });
                                    let ptr =
                                        Box::into_raw(Box::new(db));
                                    let type_set = unsafe {
                                        r::rm::ffi::RedisModule_ModuleTypeSetValue.unwrap()(
                                        safe_key.key,
                                        r::rm::ffi::DBType,
                                        ptr as *mut std::os::raw::c_void,
                                    )
                                    };

                                    match type_set {
                                    r::rm::ffi::REDISMODULE_OK => {
                                        let ok =
                                            r::QueryResult::OK {};
                                        ReplicateVerbatim(&context);
                                        ok.reply(&context)
                                    }
                                    r::rm::ffi::REDISMODULE_ERR => {
                                        let err = CString::new(
                                            "ERR - Error in saving the database inside Redis",
                                        ).unwrap();
                                        unsafe {
                                            r::rm::ffi::RedisModule_ReplyWithSimpleString.unwrap()(
                                                context.as_ptr(),
                                                err.as_ptr(),
                                            )
                                        }
                                    }
                                    _ => {
                                        let err = CString::new("ERR - Error unknow").unwrap();
                                        unsafe {
                                            r::rm::ffi::RedisModule_ReplyWithSimpleString.unwrap()(
                                                context.as_ptr(),
                                                err.as_ptr(),
                                            )
                                        }
                                    }
                                }
                                }
                            }
                        }
                        Err(_) => {
                            let error = CString::new(
                                "Err - Error \
                                 opening the in \
                                 memory databade",
                            )
                            .unwrap();
                            unsafe {
                                r::rm::ffi::RedisModule_ReplyWithError
                                    .unwrap()(
                                    context.as_ptr(),
                                    error.as_ptr(),
                                )
                            }
                        }
                    }
                }
                _ => {
                    let error = CStr::from_bytes_with_nul(
                        r::rm::ffi::REDISMODULE_ERRORMSG_WRONGTYPE,
                    )
                    .unwrap();
                    unsafe {
                        r::rm::ffi::RedisModule_ReplyWithError
                            .unwrap()(
                            context.as_ptr(),
                            error.as_ptr(),
                        )
                    }
                }
            }
        }
        _ => {
            let error = CString::new(
                "Wrong number of arguments, it \
                 accepts 2 or 3",
            )
            .unwrap();
            unsafe {
                r::rm::ffi::RedisModule_ReplyWithError.unwrap()(
                    context.as_ptr(),
                    error.as_ptr(),
                )
            }
        }
    }
}

#[allow(non_snake_case)]
pub extern "C" fn MakeCopy(
    ctx: *mut r::rm::ffi::RedisModuleCtx,
    argv: *mut *mut r::rm::ffi::RedisModuleString,
    argc: ::std::os::raw::c_int,
) -> i32 {
    debug!("MakeCopy | Start");
    let (context, argvector) = r::create_argument(ctx, argv, argc);

    if argvector.len() != 3 {
        let error = CString::new(
            "Wrong number of arguments, it accepts exactly 3",
        )
        .unwrap();
        return unsafe {
            r::rm::ffi::RedisModule_ReplyWithError.unwrap()(
                context.as_ptr(),
                error.as_ptr(),
            )
        };
    }

    let source_db =
        get_dbkey_from_name(context.as_ptr(), argvector[1]);
    if source_db.is_err() {
        let error =
            CString::new("Error in opening the SOURCE database")
                .unwrap();
        return unsafe {
            r::rm::ffi::RedisModule_ReplyWithError.unwrap()(
                context.as_ptr(),
                error.as_ptr(),
            )
        };
    }
    let source_db = source_db.unwrap();

    let dest_db = get_dbkey_from_name(context.as_ptr(), argvector[2]);
    if dest_db.is_err() {
        let error =
            CString::new("Error in opening the DESTINATION database")
                .unwrap();
        return unsafe {
            r::rm::ffi::RedisModule_ReplyWithError.unwrap()(
                context.as_ptr(),
                error.as_ptr(),
            )
        };
    }
    let dest_db = dest_db.unwrap();

    let blocked_client = r::rm::BlockedClient {
        client: unsafe {
            r::rm::ffi::RedisModule_BlockClient.unwrap()(
                context.as_ptr(),
                Some(reply_create_statement),
                Some(timeout),
                Some(free_privdata),
                10000,
            )
        },
    };

    let ch = &source_db.tx.clone();

    let cmd = r::Command::MakeCopy {
        source: source_db,
        destination: dest_db,
        client: blocked_client,
    };

    debug!("MakeCopy | End");
    match ch.send(cmd) {
        Ok(()) => {
            debug!("MakeCopy | Successfully send command");
            unsafe {
                Replicate(&context, "REDISQL.COPY.NOW", argv, argc);
            }
            r::rm::ffi::REDISMODULE_OK
        }
        Err(_) => r::rm::ffi::REDISMODULE_OK,
    }
}
