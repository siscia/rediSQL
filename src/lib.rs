#![warn(unused_extern_crates)]

mod commands;

extern crate env_logger;

#[macro_use]
extern crate log;
extern crate redisql_lib;
extern crate uuid;

use env_logger::{Builder as logBuilder, Target as logTarget};
use redisql_lib::redis as r;
use redisql_lib::redis::{
    register_function, register_function_with_keys,
    register_write_function, LoopData,
};
use redisql_lib::redis_type::Context;
use redisql_lib::sqlite as sql;
use redisql_lib::virtual_tables as vtab;
use std::ffi::CString;
use std::fs::{remove_file, File};
use std::ptr;
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use std::thread;
use uuid::Uuid;

#[cfg(feature = "pro")]
extern crate engine_pro;

#[cfg(feature = "pro")]
use engine_pro::{register, WriteAOF};

use commands::{
    CreateDB, CreateStatement, DeleteStatement, Exec, ExecStatement,
    MakeCopy, Query, QueryInto, QueryStatement, QueryStatementInto,
    UpdateStatement,
};

unsafe extern "C" fn rdb_save(
    rdb: *mut r::rm::ffi::RedisModuleIO,
    value: *mut std::os::raw::c_void,
) {
    let db: *mut r::DBKey =
        Box::into_raw(Box::from_raw(value as *mut r::DBKey));

    if (*db).in_memory {
        let path = format!("rediSQL_rdb_{}.sqlite", Uuid::new_v4());

        let db = (*db).loop_data.get_db();
        let conn = &db.lock().unwrap();
        match r::create_backup(conn, &path) {
            Err(e) => println!("{}", e),
            Ok(not_done)
                if !sql::backup_complete_with_done(not_done) =>
            {
                println!("Return NOT DONE: {}", not_done)
            }
            Ok(_) => match File::open(path.clone()) {
                Err(e) => println!("{}", e),
                Ok(f) => match r::write_file_to_rdb(f, rdb) {
                    Ok(()) => match remove_file(path) {
                        _ => (),
                    },
                    Err(_) => {
                        println!(
                            "Impossible to write the file \
                             in the rdb file"
                        );
                    }
                },
            },
        }
    }
}

unsafe extern "C" fn rdb_load(
    rdb: *mut r::rm::ffi::RedisModuleIO,
    _encoding_version: i32,
) -> *mut std::os::raw::c_void {
    let path = format!("rediSQL_rdb_{}.sqlite", Uuid::new_v4());

    let mut file = match File::create(path.clone()) {
        Err(_) => {
            println!("Was impossible to create a file!");
            return ptr::null_mut();
        }
        Ok(f) => f,
    };

    if r::write_rdb_to_file(&mut file, rdb).is_err() {
        println!("Was impossible to write the rdb file!");
        return ptr::null_mut();
    }

    let in_mem = match sql::RawConnection::open_connection(":memory:")
    {
        Err(_) => {
            println!("Was impossible to open the in memory db!");
            return ptr::null_mut();
        }
        Ok(in_mem) => in_mem,
    };

    let on_disk = match sql::RawConnection::open_connection(&path) {
        Err(_) => {
            println!("Error in opening the rdb database");
            return ptr::null_mut();
        }
        Ok(on_disk) => on_disk,
    };

    match r::make_backup(&on_disk, &in_mem) {
        Err(e) => {
            println!("{}", e);
            ptr::null_mut()
        }
        Ok(_) => {
            let (tx, rx) = channel();
            let conn = Arc::new(Mutex::new(in_mem));
            let redis_context = match vtab::register_modules(&conn) {
                Err(e) => {
                    println!("{}", e);
                    return ptr::null_mut();
                }
                Ok(redis_context) => redis_context,
            };
            let db =
                r::DBKey::new_from_arc(tx, conn, true, redis_context);
            let mut loop_data = db.loop_data.clone();

            thread::spawn(move || {
                r::listen_and_execute(&mut loop_data, &rx)
            });

            Box::into_raw(Box::new(db)) as *mut std::os::raw::c_void
        }
    }
}

unsafe extern "C" fn free_db(db_ptr: *mut ::std::os::raw::c_void) {
    let db: Box<r::DBKey> = Box::from_raw(db_ptr as *mut r::DBKey);
    let tx = &db.tx;

    match tx.send(r::Command::Stop) {
        _ => (),
    }
}

#[allow(non_snake_case)]
#[no_mangle]
pub extern "C" fn RedisModule_OnLoad(
    ctx: *mut r::rm::ffi::RedisModuleCtx,
    _argv: *mut *mut r::rm::ffi::RedisModuleString,
    _argc: i32,
) -> i32 {
    let ctx = Context::new(ctx);

    sql::disable_global_memory_statistics();

    logBuilder::new()
        .filter_level(log::LevelFilter::Debug)
        .target(logTarget::Stdout)
        .init();

    let c_data_type_name = CString::new("rediSQLDB").unwrap();
    let ptr_data_type_name = c_data_type_name.as_ptr();

    #[cfg(feature = "pro")]
    let mut types = r::rm::ffi::RedisModuleTypeMethods {
        version: 1,
        rdb_load: Some(rdb_load),
        rdb_save: Some(rdb_save),
        aof_rewrite: Some(WriteAOF),
        mem_usage: None,
        digest: None,
        free: Some(free_db),
    };

    #[cfg(not(feature = "pro"))]
    let mut types = r::rm::ffi::RedisModuleTypeMethods {
        version: 1,
        rdb_load: Some(rdb_load),
        rdb_save: Some(rdb_save),
        aof_rewrite: None,
        mem_usage: None,
        digest: None,
        free: Some(free_db),
    };

    let module_c_name = CString::new("rediSQL").unwrap();
    let module_ptr_name = module_c_name.as_ptr();
    if unsafe {
        r::rm::ffi::Export_RedisModule_Init(
            ctx.as_ptr(),
            module_ptr_name,
            1,
            r::rm::ffi::REDISMODULE_APIVER_1,
        )
    } == r::rm::ffi::REDISMODULE_ERR
    {
        return r::rm::ffi::REDISMODULE_ERR;
    }

    unsafe {
        r::rm::ffi::DBType = r::rm::ffi::RedisModule_CreateDataType
            .unwrap()(
            ctx.as_ptr(),
            ptr_data_type_name,
            1,
            &mut types,
        );
    }

    if unsafe { r::rm::ffi::DBType.is_null() } {
        return r::rm::ffi::REDISMODULE_ERR;
    }

    match register_write_function(&ctx, "REDISQL.CREATE_DB", CreateDB)
    {
        Ok(()) => (),
        Err(e) => return e,
    }

    match register_write_function(&ctx, "REDISQL.EXEC", Exec) {
        Ok(()) => (),
        Err(e) => return e,
    }

    match register_function(&ctx, "REDISQL.QUERY", "readonly", Query)
    {
        Ok(()) => (),
        Err(e) => return e,
    }

    match register_function_with_keys(
        &ctx,
        "REDISQL.QUERY.INTO",
        "readonly",
        1,
        2,
        1,
        QueryInto,
    ) {
        Ok(()) => (),
        Err(e) => return e,
    }

    match register_write_function(
        &ctx,
        "REDISQL.CREATE_STATEMENT",
        CreateStatement,
    ) {
        Ok(()) => (),
        Err(e) => return e,
    }

    match register_write_function(
        &ctx,
        "REDISQL.EXEC_STATEMENT",
        ExecStatement,
    ) {
        Ok(()) => (),
        Err(e) => return e,
    }

    match register_write_function(
        &ctx,
        "REDISQL.UPDATE_STATEMENT",
        UpdateStatement,
    ) {
        Ok(()) => (),
        Err(e) => return e,
    }

    match register_write_function(
        &ctx,
        "REDISQL.DELETE_STATEMENT",
        DeleteStatement,
    ) {
        Ok(()) => (),
        Err(e) => return e,
    }

    match register_function(
        &ctx,
        "REDISQL.QUERY_STATEMENT",
        "readonly",
        QueryStatement,
    ) {
        Ok(()) => (),
        Err(e) => return e,
    }

    match register_function_with_keys(
        &ctx,
        "REDISQL.QUERY_STATEMENT.INTO",
        "readonly",
        1,
        2,
        1,
        QueryStatementInto,
    ) {
        Ok(()) => (),
        Err(e) => return e,
    }

    match register_write_function(&ctx, "REDISQL.COPY", MakeCopy) {
        Ok(()) => (),
        Err(e) => return e,
    }

    #[cfg(feature = "pro")]
    match register(ctx) {
        Ok(()) => (),
        Err(e) => return e,
    }

    r::rm::ffi::REDISMODULE_OK
}
