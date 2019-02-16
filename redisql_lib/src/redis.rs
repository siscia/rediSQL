extern crate fnv;

use self::fnv::FnvHashMap;
use std;
use std::cell::RefCell;
use std::clone::Clone;
use std::collections::hash_map::Entry;
use std::error;
use std::ffi::{CStr, CString};
use std::fmt;
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::os::raw::{c_char, c_long};
use std::slice;
use std::str;
use std::string;
use std::sync::mpsc::{Receiver, RecvError, Sender};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

pub use redis_type as rm;
use redis_type::{BlockedClient, Context, OpenKey, ReplyWithError};

use redisql_error as err;
use redisql_error::RediSQLError;

use sqlite::StatementTrait;

use community_statement::MultiStatement;

use sqlite as sql;

#[derive(Clone)]
pub struct ReplicationBook {
    data: Arc<RwLock<FnvHashMap<String, (MultiStatement, bool)>>>,
    db: Arc<Mutex<sql::RawConnection>>,
}

pub trait StatementCache<'a> {
    fn new(&Arc<Mutex<sql::RawConnection>>) -> Self;
    fn is_statement_present(&self, &str) -> bool;
    fn insert_new_statement(
        &mut self,
        identifier: &str,
        statement: &str,
    ) -> Result<QueryResult, RediSQLError>;
    fn delete_statement(
        &mut self,
        &str,
    ) -> Result<QueryResult, RediSQLError>;
    fn update_statement(
        &mut self,
        identifier: &str,
        statement: &str,
    ) -> Result<QueryResult, RediSQLError>;
    fn exec_statement(
        &self,
        &str,
        &[&str],
    ) -> Result<QueryResult, RediSQLError>;
    fn query_statement(
        &self,
        &str,
        &[&str],
    ) -> Result<QueryResult, RediSQLError>;
}

impl<'a> StatementCache<'a> for ReplicationBook {
    fn new(db: &Arc<Mutex<sql::RawConnection>>) -> Self {
        ReplicationBook {
            data: Arc::new(RwLock::new(FnvHashMap::default())),
            db: Arc::clone(db),
        }
    }

    fn is_statement_present(&self, identifier: &str) -> bool {
        self.data.read().unwrap().contains_key(identifier)
    }

    fn insert_new_statement(
        &mut self,
        identifier: &str,
        statement: &str,
    ) -> Result<QueryResult, RediSQLError> {
        let db = self.db.clone();
        let mut map = self.data.write().unwrap();
        match map.entry(identifier.to_owned()) {
            Entry::Vacant(v) => {
                let stmt =
                    create_statement(db, identifier, statement)?;
                let read_only = stmt.is_read_only();
                v.insert((stmt, read_only));
                Ok(QueryResult::OK {})
            }
            Entry::Occupied(_) => {
                let debug = String::from("Statement already present");
                let description = String::from(
                    "The statement is already present in the database, try with UPDATE_STATEMENT",
                );
                Err(RediSQLError::new(debug, description))
            }
        }
    }

    fn delete_statement(
        &mut self,
        identifier: &str,
    ) -> Result<QueryResult, RediSQLError> {
        let db = self.db.clone();
        let mut map = self.data.write().unwrap();
        match map.entry(identifier.to_owned()) {
            Entry::Vacant(_) => {
                let debug = String::from("Statement not present.");
                let description = String::from(
                    "The statement is not present in the database, impossible to delete it.",
                );
                Err(RediSQLError::new(debug, description))
            }
            Entry::Occupied(o) => {
                remove_statement(&db, identifier)?;
                o.remove_entry();
                Ok(QueryResult::OK {})
            }
        }
    }

    fn update_statement(
        &mut self,
        identifier: &str,
        statement: &str,
    ) -> Result<QueryResult, RediSQLError> {
        let db = self.db.clone();
        let mut map = self.data.write().unwrap();
        match map.entry(identifier.to_owned()) {
            Entry::Vacant(_) => {
                let debug = String::from("Statement not present.");
                let description = String::from(
                    "The statement is not present in the database, impossible to update it.",
                );
                Err(RediSQLError::new(debug, description))
            }
            Entry::Occupied(mut o) => {
                let stmt =
                    update_statement(&db, identifier, statement)?;
                let read_only = stmt.is_read_only();
                o.insert((stmt, read_only));
                Ok(QueryResult::OK {})
            }
        }
    }

    fn query_statement(
        &self,
        identifier: &str,
        args: &[&str],
    ) -> Result<QueryResult, RediSQLError> {
        let map = self.data.read().unwrap();
        match map.get(identifier) {
            None => {
                let debug = String::from("No statement found");
                let description = String::from(
                    "The statement is not present in the database",
                );
                Err(RediSQLError::new(debug, description))
            }
            Some(&(ref stmt, true)) => {
                stmt.reset();
                let stmt = bind_statement(stmt, args)?;
                let cursor = stmt.execute()?;
                Ok(QueryResult::from(cursor))
            }
            Some(&(_, false)) => {
                let debug = String::from("Not read only statement");
                let description = String::from("Statement is not read only but it may modify the database, use `EXEC_STATEMENT` instead.",);
                Err(RediSQLError::new(debug, description))
            }
        }
    }

    fn exec_statement(
        &self,
        identifier: &str,
        args: &[&str],
    ) -> Result<QueryResult, RediSQLError> {
        let map = self.data.read().unwrap();
        match map.get(identifier) {
            None => {
                let debug = String::from("No statement found");
                let description = String::from(
                    "The statement is not present in the database",
                );
                Err(RediSQLError::new(debug, description))
            }
            Some(&(ref stmt, _)) => {
                stmt.reset();
                let stmt = bind_statement(stmt, args)?;
                let cursor = stmt.execute()?;
                Ok(QueryResult::from(cursor))
                //Ok(cursor_to_query_result(cursor))
            }
        }
    }
}

type ProtectedRedisContext =
    Arc<Mutex<Arc<Mutex<RefCell<Option<Context>>>>>>;

pub struct RedisContextSet<'external>(
    MutexGuard<'external, Arc<Mutex<RefCell<Option<Context>>>>>,
);

impl<'a> RedisContextSet<'a> {
    fn new(
        ctx: Context,
        redis_ctx: &'a ProtectedRedisContext,
    ) -> RedisContextSet<'a> {
        let wrap = redis_ctx.lock().unwrap();
        {
            let locked = wrap.lock().unwrap();
            *locked.borrow_mut() = Some(ctx);
        }
        RedisContextSet(wrap)
    }
    pub fn release(&self) -> Context {
        let locked = self.0.lock().unwrap();
        let none_to_ctx = RefCell::new(None);
        locked.swap(&none_to_ctx);
        none_to_ctx.into_inner().unwrap()
    }
}

impl<'a> Drop for RedisContextSet<'a> {
    fn drop(&mut self) {
        let locked = self.0.lock().unwrap();
        *locked.borrow_mut() = None;
        debug!("RedisContextSet | Drop");
    }
}

#[derive(Clone)]
pub struct Loop {
    db: Arc<Mutex<sql::RawConnection>>,
    replication_book: ReplicationBook,
    redis_context: ProtectedRedisContext,
}

unsafe impl Send for Loop {}

pub trait LoopData {
    fn get_replication_book(&self) -> ReplicationBook;
    fn get_db(&self) -> Arc<Mutex<sql::RawConnection>>;
    fn set_rc(&self, ctx: Context) -> RedisContextSet;
    fn with_contex_set<F>(&self, ctx: Context, f: F)
    where
        F: Fn(RedisContextSet) -> ();
}

impl LoopData for Loop {
    fn get_replication_book(&self) -> ReplicationBook {
        self.replication_book.clone()
    }
    fn get_db(&self) -> Arc<Mutex<sql::RawConnection>> {
        Arc::clone(&self.db)
    }
    fn set_rc(&self, ctx: Context) -> RedisContextSet {
        debug!("set_rc | enter");
        let wrap = self.redis_context.lock().unwrap();

        {
            let locked = wrap.lock().unwrap();
            *locked.borrow_mut() = Some(ctx);
        }

        debug!("set_rc | exit");
        RedisContextSet(wrap)
    }
    fn with_contex_set<F>(&self, ctx: Context, f: F)
    where
        F: Fn(RedisContextSet) -> (),
    {
        let redis_context =
            RedisContextSet::new(ctx, &self.redis_context);
        f(redis_context);
        debug!("with_contex_set | exit");
    }
}

impl Loop {
    fn new_from_arc(
        db: Arc<Mutex<sql::RawConnection>>,
        redis_context: Arc<Mutex<RefCell<Option<Context>>>>,
    ) -> Loop {
        let replication_book = ReplicationBook::new(&db);
        let redis_context = Arc::new(Mutex::new(redis_context));
        Loop {
            db,
            replication_book,
            redis_context,
        }
    }
}

pub trait RedisReply {
    fn reply(&self, ctx: &rm::Context) -> i32;
}

impl RedisReply for sql::Entity {
    fn reply(&self, ctx: &rm::Context) -> i32 {
        match *self {
            sql::Entity::Integer { int } => {
                rm::ReplyWithLongLong(ctx, int)
            }
            sql::Entity::Float { float } => {
                rm::ReplyWithDouble(ctx, float)
            }
            sql::Entity::Text { ref text } => {
                rm::ReplyWithStringBuffer(ctx, text.as_bytes())
            }
            sql::Entity::Blob { ref blob } => {
                rm::ReplyWithStringBuffer(ctx, blob.as_bytes())
            }
            sql::Entity::Null => rm::ReplyWithNull(ctx),
            sql::Entity::OK { .. } => QueryResult::OK {}.reply(ctx),
            sql::Entity::DONE { modified_rows, .. } => {
                QueryResult::DONE { modified_rows }.reply(ctx)
            }
        }
    }
}

fn reply_with_simple_string(
    ctx: *mut rm::ffi::RedisModuleCtx,
    s: &str,
) -> i32 {
    unsafe {
        rm::ffi::RedisModule_ReplyWithSimpleString.unwrap()(
            ctx,
            s.as_ptr() as *const c_char,
        )
    }
}

fn reply_with_ok(ctx: *mut rm::ffi::RedisModuleCtx) -> i32 {
    reply_with_simple_string(ctx, "OK\0")
}

fn reply_with_done(
    ctx: *mut rm::ffi::RedisModuleCtx,
    modified_rows: i32,
) -> i32 {
    unsafe {
        rm::ffi::RedisModule_ReplyWithArray.unwrap()(ctx, 2);
    }
    reply_with_simple_string(ctx, "DONE\0");
    unsafe {
        rm::ffi::RedisModule_ReplyWithLongLong.unwrap()(
            ctx,
            i64::from(modified_rows),
        );
    }
    rm::ffi::REDISMODULE_OK
}

fn reply_with_array(ctx: &rm::Context, array: Vec<sql::Row>) -> i32 {
    let len = array.len() as c_long;
    unsafe {
        rm::ffi::RedisModule_ReplyWithArray.unwrap()(
            ctx.as_ptr(),
            len,
        );
    }
    for row in array {
        unsafe {
            rm::ffi::RedisModule_ReplyWithArray.unwrap()(
                ctx.as_ptr(),
                row.len() as c_long,
            );
        }
        for entity in row {
            entity.reply(&ctx);
        }
    }
    rm::ffi::REDISMODULE_OK
}

impl RedisReply for sql::SQLite3Error {
    fn reply(&self, ctx: &Context) -> i32 {
        let error = format!("{}", self);
        reply_with_error(ctx.as_ptr(), error)
    }
}

impl RedisReply for RediSQLError {
    fn reply(&self, ctx: &Context) -> i32 {
        let error = format!("{}", self);
        reply_with_error(ctx.as_ptr(), error)
    }
}

fn reply_with_error(
    ctx: *mut rm::ffi::RedisModuleCtx,
    s: String,
) -> i32 {
    let s = CString::new(s).unwrap();
    unsafe {
        rm::ffi::RedisModule_ReplyWithError.unwrap()(ctx, s.as_ptr())
    }
}

pub fn create_argument(
    ctx: *mut rm::ffi::RedisModuleCtx,
    argv: *mut *mut rm::ffi::RedisModuleString,
    argc: i32,
) -> (rm::Context, Vec<&'static str>) {
    let context = rm::Context::new(ctx);
    let argvector = parse_args(argv, argc).unwrap();
    (context, argvector)
}

fn parse_args(
    argv: *mut *mut rm::ffi::RedisModuleString,
    argc: i32,
) -> Result<Vec<&'static str>, string::FromUtf8Error> {
    let mut args: Vec<&'static str> =
        Vec::with_capacity(argc as usize);
    for i in 0..argc {
        let redis_str = unsafe { *argv.offset(i as isize) };
        let arg = unsafe { string_ptr_len(redis_str) };
        args.push(arg);
    }
    Ok(args)
}

pub unsafe fn string_ptr_len(
    str: *mut rm::ffi::RedisModuleString,
) -> &'static str {
    let mut len = 0;
    let base =
        rm::ffi::RedisModule_StringPtrLen.unwrap()(str, &mut len)
            as *mut u8;
    let slice = slice::from_raw_parts(base, len);
    str::from_utf8_unchecked(slice)
}

#[repr(C)]
pub struct RedisKey {
    pub key: *mut rm::ffi::RedisModuleKey,
}

impl Drop for RedisKey {
    fn drop(&mut self) {
        unsafe {
            rm::ffi::RedisModule_CloseKey.unwrap()(self.key);
        }
    }
}

pub enum ReturnMethod {
    Reply,
    Stream { name: &'static str },
}

pub enum Command {
    Stop,
    Exec {
        query: &'static str,
        client: BlockedClient,
    },
    Query {
        query: &'static str,
        return_method: ReturnMethod,
        client: BlockedClient,
    },
    CompileStatement {
        identifier: &'static str,
        statement: &'static str,
        client: BlockedClient,
    },
    ExecStatement {
        identifier: &'static str,
        arguments: Vec<&'static str>,
        client: BlockedClient,
    },
    UpdateStatement {
        identifier: &'static str,
        statement: &'static str,
        client: BlockedClient,
    },
    DeleteStatement {
        identifier: &'static str,
        client: BlockedClient,
    },
    QueryStatement {
        identifier: &'static str,
        arguments: Vec<&'static str>,
        return_method: ReturnMethod,
        client: BlockedClient,
    },
    MakeCopy {
        source: DBKey,
        destination: DBKey,
        client: BlockedClient,
    },
}

pub enum QueryResult {
    OK {},
    DONE {
        modified_rows: i32,
    },
    Array {
        names: Vec<String>,
        array: Vec<sql::Row>,
    },
}

impl QueryResult {
    pub fn reply(self, ctx: &rm::Context) -> i32 {
        debug!("Start replying!");
        match self {
            QueryResult::OK { .. } => reply_with_ok(ctx.as_ptr()),
            QueryResult::DONE { modified_rows, .. } => {
                debug!("QueryResult::DONE");
                reply_with_done(ctx.as_ptr(), modified_rows)
            }
            QueryResult::Array { array, .. } => {
                debug!("QueryResult::Array");
                reply_with_array(ctx, array)
            }
        }
    }
}

pub fn do_execute(
    db: &Arc<Mutex<sql::RawConnection>>,
    query: &str,
) -> Result<QueryResult, err::RediSQLError> {
    let stmt = MultiStatement::new(db.clone(), query)?;
    debug!("do_execute | created statement");
    let cursor = stmt.execute()?;
    debug!("do_execute | statement executed");
    Ok(QueryResult::from(cursor))
}

pub fn do_query(
    db: &Arc<Mutex<sql::RawConnection>>,
    query: &str,
) -> Result<QueryResult, err::RediSQLError> {
    let stmt = MultiStatement::new(db.clone(), query)?;
    if stmt.is_read_only() {
        let cursor = stmt.execute()?;
        Ok(QueryResult::from(cursor))
    } else {
        let debug = String::from("Not read only statement");
        let description = String::from("Statement is not read only but it may modify the database, use `EXEC_STATEMENT` instead.",);
        Err(RediSQLError::new(debug, description))
    }
}

/// implements the copy of the source database into the destination one
/// it also leak the two DBKeys
pub fn do_copy(
    source: DBKey,
    destination: DBKey,
) -> Result<QueryResult, err::RediSQLError> {
    let source_connection = source.loop_data.get_db();
    let destination_connection = destination.loop_data.get_db();

    let backup_result = {
        let source_db = source_connection.lock().unwrap();
        let destination_db = destination_connection.lock().unwrap();
        match make_backup(&source_db, &destination_db) {
            Err(e) => Err(RediSQLError::from(e)),
            Ok(_) => Ok(QueryResult::OK {}),
        }
    };

    restore_previous_statements(&destination.loop_data);

    std::mem::forget(source);
    std::mem::forget(destination);

    backup_result
}

fn bind_statement<'a>(
    stmt: &'a MultiStatement,
    arguments: &[&str],
) -> Result<&'a MultiStatement, sql::SQLite3Error> {
    match stmt.bind_texts(arguments) {
        Err(e) => Err(e),
        Ok(_) => Ok(stmt),
    }
}

pub struct RedisError {
    pub msg: String,
}

impl fmt::Display for RedisError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ERR - {}", self.msg)
    }
}

impl fmt::Debug for RedisError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl error::Error for RedisError {
    fn description(&self) -> &str {
        self.msg.as_str()
    }
}

/// restore_previous_statements wread the statements written in the database and add them to the
/// loopdata datastructure.
/// At the moment this function returns `()` no matter if there are errors or not.
/// Errors are pretty unlikely, especially if nobody touched manually the metadata database, but
/// still they may happens.
/// I am not sure if it is a good idea or if I should upgrade the code to return an error, and
/// maybe just ignore the error to keep the whole flow as it is now.
fn restore_previous_statements<'a, L: 'a + LoopData>(loopdata: &L) {
    let saved_statements = get_statement_metadata(loopdata.get_db());
    match saved_statements {
        Ok(QueryResult::Array { array, .. }) => {
            for row in array {
                let identifier = match row[1] {
                    sql::Entity::Text { ref text } => text,
                    _ => continue,
                };
                let statement = match row[2] {
                    sql::Entity::Text { ref text } => text,
                    _ => continue,
                };
                if let Err(e) = compile_and_insert_statement(
                    identifier, statement, loopdata,
                ) {
                    println!("Error: {}", e)
                }
            }
        }
        Err(e) => println!("Error: {}", e),
        _ => (),
    }
}

fn return_value(
    client: &BlockedClient,
    result: Result<QueryResult, err::RediSQLError>,
) {
    unsafe {
        rm::ffi::RedisModule_UnblockClient.unwrap()(
            client.client,
            Box::into_raw(Box::new(result))
                as *mut std::os::raw::c_void,
        );
    }
}

fn return_value_v2(
    client: &BlockedClient,
    return_method: &ReturnMethod,
    result: Result<QueryResult, err::RediSQLError>,
) {
    match return_method {
        ReturnMethod::Reply => return_value(client, result),
        ReturnMethod::Stream { name: stream_name } => match result {
            Err(_) => return_value(client, result),
            Ok(QueryResult::OK {}) => return_value(client, result),
            Ok(QueryResult::DONE { .. }) => {
                return_value(client, result)
            }
            Ok(QueryResult::Array {
                array: rows,
                names: columns_names,
            }) => {
                let context = Context::thread_safe(client);
                let result = stream_query_result_array(
                    &context,
                    stream_name,
                    columns_names.as_slice(),
                    rows,
                );
                return_value(client, result)
            }
        },
    }
}

pub fn stream_query_result_array(
    context: &Context,
    stream_name: &str,
    columns_names: &[String],
    array: Vec<sql::Row>,
) -> Result<QueryResult, err::RediSQLError> {
    let row_last_index = array.len() - 1;

    let mut result = Vec::with_capacity(4);
    result.push(sql::Entity::Text {
        text: stream_name.to_string(),
    });

    let mut lock = context.lock();
    for (i, row) in array.iter().enumerate() {
        if i % 256 == 255 {
            // avoid that a big results lock the context for too long, should help in
            // keeping the latency low.
            context.release(lock);
            lock = context.lock();
        }
        let mut xadd = rm::XADDCommand::new(&context, stream_name);
        for (j, entity) in row.iter().enumerate() {
            match entity {
                sql::Entity::OK {} | sql::Entity::DONE { .. } => {
                    // do nothing
                }
                sql::Entity::Null => {
                    xadd.add_element(
                        &format!("null:{}", &columns_names[j]),
                        "(null)",
                    );
                }
                sql::Entity::Integer { int } => {
                    xadd.add_element(
                        &format!("int:{}", &columns_names[j]),
                        &int.to_string(),
                    );
                }
                sql::Entity::Float { float } => {
                    xadd.add_element(
                        &format!("real:{}", &columns_names[j]),
                        &float.to_string(),
                    );
                }
                sql::Entity::Text { text } => {
                    xadd.add_element(
                        &format!("text:{}", &columns_names[j]),
                        text,
                    );
                }
                sql::Entity::Blob { blob } => {
                    xadd.add_element(
                        &format!("blob:{}", &columns_names[j]),
                        blob,
                    );
                }
            }
        }
        debug!("XADD {:?}", xadd);
        let xadd_result = xadd.execute(&lock);
        match xadd_result {
            rm::CallReply::RString { .. } => match i {
                0 => {
                    result.push(sql::Entity::Text {
                        text: xadd_result.access_string().unwrap(),
                    });
                }
                n if n == row_last_index => {
                    result.push(sql::Entity::Text {
                        text: xadd_result.access_string().unwrap(),
                    });
                }
                _ => {
                    // do nothing
                }
            },
            rm::CallReply::RError { .. } => {
                context.release(lock);
                return Err(RediSQLError::new(
                    xadd_result.access_error().unwrap().to_string(),
                    format!("Error in XADD to {}", stream_name),
                ));
                // return an error and unlock
            }
            _ => {
                debug!("XADD result: {:?}", xadd_result);
                panic!();
            }
        }
    }
    context.release(lock);

    if result.len() == 2 {
        let start_and_end = result[1].clone();
        result.push(start_and_end);
    }
    result.push(sql::Entity::Integer {
        int: array.len() as i64,
    });

    Ok(QueryResult::Array {
        names: vec![
            String::from("stream"),
            String::from("first_id"),
            String::from("last_id"),
            String::from("size"),
        ],
        array: vec![result],
    })
}

pub fn listen_and_execute<'a, L: 'a + LoopData>(
    loopdata: &mut L,
    rx: &Receiver<Command>,
) {
    debug!("Start thread execution");
    restore_previous_statements(loopdata);
    loop {
        debug!("Loop iteration");
        match rx.recv() {
            Ok(Command::Exec { query, client }) => {
                debug!("Exec | Query = {:?}", query);
                loopdata.with_contex_set(
                    Context::thread_safe(&client),
                    |_| {
                        let result =
                            do_execute(&loopdata.get_db(), query);
                        return_value(&client, result);
                    },
                );
                debug!("Exec | DONE, returning result");
            }
            Ok(Command::Query {
                query,
                return_method,
                client,
            }) => {
                debug!("Query | Query = {:?}", query);
                loopdata.with_contex_set(
                    Context::thread_safe(&client),
                    |_| {
                        let result =
                            do_query(&loopdata.get_db(), query);
                        return_value_v2(
                            &client,
                            &return_method,
                            result,
                        );
                    },
                );
            }
            Ok(Command::UpdateStatement {
                identifier,
                statement,
                client,
            }) => {
                debug!(
                    "UpdateStatement | Identifier = {:?} Statement = {:?}",
                    identifier, statement
                );
                let result = loopdata
                    .get_replication_book()
                    .update_statement(identifier, statement);
                return_value(&client, result)
            }
            Ok(Command::DeleteStatement { identifier, client }) => {
                debug!(
                    "DeleteStatement | Identifier = {:?}",
                    identifier
                );
                let result = loopdata
                    .get_replication_book()
                    .delete_statement(identifier);

                return_value(&client, result);
            }
            Ok(Command::CompileStatement {
                identifier,
                statement,
                client,
            }) => {
                debug!(
                    "CompileStatement | Identifier = {:?} Statement = {:?}",
                    identifier, statement
                );
                let result = loopdata
                    .get_replication_book()
                    .insert_new_statement(identifier, statement);
                return_value(&client, result);
            }

            Ok(Command::ExecStatement {
                identifier,
                arguments,
                client,
            }) => {
                debug!(
                    "ExecStatement | Identifier = {:?} Arguments = {:?}",
                    identifier, arguments
                );
                loopdata.with_contex_set(
                    Context::thread_safe(&client),
                    |_| {
                        let result = loopdata
                            .get_replication_book()
                            .exec_statement(identifier, &arguments);
                        return_value(&client, result);
                    },
                );
            }
            Ok(Command::QueryStatement {
                identifier,
                arguments,
                return_method,
                client,
            }) => {
                loopdata.with_contex_set(
                    Context::thread_safe(&client),
                    |_| {
                        let result = loopdata
                            .get_replication_book()
                            .query_statement(
                                identifier,
                                arguments.as_slice(),
                            );
                        return_value_v2(
                            &client,
                            &return_method,
                            result,
                        );
                    },
                );
            }
            Ok(Command::MakeCopy {
                source,
                destination,
                client,
            }) => {
                // The DBKeys MUST be leaked, they are leaked by do_copy
                let result = do_copy(source, destination);
                return_value(&client, result);
            }
            Ok(Command::Stop) => {
                debug!("Stop, exiting from work loop");
                return;
            }
            Err(RecvError) => {
                debug!("RecvError, exiting from work loop");
                return;
            }
        }
    }
}

fn compile_and_insert_statement<'a, L: 'a + LoopData>(
    identifier: &str,
    statement: &str,
    loop_data: &L,
) -> Result<QueryResult, err::RediSQLError> {
    let stmt_cache = &loop_data.get_replication_book().data;
    let mut statements_cache = stmt_cache.write().unwrap();
    /* On the map (statements_cache) we need to own the value of
     * identifiers, we are ok with this
     * since it happens rarely.
     */
    match statements_cache.entry(identifier.to_owned()) {
        Entry::Vacant(v) => {
            let db = loop_data.get_db();
            match create_statement(db, identifier, statement) {
                Ok(stmt) => {
                    let read_only = stmt.is_read_only();
                    v.insert((stmt, read_only));
                    Ok(QueryResult::OK {})
                }
                Err(e) => Err(e),
            }
        }
        Entry::Occupied(_) => {
            let err = RedisError {
                msg: String::from(
                    "Statement already exists, \
                     impossible to overwrite it with \
                     this command, try with \
                     UPDATE_STATEMENT",
                ),
            };
            Err(err::RediSQLError::from(err))
        }
    }
}

pub struct DBKey {
    pub tx: Sender<Command>,
    pub in_memory: bool,
    pub loop_data: Loop,
}

impl DBKey {
    pub fn new_from_arc(
        tx: Sender<Command>,
        db: Arc<Mutex<sql::RawConnection>>,
        in_memory: bool,
        redis_context: Arc<Mutex<RefCell<Option<Context>>>>,
    ) -> DBKey {
        let loop_data = Loop::new_from_arc(db, redis_context);
        DBKey {
            tx,
            in_memory,
            loop_data,
        }
    }
}

impl Drop for DBKey {
    fn drop(&mut self) {
        debug!("### Dropping DBKey ###")
    }
}

pub fn create_metadata_table(
    db: Arc<Mutex<sql::RawConnection>>,
) -> Result<(), sql::SQLite3Error> {
    let statement = "CREATE TABLE IF NOT EXISTS RediSQLMetadata(data_type TEXT, key TEXT, value TEXT);";

    let stmt = MultiStatement::new(db, statement)?;
    stmt.execute()?;
    Ok(())
}

pub fn insert_metadata(
    db: Arc<Mutex<sql::RawConnection>>,
    data_type: &str,
    key: &str,
    value: &str,
) -> Result<(), sql::SQLite3Error> {
    let statement = "INSERT INTO RediSQLMetadata VALUES(?1, ?2, ?3);";

    let stmt = MultiStatement::new(db, statement)?;
    stmt.bind_index(1, data_type)?;
    stmt.bind_index(2, key)?;
    stmt.bind_index(3, value)?;
    stmt.execute()?;
    Ok(())
}

pub fn enable_foreign_key(
    db: Arc<Mutex<sql::RawConnection>>,
) -> Result<(), sql::SQLite3Error> {
    let enable_foreign_key = "PRAGMA foreign_keys = ON;";
    match MultiStatement::new(db, enable_foreign_key) {
        Err(e) => Err(e),
        Ok(stmt) => match stmt.execute() {
            Err(e) => Err(e),
            Ok(_) => Ok(()),
        },
    }
}

fn update_statement_metadata(
    db: Arc<Mutex<sql::RawConnection>>,
    key: &str,
    value: &str,
) -> Result<(), sql::SQLite3Error> {
    let statement =
        "UPDATE RediSQLMetadata SET value = ?1 WHERE data_type = 'statement' AND key = ?2";

    let stmt = MultiStatement::new(db, statement)?;
    stmt.bind_index(1, value)?;
    stmt.bind_index(2, key)?;
    stmt.execute()?;
    Ok(())
}

fn remove_statement_metadata(
    db: Arc<Mutex<sql::RawConnection>>,
    key: &str,
) -> Result<(), sql::SQLite3Error> {
    let statement = "DELETE FROM RediSQLMetadata WHERE data_type = 'statement' AND key = ?1";

    let stmt = MultiStatement::new(db, statement)?;
    stmt.bind_index(1, key)?;
    stmt.execute()?;
    Ok(())
}

fn get_statement_metadata(
    db: Arc<Mutex<sql::RawConnection>>,
) -> Result<QueryResult, sql::SQLite3Error> {
    let statement = "SELECT * FROM RediSQLMetadata WHERE data_type = 'statement';";

    let stmt = MultiStatement::new(db, statement)?;
    let cursor = stmt.execute()?;
    Ok(QueryResult::from(cursor))
}

pub fn make_backup(
    conn1: &sql::RawConnection,
    conn2: &sql::RawConnection,
) -> Result<i32, sql::SQLite3Error> {
    match sql::create_backup(conn1, conn2) {
        Err(e) => Err(e),
        Ok(bk) => {
            let mut result = unsafe { sql::BackupStep(&bk, 1) };
            while sql::backup_should_step_again(result) {
                result = unsafe { sql::BackupStep(&bk, 1) };
            }
            unsafe { sql::BackupFinish(&bk) };
            Ok(result)
        }
    }
}

pub fn create_backup(
    conn: &sql::RawConnection,
    path: &str,
) -> Result<i32, sql::SQLite3Error> {
    match sql::RawConnection::open_connection(path) {
        Err(e) => Err(e),
        Ok(new_db) => make_backup(conn, &new_db),
    }
}

pub unsafe fn write_file_to_rdb(
    f: File,
    rdb: *mut rm::ffi::RedisModuleIO,
) -> Result<(), std::io::Error> {
    let block_size = 1024 * 4 as i64;
    let lenght = f.metadata().unwrap().len();
    let blocks = lenght / block_size as u64;

    rm::SaveSigned(rdb, blocks as i64);

    let to_write: Vec<u8> = vec![0; block_size as usize];
    let mut buffer = BufReader::with_capacity(block_size as usize, f);
    loop {
        let mut tw = to_write.clone();
        match buffer.read(tw.as_mut_slice()) {
            Ok(0) => return Ok(()),
            Ok(_n) => rm::SaveStringBuffer(rdb, tw.as_slice()),
            Err(e) => return Err(e),
        }
    }
}

struct SafeRedisModuleString {
    ptr: *mut std::os::raw::c_char,
}

impl Drop for SafeRedisModuleString {
    fn drop(&mut self) {
        unsafe {
            rm::ffi::RedisModule_Free.unwrap()(
                self.ptr as *mut std::os::raw::c_void,
            )
        }
    }
}

pub unsafe fn write_rdb_to_file(
    f: &mut File,
    rdb: *mut rm::ffi::RedisModuleIO,
) -> Result<(), std::io::Error> {
    let blocks = rm::LoadSigned(rdb);
    for _ in 0..blocks {
        let mut dimension: usize = 0;
        let c_str_ptr = SafeRedisModuleString {
            ptr: rm::ffi::RedisModule_LoadStringBuffer.unwrap()(
                rdb,
                &mut dimension,
            ),
        };
        if dimension == 0 {
            break;
        }
        let slice = slice::from_raw_parts(
            c_str_ptr.ptr as *mut u8,
            dimension,
        );
        let y = f.write_all(slice);
        if let Err(e) = y {
            return Err(e);
        }
    }
    Ok(())
}

pub fn with_leaky_db<F>(
    ctx: *mut rm::ffi::RedisModuleCtx,
    name: &str,
    f: F,
) -> i32
where
    F: Fn(&Result<DBKey, i32>) -> i32,
{
    let db = match get_dbkeyptr_from_name(ctx, name) {
        Err(err) => Err(err),
        Ok(ptr) => Ok(unsafe { ptr.read() }),
    };
    let result = f(&db);
    debug!("with_leaky_db | go result {}", result);
    if db.is_ok() {
        debug!("with_leaky_db | forgetting db");
        // Box::into_raw(db);
        std::mem::forget(db);
    }
    result
}

pub fn get_dbkeyptr_from_name(
    ctx: *mut rm::ffi::RedisModuleCtx,
    name: &str,
) -> Result<*mut DBKey, i32> {
    let context = Context::new(ctx);
    let key_name = rm::RMString::new(&context, name);
    let key =
        OpenKey(&context, &key_name, rm::ffi::REDISMODULE_WRITE);
    let safe_key = RedisKey { key };
    let key_type = unsafe {
        rm::ffi::RedisModule_KeyType.unwrap()(safe_key.key)
    };
    if unsafe {
        rm::ffi::DBType
            == rm::ffi::RedisModule_ModuleTypeGetType.unwrap()(
                safe_key.key,
            )
    } {
        let db_ptr = unsafe {
            rm::ffi::RedisModule_ModuleTypeGetValue.unwrap()(
                safe_key.key,
            ) as *mut DBKey
        };
        Ok(db_ptr)
    } else {
        Err(key_type)
    }
}

pub fn get_dbkey_from_name(
    ctx: *mut rm::ffi::RedisModuleCtx,
    name: &str,
) -> Result<DBKey, i32> {
    let dbptr = get_dbkeyptr_from_name(ctx, name)?;
    let dbkey = unsafe { dbptr.read() };
    Ok(dbkey)
}

pub fn with_ch_and_loopdata<F>(
    ctx: *mut rm::ffi::RedisModuleCtx,
    name: &str,
    f: F,
) -> i32
where
    F: Fn(Result<(&Sender<Command>, &mut Loop), i32>) -> i32,
{
    let r = get_ch_and_loopdata_from_name(ctx, name);
    f(r)
}

pub fn get_ch_and_loopdata_from_name(
    ctx: *mut rm::ffi::RedisModuleCtx,
    name: &str,
) -> Result<(&Sender<Command>, &mut Loop), i32> {
    // here we are intentionally leaking the DBKey, so that it does
    // not get destroyed
    let db: *mut DBKey = get_dbkeyptr_from_name(ctx, name)?;
    let channel = unsafe { &(*db).tx };
    let loopdata = unsafe { &mut (*db).loop_data };
    Ok((channel, loopdata))
}

pub fn get_db_channel_from_name(
    ctx: *mut rm::ffi::RedisModuleCtx,
    name: &str,
) -> Result<Sender<Command>, i32> {
    let db: *mut DBKey = get_dbkeyptr_from_name(ctx, name)?;
    let db = unsafe { Box::from_raw(db) };
    let channel = db.tx.clone();
    std::mem::forget(db);
    Ok(channel)
}

pub fn reply_with_error_from_key_type(
    ctx: *mut rm::ffi::RedisModuleCtx,
    key_type: i32,
) -> i32 {
    let context = &Context::new(ctx);
    match key_type {
        rm::ffi::REDISMODULE_KEYTYPE_EMPTY => {
            ReplyWithError(context, "ERR - Error the key is empty\0")
        }
        _ => {
            let error = CStr::from_bytes_with_nul(
                rm::ffi::REDISMODULE_ERRORMSG_WRONGTYPE,
            )
            .unwrap();
            ReplyWithError(context, error.to_str().unwrap())
        }
    }
}

fn create_statement(
    db: Arc<Mutex<sql::RawConnection>>,
    identifier: &str,
    statement: &str,
) -> Result<MultiStatement, err::RediSQLError> {
    let stmt = MultiStatement::new(Arc::clone(&db), statement)?;
    insert_metadata(db, "statement", identifier, statement)?;
    Ok(stmt)
}

fn update_statement(
    db: &Arc<Mutex<sql::RawConnection>>,
    identifier: &str,
    statement: &str,
) -> Result<MultiStatement, err::RediSQLError> {
    let stmt = MultiStatement::new(Arc::clone(db), statement)?;
    update_statement_metadata(Arc::clone(db), identifier, statement)?;
    Ok(stmt)
}

fn remove_statement(
    db: &Arc<Mutex<sql::RawConnection>>,
    identifier: &str,
) -> Result<(), err::RediSQLError> {
    remove_statement_metadata(Arc::clone(db), identifier)
        .or_else(|e| Err(err::RediSQLError::from(e)))
}

#[allow(non_snake_case)]
pub unsafe fn Replicate(
    _ctx: &rm::Context,
    _command: &str,
    _argv: *mut *mut rm::ffi::RedisModuleString,
    _argc: std::os::raw::c_int,
) {
}

pub fn register_function(
    context: &rm::Context,
    name: &str,
    flags: &str,
    f: extern "C" fn(
        *mut rm::ffi::RedisModuleCtx,
        *mut *mut rm::ffi::RedisModuleString,
        ::std::os::raw::c_int,
    ) -> i32,
) -> Result<(), i32> {
    let create_db: rm::ffi::RedisModuleCmdFunc = Some(f);

    if { rm::CreateCommand(context, name, create_db, flags) }
        == rm::ffi::REDISMODULE_ERR
    {
        return Err(rm::ffi::REDISMODULE_ERR);
    }
    Ok(())
}

pub fn register_function_with_keys(
    context: &rm::Context,
    name: &str,
    flags: &str,
    first_key: i32,
    last_key: i32,
    key_step: i32,
    f: extern "C" fn(
        *mut rm::ffi::RedisModuleCtx,
        *mut *mut rm::ffi::RedisModuleString,
        ::std::os::raw::c_int,
    ) -> i32,
) -> Result<(), i32> {
    let create_db: rm::ffi::RedisModuleCmdFunc = Some(f);

    if {
        rm::CreateCommandWithKeys(
            context, name, create_db, flags, first_key, last_key,
            key_step,
        )
    } == rm::ffi::REDISMODULE_ERR
    {
        return Err(rm::ffi::REDISMODULE_ERR);
    }
    Ok(())
}
pub fn register_write_function(
    ctx: &rm::Context,
    name: &str,
    f: extern "C" fn(
        *mut rm::ffi::RedisModuleCtx,
        *mut *mut rm::ffi::RedisModuleString,
        ::std::os::raw::c_int,
    ) -> i32,
) -> Result<(), i32> {
    register_function(ctx, name, "write", f)
}
