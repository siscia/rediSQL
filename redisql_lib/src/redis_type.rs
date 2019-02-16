use std::ffi::CString;
use std::mem;
use std::os::raw::{c_char, c_int};

#[allow(dead_code)]
#[allow(non_snake_case)]
#[allow(non_camel_case_types)]
#[allow(non_upper_case_globals)]
#[allow(improper_ctypes)]
#[allow(unknown_lints)]
pub mod ffi {
    #![allow(clippy::all)]
    include!(concat!(env!("OUT_DIR"), "/bindings_redis.rs"));
}

#[derive(Debug)]
pub struct Context {
    ctx: *mut ffi::RedisModuleCtx,
    thread_safe: bool,
}

pub struct ContextLock {}

impl Context {
    pub fn new(ctx: *mut ffi::RedisModuleCtx) -> Context {
        Context {
            ctx,
            thread_safe: false,
        }
    }
    pub fn as_ptr(&self) -> *mut ffi::RedisModuleCtx {
        self.ctx
    }
    pub fn thread_safe(blocked_client: &BlockedClient) -> Context {
        let ctx = unsafe {
            ffi::RedisModule_GetThreadSafeContext.unwrap()(
                blocked_client.as_ptr(),
            )
        };
        Context {
            ctx,
            thread_safe: true,
        }
    }
    pub fn lock(&self) -> ContextLock {
        if self.thread_safe {
            unsafe {
                ffi::RedisModule_ThreadSafeContextLock.unwrap()(
                    self.as_ptr(),
                );
            }
        }
        ContextLock {}
    }
    pub fn release(&self, _lock: ContextLock) {
        if self.thread_safe {
            unsafe {
                ffi::RedisModule_ThreadSafeContextUnlock.unwrap()(
                    self.as_ptr(),
                );
            }
        }
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        if self.thread_safe {
            debug!("Free thread safe context");
            unsafe {
                ffi::RedisModule_FreeThreadSafeContext.unwrap()(
                    self.as_ptr(),
                );
            }
        }
    }
}

unsafe impl Send for Context {}
unsafe impl Sync for Context {}

pub struct BlockedClient {
    pub client: *mut ffi::RedisModuleBlockedClient,
}

unsafe impl Send for BlockedClient {}

impl BlockedClient {
    pub fn as_ptr(&self) -> *mut ffi::RedisModuleBlockedClient {
        self.client
    }
}

#[derive(Debug)]
pub struct RMString<'a> {
    ptr: *mut ffi::RedisModuleString,
    ctx: &'a Context,
}

impl<'a> RMString<'a> {
    pub fn new(ctx: &'a Context, s: &str) -> RMString<'a> {
        let ptr = unsafe {
            ffi::RedisModule_CreateString.unwrap()(
                ctx.as_ptr(),
                s.as_ptr() as *const c_char,
                s.len(),
            )
        };
        RMString { ptr, ctx }
    }
    pub fn as_ptr(&self) -> *mut ffi::RedisModuleString {
        self.ptr
    }
}

impl<'a> Drop for RMString<'a> {
    fn drop(&mut self) {
        unsafe {
            ffi::RedisModule_FreeString.unwrap()(
                self.ctx.as_ptr(),
                self.as_ptr(),
            );
        }
    }
}

//this structure contains an array of pointer to RMString, however, on drop, it drops only the
//array and leak the RMString.
//It is to be used as argument to RM_Call when the format includes the `v`, an array of RMString.
#[derive(Debug)]
pub struct LeakyArrayOfRMString<'a> {
    array: Vec<*mut ffi::RedisModuleString>,
    ctx: &'a Context,
}

impl<'a> LeakyArrayOfRMString<'a> {
    pub fn new(ctx: &'a Context) -> LeakyArrayOfRMString {
        LeakyArrayOfRMString {
            array: Vec::with_capacity(24),
            ctx,
        }
    }
    pub fn push(&mut self, s: &str) {
        let ptr = unsafe {
            ffi::RedisModule_CreateString.unwrap()(
                self.ctx.as_ptr(),
                s.as_ptr() as *const c_char,
                s.len(),
            )
        };
        self.array.push(ptr);
    }
    pub fn as_ptr(&mut self) -> *mut *mut ffi::RedisModuleString {
        self.array.as_mut_ptr()
    }
    pub fn len(&self) -> usize {
        self.array.len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug)]
pub struct XADDCommand<'a> {
    ctx: &'a Context,
    array: LeakyArrayOfRMString<'a>,
}

impl<'a> XADDCommand<'a> {
    pub fn new(ctx: &'a Context, key: &str) -> XADDCommand<'a> {
        XADDCommand::new_with_id(ctx, key, "*")
    }
    pub fn new_with_id(
        ctx: &'a Context,
        key: &str,
        id: &str,
    ) -> XADDCommand<'a> {
        let mut array = LeakyArrayOfRMString::new(ctx);
        array.push(key);
        array.push(id);
        XADDCommand { ctx, array }
    }
    pub fn add_element(&mut self, field: &str, value: &str) {
        self.array.push(field);
        self.array.push(value);
    }
    // this command must be called inside a transaction with the context locked, hence we use as
    // parameter also a ContextLock
    pub fn execute(&mut self, _lock: &ContextLock) -> CallReply {
        let xadd = CString::new("XADD").unwrap();
        let call_specifiers = CString::new("!v").unwrap();
        let reply = unsafe {
            ffi::RedisModule_Call.unwrap()(
                self.ctx.as_ptr(),
                xadd.as_ptr(),
                call_specifiers.as_ptr(),
                self.array.as_ptr(),
                self.array.len(),
            )
        };
        unsafe { CallReply::new(reply) }
    }
}

#[allow(non_snake_case)]
pub fn CreateCommand(
    ctx: &Context,
    name: &str,
    f: ffi::RedisModuleCmdFunc,
    flags: &str,
) -> i32 {
    CreateCommandWithKeys(ctx, name, f, flags, 1, 1, 1)
}

#[allow(non_snake_case)]
pub fn CreateCommandWithKeys(
    ctx: &Context,
    name: &str,
    f: ffi::RedisModuleCmdFunc,
    flags: &str,
    first_key: i32,
    last_key: i32,
    key_step: i32,
) -> i32 {
    let command_c_name = CString::new(name).unwrap();
    let command_ptr_name = command_c_name.as_ptr();

    let flag_c_name = CString::new(flags).unwrap();
    let flag_ptr_name = flag_c_name.as_ptr();

    unsafe {
        ffi::RedisModule_CreateCommand.unwrap()(
            ctx.as_ptr(),
            command_ptr_name,
            f,
            flag_ptr_name,
            first_key,
            last_key,
            key_step,
        )
    }
}

#[allow(non_snake_case)]
pub fn ReplicateVerbatim(ctx: &Context) -> i32 {
    unsafe {
        ffi::RedisModule_ReplicateVerbatim.unwrap()(ctx.as_ptr())
    }
}

#[allow(non_snake_case)]
pub unsafe fn Replicate(
    ctx: &Context,
    command: &str,
    argv: *mut *mut ffi::RedisModuleString,
    argc: c_int,
) -> i32 {
    let command = CString::new(command).unwrap();
    let v = CString::new("v").unwrap();
    ffi::RedisModule_Replicate.unwrap()(
        ctx.as_ptr(),
        command.as_ptr(),
        v.as_ptr(),
        argv.offset(1),
        argc - 1,
    )
}

#[allow(non_snake_case)]
pub fn ReplyWithError(ctx: &Context, error: &str) -> i32 {
    unsafe {
        ffi::RedisModule_ReplyWithError.unwrap()(
            ctx.as_ptr(),
            error.as_ptr() as *const c_char,
        )
    }
}

#[allow(non_snake_case)]
pub fn OpenKey(
    ctx: &Context,
    name: &RMString,
    mode: i32,
) -> *mut ffi::RedisModuleKey {
    unsafe {
        ffi::Export_RedisModule_OpenKey(ctx.as_ptr(), name.ptr, mode)
    }
}

/*
#[allow(non_snake_case)]
pub fn LoadStringBuffer(rdb: *mut rm::ffi::RedisModuleIO,
                        dimension: &mut usize)
                        ->  {
    unsafe { ffi::RedisModule_LoadStringBuffer(rdb, dimension) }
}
*/

#[allow(non_snake_case)]
pub unsafe fn LoadSigned(rdb: *mut ffi::RedisModuleIO) -> i64 {
    ffi::RedisModule_LoadSigned.unwrap()(rdb) as i64
}

#[allow(non_snake_case)]
pub unsafe fn SaveSigned(rdb: *mut ffi::RedisModuleIO, to_save: i64) {
    ffi::RedisModule_SaveSigned.unwrap()(rdb, to_save)
}

#[allow(non_snake_case)]
pub unsafe fn SaveStringBuffer(
    rdb: *mut ffi::RedisModuleIO,
    buffer: &[u8],
) {
    let ptr = buffer.as_ptr() as *const c_char;
    let len = buffer.len();
    ffi::RedisModule_SaveStringBuffer.unwrap()(rdb, ptr, len)
}

#[allow(non_snake_case)]
pub fn ReplyWithNull(ctx: &Context) -> i32 {
    unsafe { ffi::RedisModule_ReplyWithNull.unwrap()(ctx.as_ptr()) }
}

#[allow(non_snake_case)]
pub fn ReplyWithLongLong(ctx: &Context, ll: i64) -> i32 {
    unsafe {
        ffi::RedisModule_ReplyWithLongLong.unwrap()(ctx.as_ptr(), ll)
    }
}

#[allow(non_snake_case)]
pub fn ReplyWithDouble(ctx: &Context, dd: f64) -> i32 {
    unsafe {
        ffi::RedisModule_ReplyWithDouble.unwrap()(ctx.as_ptr(), dd)
    }
}

#[allow(non_snake_case)]
pub fn ReplyWithStringBuffer(ctx: &Context, buffer: &[u8]) -> i32 {
    let ptr = buffer.as_ptr() as *const c_char;
    let len = buffer.len();
    unsafe {
        ffi::RedisModule_ReplyWithStringBuffer.unwrap()(
            ctx.as_ptr(),
            ptr,
            len,
        )
    }
}

pub struct AOF {
    aof: *mut ffi::RedisModuleIO,
}

impl AOF {
    pub fn new(aof: *mut ffi::RedisModuleIO) -> AOF {
        AOF { aof }
    }
    pub fn as_ptr(&self) -> *mut ffi::RedisModuleIO {
        self.aof
    }
}

#[allow(non_snake_case)]
pub unsafe fn EmitAOF(
    aof: &AOF,
    command: &str,
    specifier: &str,
    key: *mut ffi::RedisModuleString,
    data: &str,
) {
    ffi::RedisModule_EmitAOF.unwrap()(
        aof.as_ptr(),
        command.as_ptr() as *const c_char,
        specifier.as_ptr() as *const c_char,
        key,
        data.as_ptr(),
    )
}

#[derive(Debug)]
pub enum CallReply {
    RString { ptr: *mut ffi::RedisModuleCallReply },
    RError { ptr: *mut ffi::RedisModuleCallReply },
    RInteger { ptr: *mut ffi::RedisModuleCallReply },
    RArray { ptr: *mut ffi::RedisModuleCallReply },
    RNull { ptr: *mut ffi::RedisModuleCallReply },
}

impl CallReply {
    pub unsafe fn new(
        ptr: *mut ffi::RedisModuleCallReply,
    ) -> CallReply {
        match ffi::RedisModule_CallReplyType.unwrap()(ptr) {
            ffi::REDISMODULE_REPLY_STRING => {
                CallReply::RString { ptr }
            }
            ffi::REDISMODULE_REPLY_ERROR => CallReply::RError { ptr },
            ffi::REDISMODULE_REPLY_INTEGER => {
                CallReply::RInteger { ptr }
            }
            ffi::REDISMODULE_REPLY_ARRAY => CallReply::RArray { ptr },
            _ => CallReply::RNull { ptr },
        }
    }

    pub fn as_ptr(&self) -> *mut ffi::RedisModuleCallReply {
        match self {
            CallReply::RString { ptr }
            | CallReply::RError { ptr }
            | CallReply::RInteger { ptr }
            | CallReply::RArray { ptr }
            | CallReply::RNull { ptr } => *ptr,
        }
    }

    pub fn length(&self) -> Option<usize> {
        match self {
            CallReply::RString { ptr }
            | CallReply::RArray { ptr } => {
                let size = unsafe {
                    ffi::RedisModule_CallReplyLength.unwrap()(*ptr)
                };
                Some(size)
            }
            _ => None,
        }
    }
    pub fn access_array_subelement(
        &self,
        idx: usize,
    ) -> Option<CallReply> {
        match self {
            CallReply::RString { .. }
            | CallReply::RError { .. }
            | CallReply::RInteger { .. }
            | CallReply::RNull { .. } => None,

            CallReply::RArray { ptr } => {
                let sub_reply = unsafe {
                    ffi::RedisModule_CallReplyArrayElement.unwrap()(
                        *ptr,
                        idx as usize,
                    )
                };
                Some(unsafe { CallReply::new(sub_reply) })
            }
        }
    }
    pub fn access_integer(&self) -> Option<i64> {
        match self {
            CallReply::RString { .. }
            | CallReply::RError { .. }
            | CallReply::RArray { .. }
            | CallReply::RNull { .. } => None,

            CallReply::RInteger { ptr } => {
                let integer = unsafe {
                    ffi::RedisModule_CallReplyInteger.unwrap()(*ptr)
                };
                Some(integer)
            }
        }
    }
    pub fn access_string(&self) -> Option<String> {
        debug!("access_string");
        match self {
            CallReply::RInteger { .. }
            | CallReply::RError { .. }
            | CallReply::RArray { .. }
            | CallReply::RNull { .. } => None,

            CallReply::RString { ptr } => {
                let mut size = 0;
                unsafe {
                    let ptr = ffi::RedisModule_CallReplyStringPtr
                        .unwrap()(
                        *ptr, &mut size
                    );
                    let string = String::from_raw_parts(
                        ptr as *mut u8,
                        size,
                        size,
                    );
                    debug!("access_string about to drop");
                    let to_return = string.clone();
                    mem::forget(string);
                    Some(to_return)
                }
            }
        }
    }

    pub fn access_error(&self) -> Option<String> {
        match self {
            CallReply::RString { .. }
            | CallReply::RInteger { .. }
            | CallReply::RArray { .. }
            | CallReply::RNull { .. } => None,

            CallReply::RError { ptr } => {
                let mut size = 0;
                unsafe {
                    let ptr = ffi::RedisModule_CallReplyStringPtr
                        .unwrap()(
                        *ptr, &mut size
                    );
                    let string = String::from_raw_parts(
                        ptr as *mut u8,
                        size,
                        size,
                    );
                    debug!("access_string about to drop");
                    let to_return = string.clone();
                    mem::forget(string);
                    Some(to_return)
                }
            }
        }
    }
}

impl Drop for CallReply {
    fn drop(&mut self) {
        unsafe {
            ffi::RedisModule_FreeCallReply.unwrap()(self.as_ptr())
        }
    }
}
