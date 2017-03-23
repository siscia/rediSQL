/*
 *
 * <RediSQL, SQL capabilities to redis.>
 * Copyright (C) 2016  Simone Mosciatti
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * */

#include <stdio.h>
#include <string.h>
#include <pthread.h>

#include <unistd.h>
#include <sys/syscall.h>

#include <stdlib.h>

#include "sqlite3.h"
#include "redismodule.h"
#include "rmutil/util.h"
#include "rmutil/strings.h"
#include "rmutil/test_util.h"

#define rediSQL_ENCODING_VERSION 0

#define LIST_RETURN 0
#define SIMPLE_STRING_RETURN 1
#define ERROR_RETURN 2

#define PERSISTENTSQLITEDB_ENCODING_VERSION 1

static RedisModuleType *DB_Type;

typedef struct Query {
  RedisModuleBlockedClient* blocked_client;
  char* query;
  size_t query_len;
  struct Query* next;
  sqlite3 *connection;
} Query;

typedef struct Queue {
  Query* head;
  Query* tail;
  pthread_mutex_t mutex;
  pthread_cond_t cond;
} Queue;

typedef struct DB {
  char *name;
  sqlite3 *connection;
  Queue *queue;
  pthread_t executor;
} DB;

typedef struct ReturnExec {
  int type_of_return;
  void* to_return;
  char* error;
} ReturnExec;

typedef union SingleSelectEntity {
  int Int;
  char* Float;
  char* Text;
  char* Blob;
  void* Null;
} singleSelectEntity;

typedef struct Entity {
  singleSelectEntity entity;
  int type_entity;
} Entity;

typedef struct SelectBuffer {
  int size;
  int used;
  int num_columns;
  char* error;
  Entity* buffer;
} SelectBuffer;

void ReadAndSave_Integer(sqlite3_stmt *stmt, SelectBuffer* sb, int i){
  int result = sqlite3_column_int(stmt, i);
  int position = (sb->used * sb->num_columns) + i;
  sb->buffer[position].entity.Int = result;
  sb->buffer[position].type_entity = SQLITE_INTEGER;
}

void ReadAndSave_Float(sqlite3_stmt *stmt, SelectBuffer* sb, int i){
  double result = sqlite3_column_double(stmt, i);
  char *converted_result = RedisModule_Alloc(sizeof(char) * (19 + 1));
  snprintf(converted_result, 19 + 1, "%.17g", result);
  int position = (sb->used * sb->num_columns) + i;
  sb->buffer[position].entity.Float = converted_result;
  sb->buffer[position].type_entity = SQLITE_FLOAT;
}

void ReadAndSave_Blob(sqlite3_stmt *stmt, SelectBuffer* sb, int i){
  const char *buffer_result = (char*)sqlite3_column_text(stmt, i);
  char *result = RedisModule_Alloc(sizeof(char) * 
      (strlen(buffer_result) + 1));
  strcpy(result, buffer_result);
  int position = (sb->used * sb->num_columns) + i;
  sb->buffer[position].entity.Blob = result;
  sb->buffer[position].type_entity = SQLITE_BLOB;
}

void ReadAndSave_Null(sqlite3_stmt *stmt, SelectBuffer* sb, int i){
  int position = (sb->used * sb->num_columns) + i;
  sb->buffer[position].entity.Null = NULL;
  sb->buffer[position].type_entity = SQLITE_NULL;
}

void ReadAndSave_Text(sqlite3_stmt *stmt, SelectBuffer* sb, int i){
  const char *buffer_result = (char*)sqlite3_column_text(stmt, i);
  char *result = RedisModule_Alloc(sizeof(char) * 
      (strlen(buffer_result) + 1));
  strcpy(result, buffer_result);
  int position = (sb->used * sb->num_columns) + i;
  sb->buffer[position].entity.Text = result;
  sb->buffer[position].type_entity = SQLITE_TEXT;
}

typedef void (*ReadAndSave_type)(sqlite3_stmt*, SelectBuffer*, int);

Queue* make_queue(){
  Queue* q = RedisModule_Alloc(sizeof(Queue));
  q->head = NULL;
  q->tail = NULL;
  pthread_mutex_init(&(q->mutex), NULL);
  pthread_cond_init(&(q->cond), NULL);
  return q;
}

int push(Queue* queue, Query* query){
  printf("\tPUSH: \tQueue: %p\n", queue);
  printf("\tPUSH: \tMutex: %p = Cond: %p\n", queue->mutex, queue->cond);
  printf("\tPUSH:\tAbout to get the mutex\n");
  pthread_mutex_lock(&(queue->mutex));
  printf("\tPUSH:\tGot the mutex\n");
  
  query->next = NULL;
  
  if (queue->head == NULL){
    printf("\tPUSH: \t A0\n");   
    queue->head = query;
    queue->tail = query;
    printf("\tPUSH: \t A1\n");
  } else {
    printf("\tPUSH: \t B0\n");
    queue->tail->next = query;
    queue->tail       = query;
    printf("\tPUSH: \t B1\n");
  }
  int rc = pthread_cond_signal(&(queue->cond));
  printf("\tPUSH:\tSignal release with code %d\n", rc);
  pthread_mutex_unlock(&(queue->mutex));
  printf("\tPUSH:\tRelease mutex\n");
  return 1;
}

Query* pop(Queue* queue){
  Query* query;
  
  printf("\tPOP: \tQueue: %p\n", queue);
  printf("\tPOP: \tMutex: %p = Cond: %p\n", queue->mutex, queue->cond);
 
  pthread_mutex_lock(&(queue->mutex));
  printf("\tPOP: \tMutex Locked\n");
  while (queue->head == NULL){
    printf("\t\tAbout to wait in POP\n");
    pthread_cond_wait(&(queue->cond), &(queue->mutex));
    printf("\t\tReceived the signal and unblocked \n");
  }
  printf("\tPOP: \tGot the mutex\n");
  query = queue->head;
  queue->head = queue->head->next;

  pthread_mutex_unlock(&(queue->mutex));
  printf("\tPOP: \tRelease mutex\n");
  return query;
}

void exec_func(Query *q){
  
  printf("\t Inside exec_func\n");

  ReturnExec* r = RedisModule_Alloc(sizeof(ReturnExec));
  r->error = NULL;
  
  sqlite3_stmt *stm;
  size_t dimension_error_message = 0;
  int result_code = 0;
  int num_results = 0;
  int num_of_columns = 0;
  int i = 0;
  const char* point_to_rest = NULL;
  const char* error_preparing_format = "ERR - Error during prepare | \
	  Code: %d | \
	  Error %s | \
	  Query: %s | \
	  Leftover: %s | \
	  Query Len: %d | \
	  Counted Len: %d";
  // const char* bad_alloc_error = "ERR - Impossible to allocate memory | Query: %s";
  const char* unknow_return_type_error = "ERR - Unmanaged return code during the execution | Return Code: %d | Error: %s | Query: %s";
  char* error_message;

  sqlite3 *db = q->connection;
 
  printf("\tBefore result code \n");

  result_code = sqlite3_prepare_v2(db, q->query, -1, 
      &stm, &point_to_rest);
 
  printf("\tGot result code %d\n", result_code);

  if (SQLITE_OK != result_code){
    dimension_error_message = snprintf(NULL, 0, error_preparing_format, 
	result_code, sqlite3_errmsg(db), q->query, point_to_rest, q->query_len, strlen(q->query));
    error_message = RedisModule_Alloc(dimension_error_message * sizeof(char));
    snprintf(error_message, dimension_error_message, error_preparing_format, 
	result_code, sqlite3_errmsg(db), q->query, point_to_rest, q->query_len, strlen(q->query));
    
    sqlite3_finalize(stm);

    r->type_of_return = ERROR_RETURN;
    r->to_return = error_message;
    RedisModule_UnblockClient(q->blocked_client, r);
    
    RedisModule_Free(q);
    /*
     *Deallocate Query: q
     * */
    
    return;
  }
 
  printf("Exec prepare!\n");

  result_code = sqlite3_step(stm);

  if (SQLITE_OK == result_code){
    sqlite3_finalize(stm);
    
    r->type_of_return = SIMPLE_STRING_RETURN;
    r->to_return = "OK";
    if (q->blocked_client)
      RedisModule_UnblockClient(q->blocked_client, r);
  }
  else if (SQLITE_ROW == result_code) {
    num_of_columns = sqlite3_column_count(stm);

    ReadAndSave_type* ReadAndSave_Functions = RedisModule_Alloc(sizeof(ReadAndSave_type) * num_of_columns);
      
    for(i = 0; i < num_of_columns; i++){
      switch(sqlite3_column_type(stm, i)){
	case SQLITE_INTEGER: 
	  ReadAndSave_Functions[i] = ReadAndSave_Integer;
	break;
	
	case SQLITE_FLOAT:
	  ReadAndSave_Functions[i] = ReadAndSave_Float;
	break;
	  
	case SQLITE_BLOB:
	  ReadAndSave_Functions[i] = ReadAndSave_Blob;
	break;
	  
	case SQLITE_NULL:
	  ReadAndSave_Functions[i] = ReadAndSave_Null;
	break;
	  	  
	case SQLITE_TEXT:
	  ReadAndSave_Functions[i] = ReadAndSave_Text;
	break;
	
	default:
	  printf("Value type during insert: %d", sqlite3_column_type(stm, i));
      }
    }

    SelectBuffer *m = RedisModule_Alloc(sizeof(SelectBuffer));
    m->used = 0;
    m->size = 30;
    m->num_columns = num_of_columns;
    m->error = NULL;/* TODO allocation error to check*/
    m->buffer = RedisModule_Alloc(m->size * num_of_columns * sizeof(Entity));
    Entity* toRealloc;

    while(SQLITE_ROW == result_code) {
      num_results++;
      if (num_results >= m->size){
	m->size = m->size * 2;
	toRealloc = RedisModule_Realloc(m->buffer, 
	    (m->size * num_of_columns * sizeof(Entity)));
	if (NULL == toRealloc) {
	  /*TODO set error*/
	  printf("Error realloc");
	}
	m->buffer = toRealloc;

	}
      
      
      for(i = 0; i < num_of_columns; i++){
	ReadAndSave_Functions[i](stm, m, i);
      }

      m->used++;
      result_code = sqlite3_step(stm);
    }

    sqlite3_finalize(stm);
    /* TODO check we exit without error */
  
    r->type_of_return = LIST_RETURN;
    r->to_return = m;
    RedisModule_UnblockClient(q->blocked_client, r);
    RedisModule_Free(ReadAndSave_Functions);
  }
  else if (SQLITE_DONE == result_code) {
    sqlite3_finalize(stm);
    
    r->type_of_return = SIMPLE_STRING_RETURN;
    r->to_return = "DONE";
    
    if (q->blocked_client)
      RedisModule_UnblockClient(q->blocked_client, r);
  }
  else {
    dimension_error_message = snprintf(NULL, 0, unknow_return_type_error, result_code, sqlite3_errmsg(db), q->query);
    error_message = RedisModule_Alloc(dimension_error_message * sizeof(char));

    snprintf(error_message, dimension_error_message, unknow_return_type_error, result_code, sqlite3_errmsg(db), q->query);
    sqlite3_finalize(stm);

    r->type_of_return = ERROR_RETURN;
    r->to_return = error_message;
    RedisModule_UnblockClient(q->blocked_client, r);
  }

  RedisModule_Free(q);
  /*
   * Deallocate Query: q
   * */
  return;
}

void print_thread_id(){
	int id = syscall(SYS_gettid);
	printf("\n\n\nThread id: %d\n\n\n", id);
}

void* start_db_thread(Queue *q){
  Queue *queue = (Queue*) q;
  print_thread_id();

  printf("Queue: %p", q);

  Query* query;

  while(1) {
    printf("Waiting for pop\n");
    query = pop(queue);
    printf("Got query\" %s\"", query->query);
    exec_func(query);
  }

}

int createDB(RedisModuleCtx *ctx, RedisModuleString *key_name, const char *path){
  int rc; // return code
  RedisModuleKey *key = RedisModule_OpenKey(ctx, key_name, REDISMODULE_WRITE);

  if (REDISMODULE_KEYTYPE_EMPTY != RedisModule_KeyType(key)){
    RedisModule_CloseKey(key);
    return RedisModule_ReplyWithError(ctx, "KEY_USED The key used is already bind");
  }
 
  DB *DB = RedisModule_Alloc(sizeof(DB));
  DB->name = RedisModule_Alloc(sizeof(char) * 124); //TODO count
  DB->queue = make_queue();
  
  printf("\tDBC: \tDB: %p = Queue: %p\n", DB, DB->queue);
  printf("\tDBC: \tMutex: %p = Cond: %p\n", DB->queue->mutex, DB->queue->cond);
  
  if (NULL == path){
    strcpy(DB->name, ":memory:");
    rc = sqlite3_open(":memory:", &DB->connection);
  } else {
    strcpy(DB->name, path);
    rc = sqlite3_open(path, &DB->connection);
  }

  if (SQLITE_OK != rc){
    RedisModule_CloseKey(key);
    return RedisModule_ReplyWithError(ctx, "ERR - Problem opening the database");
  }
  
  Query* qtest = RedisModule_Alloc(sizeof(Query));
  qtest->query = "create table test (a int);";
  qtest->connection = DB->connection;
  qtest->blocked_client = NULL;
    
  pthread_create(&(DB->executor), NULL, start_db_thread, DB->queue);
  
  if (REDISMODULE_OK == RedisModule_ModuleTypeSetValue(
	key, DB_Type, DB)){
    
    RedisModule_CloseKey(key);
    key = RedisModule_OpenKey(ctx, key_name, REDISMODULE_WRITE);
    DB = RedisModule_ModuleTypeGetValue(key);
    
    printf("\tDBC2: \tDB: %p = Queue: %p\n", &(DB), DB->queue);
    printf("\tDBC2: \tMutex: %p = Cond: %p\n", &(DB->queue->mutex), &(DB->queue->cond));
  

    //sleep(1);
    //push(DB->queue, qtest);
 
 
    RedisModule_CloseKey(key);
    
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
  } else {
    RedisModule_CloseKey(key);
    return RedisModule_ReplyWithError(ctx, "ERR - Impossible to set the key");
  }
  
}

int CreateDB(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
  const char *path;
  if (2 == argc){
    return createDB(ctx, argv[1], NULL);
  }
  if (3 == argc){
    path = RedisModule_StringPtrLen(argv[2], NULL); 
    return createDB(ctx, argv[1], path);
  }
  return RedisModule_WrongArity(ctx);
}

int DeleteDB(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
  // TODO check if I am leaking memory in the queue.
  if (2 != argc){
    return RedisModule_WrongArity(ctx);
  }
  RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE);
  
  if (REDISMODULE_KEYTYPE_EMPTY == RedisModule_KeyType(key)){
    RedisModule_CloseKey(key);
    return RedisModule_ReplyWithError(ctx, "KEY_EMPTY The key used is empty");
  }
 
  if (DB_Type !=  RedisModule_ModuleTypeGetType(key)){
    RedisModule_CloseKey(key);
    return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
  }

  RedisModule_DeleteKey(key);

  return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

void free_queue(Queue* q){
  pthread_mutex_destroy(&(q->mutex));
  pthread_cond_destroy(&(q->cond));
  RedisModule_Free(q);
}

int reply_func(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
  ReturnExec *r = RedisModule_GetBlockedClientPrivateData(ctx);
  
  int totalRow = 0;
  int rowIndex = 0;
  int columnIndex = 0;
  int index = 0;
  const char * text_result;
  const char * blob_result;
  const char * float_result;
  const SelectBuffer *sb;
      
  switch (r->type_of_return){
    case (LIST_RETURN):
      sb = r->to_return;

      RedisModule_ReplyWithArray(ctx, sb->used);

      for(rowIndex = 0; rowIndex < sb->used; rowIndex++){
	
	RedisModule_ReplyWithArray(ctx, sb->num_columns);
	
	for (columnIndex = 0; 
	     columnIndex < sb->num_columns; 
	     columnIndex++){

	  index = (rowIndex * sb->num_columns) + columnIndex;
	  switch(sb->buffer[index].type_entity){
	    case SQLITE_INTEGER:
	      RedisModule_ReplyWithLongLong(ctx, 
		  sb->buffer[index].entity.Int);
	      break;
	    case SQLITE_FLOAT:
	      float_result = sb->buffer[index].entity.Float;
	      RedisModule_ReplyWithStringBuffer(ctx,
		  float_result, strlen(float_result));
	      break;
	    case SQLITE_BLOB:
	      blob_result = (char*)sb->buffer[index].entity.Blob;
	      RedisModule_ReplyWithStringBuffer(ctx,
		  blob_result, strlen(blob_result));
	      break;
	    case SQLITE_TEXT:
	      text_result = sb->buffer[index].entity.Text;
	      RedisModule_ReplyWithStringBuffer(ctx, 
		  text_result, strlen(text_result));
	      break;
	    case SQLITE_NULL:
	      RedisModule_ReplyWithNull(ctx);
	      break;
	  }
	}
	totalRow++;
      }
      return REDISMODULE_OK;
      break;
    case SIMPLE_STRING_RETURN:
      return RedisModule_ReplyWithSimpleString(ctx, r->to_return);
      break;
    case ERROR_RETURN:
      return RedisModule_ReplyWithError(ctx, r->to_return);
      break;
  }
  return RedisModule_ReplyWithError(ctx, "Error returning from thread");
}

int timeout_func(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
  return RedisModule_ReplyWithError(ctx, "TIMEOUT");
}

void free_privdata(void *privdata){
  ReturnExec *r = privdata;
  int rowIndex, columnIndex, index;
  SelectBuffer *sb;
  switch (r->type_of_return){
    case LIST_RETURN:
      sb = (SelectBuffer*)r->to_return;
      
      for(rowIndex = 0; rowIndex < sb->used; rowIndex++){
	for(columnIndex = 0; 
	    columnIndex < sb->num_columns;
	    columnIndex++){
	  index = (rowIndex * sb->num_columns) + columnIndex;
	  switch(sb->buffer[index].type_entity){
	    case SQLITE_BLOB:
	      RedisModule_Free(sb->buffer[index].entity.Blob);
	      break;
	    case SQLITE_TEXT:
	      RedisModule_Free(sb->buffer[index].entity.Text);
	      break;
	    case SQLITE_FLOAT:
	      RedisModule_Free(sb->buffer[index].entity.Float);
	      break;
	    default:
	      break;
	  }
	}
      }
      
      RedisModule_Free(sb->buffer);
      
      RedisModule_Free(sb->error);
      RedisModule_Free(r->to_return);
      break;
    case SIMPLE_STRING_RETURN:
      break;
    case ERROR_RETURN:
      RedisModule_Free(r->error);
      break;
  }
  RedisModule_Free(r);
}

int ExecCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
  RedisModuleKey *key;
  int key_type;
  DB* DB;

  if (3 != argc){
    return RedisModule_WrongArity(ctx);
  }

  RedisModule_Log(ctx, "notice", "KEY: %s\n", RedisModule_StringPtrLen(argv[1], NULL));

  key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE);
  key_type = RedisModule_KeyType(key);  
 
  if (REDISMODULE_KEYTYPE_EMPTY          == key_type ||
      RedisModule_ModuleTypeGetType(key) != DB_Type ){  
    return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
  }
  
  DB = RedisModule_ModuleTypeGetValue(key);
  
  printf("\tEXEC: \tDB: %p = Queue: %p\n", DB, DB->queue);
  printf("\tEXEC: \tMutex: %p = Cond: %p\n", DB->queue->mutex, DB->queue->cond);

  RedisModuleBlockedClient *bc =
	  RedisModule_BlockClient(ctx, reply_func, timeout_func, free_privdata, 10 * 1000);
  
  Query *q = RedisModule_Alloc(sizeof(Query));

  q->blocked_client = bc;
  q->connection = DB->connection;

  const char* buffer_query;
  buffer_query = RedisModule_StringPtrLen(argv[2], NULL); 
  q->query = RedisModule_Alloc(sizeof(char) * (strlen(buffer_query) + 1));
  strcpy(q->query, buffer_query);
  q->query_len = strlen(q->query);

  RedisModule_Log(ctx, "notice", "About to push!");

  push(DB->queue, q);
  
  RedisModule_Log(ctx, "notice", "Pushed!");

  return REDISMODULE_OK;
}

int SQLiteVersion(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
  if (argc != 1)
    return RedisModule_WrongArity(ctx);

  return RedisModule_ReplyWithSimpleString(ctx, sqlite3_version);
}

void FreeDB(void *pDB){
  DB *realDB = (DB*)pDB;

  RedisModule_Free(realDB->name);
  sqlite3_close(realDB->connection);
}

int RedisModule_OnLoad(RedisModuleCtx *ctx) {
  if (RedisModule_Init(ctx, "rediSQL__", 1, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
   
  RedisModuleTypeMethods tm = {
	.version = PERSISTENTSQLITEDB_ENCODING_VERSION,
	.rdb_load = NULL,
	.rdb_save = NULL,
	.aof_rewrite = NULL,
	.free = FreeDB};

  DB_Type = RedisModule_CreateDataType(
	ctx, "Per_DB_Co", 
	PERSISTENTSQLITEDB_ENCODING_VERSION, &tm);

  if (RedisModule_CreateCommand(ctx, "rediSQL.EXEC", ExecCommand, 
	"deny-oom random no-cluster", 1, 1, 1) == REDISMODULE_ERR){
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "rediSQL.SQLITE_VERSION", SQLiteVersion, "readonly", 1, 1, 1) == REDISMODULE_ERR){
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "rediSQL.CREATE_DB", CreateDB, "write deny-oom no-cluster", 1, 1, 1) == REDISMODULE_ERR){
    return REDISMODULE_ERR;
  }

  if (RedisModule_CreateCommand(ctx, "rediSQL.DELETE_DB", DeleteDB, "write no-cluster", 1, 1, 1) == REDISMODULE_ERR){
    return REDISMODULE_ERR;
  }


  return REDISMODULE_OK;
}

