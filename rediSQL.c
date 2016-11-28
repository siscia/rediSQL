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

#include "sqlite3.h"
#include "redismodule.h"
#include "rmutil/util.h"
#include "rmutil/strings.h"
#include "rmutil/test_util.h"

#define rediSQL_ENCODING_VERSION 0

#define LIST_RETURN 0
#define SIMPLE_STRING_RETURN 1
#define ERROR_RETURN 2

typedef sqlite3 redisSQL;

typedef struct {
  RedisModuleCtx* ctx;
  RedisModuleBlockedClient* blocked_client;
  const char* query;
  size_t query_len;
} Query;

typedef struct {
  int type_of_return;
  const void* to_return;
} ReturnExec;

typedef union {
  int Int;
  float Float;
  const char* Text;
  const char* Blob;
  void* Null;
} singleSelectEntity;

typedef struct {
  singleSelectEntity entity;
  int type_entity;
} Entity;

typedef struct SelectBuffer {
  int size;
  int used;
  int num_columns;
  struct SelectBuffer* next;
  Entity* buffer;
} SelectBuffer;

redisSQL *db;

typedef void (*ReadAndSave_type)(sqlite3_stmt*, SelectBuffer*, int);

void ReadAndSave_Integer(sqlite3_stmt *stmt, SelectBuffer* sb, int i){
  int result = sqlite3_column_int(stmt, i);
  int position = (sb->used * sb->num_columns) + i;
  sb->buffer[position].entity.Int = result;
  sb->buffer[position].type_entity = SQLITE_INTEGER;
}

void ReadAndSave_Float(sqlite3_stmt *stmt, SelectBuffer* sb, int i){
  double result = sqlite3_column_double(stmt, i);
  int position = (sb->used * sb->num_columns) + i;
  sb->buffer[position].entity.Float = result;
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


int reply_func(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
  ReturnExec *r = RedisModule_GetBlockedClientPrivateData(ctx);
  
  int totalRow = 0;
  int rowIndex = 0;
  int columnIndex = 0;
  int index = 0;
  const char * text_result;
  const char * blob_result;
  const SelectBuffer *sb;
      
  switch (r->type_of_return){
    case (LIST_RETURN):
      RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
      sb = r->to_return;

      for(rowIndex = 0; rowIndex < sb->used; rowIndex++){
	
	RedisModule_ReplyWithArray(ctx, sb->num_columns);
	
	for (columnIndex = 0; 
	     columnIndex < sb->num_columns; 
	     columnIndex++){

	  index = (rowIndex * sb->num_columns) + columnIndex;
	  
	  switch( sb->buffer[index].type_entity){
	    case SQLITE_INTEGER:
	      RedisModule_ReplyWithLongLong(ctx, 
		sb->buffer[index].entity.Int);
	      break;
	    case SQLITE_FLOAT:
	      RedisModule_ReplyWithDouble(ctx,
		  sb->buffer[index].entity.Float);
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
      RedisModule_ReplySetArrayLength(ctx, totalRow);
      break;
    case SIMPLE_STRING_RETURN:
      return RedisModule_ReplyWithString(ctx, r->to_return);
      break;
    case ERROR_RETURN:
      return RedisModule_ReplyWithError(ctx, r->to_return);
      break;
  }
}

int timeout_func(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
  return 0;
}

void free_privdata(void *privdata){
  ReturnExec *r = privdata;
  switch (r->type_of_return){
    case LIST_RETURN:
      break;
    case SIMPLE_STRING_RETURN:
      break;
    case ERROR_RETURN:
      break;
  }
}

void *exec_func(void *arg){
  Query *q = arg;
  
  ReturnExec* r = RedisModule_Alloc(sizeof(ReturnExec));

  sqlite3_stmt *stm;
  int result_code = 0;
  int num_results = 0;
  int num_of_columns = 0;
  int i = 0;

  result_code = sqlite3_prepare_v2(db, q->query, q->query_len, 
      &stm, NULL);
  
  if (SQLITE_OK != result_code){
    RedisModuleString *e = RedisModule_CreateStringPrintf(q->ctx, 
		    "ERR - %s | Query: %s", sqlite3_errmsg(db), q->query);
    sqlite3_finalize(stm);

    r->type_of_return = ERROR_RETURN;
    r->to_return = RedisModule_StringPtrLen(e, NULL);
    RedisModule_UnblockClient(q->blocked_client, r);
  }

  result_code = sqlite3_step(stm);
    
  if (SQLITE_OK == result_code){
    sqlite3_finalize(stm);
    
    r->type_of_return = SIMPLE_STRING_RETURN;
    r->to_return = RedisModule_CreateString(q->ctx, "OK", 2);
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
	
      }
    }

    SelectBuffer *m = RedisModule_Alloc(sizeof(SelectBuffer));
    m->used = 0;
    m->size = 30;
    m->num_columns = num_of_columns;
    /* TODO allocation error to check*/
    m->buffer = RedisModule_Alloc(m->size * num_of_columns * sizeof(singleSelectEntity));
    m->next = NULL;

    while(SQLITE_ROW == result_code) {
      num_results++;

      for(i = 0; i < num_of_columns; i++){
	ReadAndSave_Functions[i](stm, m, i);
      }
      m->used++;
      result_code = sqlite3_step(stm);
      if (num_results > 30){
      	RedisModule_Log(q->ctx, "notice", "Too many elements");
      }
    }

    sqlite3_finalize(stm);
    /* TODO check we exit without error */
  
    r->type_of_return = LIST_RETURN;
    r->to_return = m;
    RedisModule_UnblockClient(q->blocked_client, r);
  }
  else if (SQLITE_DONE == result_code) {
    sqlite3_finalize(stm);
    
    r->type_of_return = SIMPLE_STRING_RETURN;
    r->to_return = RedisModule_CreateString(q->ctx, "OK", 2);
    RedisModule_UnblockClient(q->blocked_client, r);
  }
  else {
    RedisModuleString *e = RedisModule_CreateStringPrintf(q->ctx, 
		    "ERR - %s | Query: %s", sqlite3_errmsg(db), q->query);
    sqlite3_finalize(stm);

    r->type_of_return = ERROR_RETURN;
    r->to_return = RedisModule_StringPtrLen(e, NULL);
    RedisModule_UnblockClient(q->blocked_client, r);
  }
}

int ExecCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
  RedisModule_AutoMemory(ctx);
  if (argc != 2){
    return RedisModule_WrongArity(ctx);
  }

  RedisModuleBlockedClient *bc =
	  RedisModule_BlockClient(ctx, reply_func, timeout_func, free_privdata, 1000);
  
  Query *q = RedisModule_Alloc(sizeof(Query));

  q->ctx = ctx;
  q->blocked_client = bc;
  q->query = RedisModule_StringPtrLen(argv[1], &(q->query_len)); 
  
  pthread_t tid;
  pthread_create(&tid, NULL, exec_func, q);
  
  return REDISMODULE_OK;
}



int RedisModule_OnLoad(RedisModuleCtx *ctx) {
  if (RedisModule_Init(ctx, "rediSQL__", 1, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  int rc;
  
  rc = sqlite3_open(":memory:", &db);
  if (rc != SQLITE_OK)
    return REDISMODULE_ERR;

  if (RedisModule_CreateCommand(ctx, "rediSQL.exec", ExecCommand, 
	"deny-oom random no-cluster", 1, 1, 1) == REDISMODULE_ERR){
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

