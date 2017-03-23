CC=gcc
CFLAGS=-g -Wall -Wpedantic -fPIC -c
LD=ld

all: sqlite3.o rediSQL.c RedisModulesSDK/rmutil/librmutil.a
	$(CC) $(CFLAGS) -O3 -IRedisModulesSDK/ -Bstatic rediSQL.c -o rediSQL.o
	$(LD) -g -o rediSQL.so rediSQL.o sqlite3.o RedisModulesSDK/rmutil/librmutil.a --shared -lc

RedisModulesSDK/rmutil/librmutil.a:
	$(MAKE) -C RedisModulesSDK/rmutil

direct_test:
	$(CC) -g -o test_direct

sqlite3.o: sqlite3.c
	$(CC) -fPIC -g -O3 -c -o sqlite3.o sqlite3.c

test_direct: test_direct.c
	$(CC) -g test_direct.c -o test_direct sqlite3.o -lpthread -ldl

test_queue: test_queue.c
	$(CC) -g test_queue.c -IC-Thread-Pool/ thpool.o  sqlite3.o -lpthread -ldl -o test_queue  

test_custom_queue: test_custom_queue.c
	$(CC) -g test_custom_queue.c -IC-Thread-Pool/ thpool.o sqlite3.o -lpthread -ldl -o test_custom_queue  
