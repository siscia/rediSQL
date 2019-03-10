import json

import asyncio
import aioredis
import aiohttp

def max_item_url():
    return "https://hacker-news.firebaseio.com/v0/maxitem.json"

def get_item_url(item_id):
    return "https://hacker-news.firebaseio.com/v0/item/{0}.json".format(str(item_id, 'utf-8'))

async def get_max_item(http):
    while True:
        async with http.get(max_item_url()) as max_item:
            if 200 <= max_item.status < 300:
                return int(await max_item.text())

async def get_item(http, item_id):
    while True:
        async with http.get(get_item_url(item_id)) as item:
            if 200 <= item.status < 300:
                return await item.text()

async def update_max_item(redis, http):
    max_item = await get_max_item(http)
    await redis.execute("SET", "max-item", max_item)
    old_max_item = max_item
    while True:
        max_item = await get_max_item(http)
        if max_item > old_max_item:
            for i in range(old_max_item, max_item):
                await redis.execute("RPUSH", "items", "{0}".format(i))
            await redis.execute("SET", "max-item", max_item)
            old_max_item = max_item

def get_item_from_queue(queue):
    async def f(redis, http):
        while True:
            try:
                item_id = await redis.execute("LPOP", queue)
                if item_id:
                    item = await get_item(http, item_id)
                    if item == "null":
                        pass
                        #await redis.execute("RPUSH", queue, item_id)
                    else:
                        await redis.execute("INCR", "downloaded-items")
                        await store_on_db(redis, item)
            except Exception as e:
                print(e)

    return f

get_items = get_item_from_queue("items")
get_backward = get_item_from_queue("back-items")

async def get_oldest():
    n = await redis.execute("GET", "OLDEST")
    try:
        return int(n)
    except:
        return 19086939

async def going_backward(redis, http):
    oldest = await get_oldest()
    while True:
        for i in range(oldest, oldest - 1000, -1):
            await redis.execute("RPUSH", "back-items", str(i))
        await redis.execute("SET", "OLDEST", str(i))
        oldest = oldest - 1000
        n = 10000
        while n > 500:
            n = int(await redis.execute("LLEN", "back-items"))
            await asyncio.sleep(1)

async def write_old_element(redis):
    for i in range(19086939, 19087939):
        await redis.execute("RPUSH", "items", "{0}".format(i))

async def set_up(redis):
    try:
        await redis.execute("REDISQL.CREATE_DB", "HN")
    except Exception as e:
        print(e)
    
    query = """CREATE TABLE IF NOT EXISTS hn(id integer primary key, author text, time int, item text);"""
    try:
        await redis.execute("REDISQL.EXEC", "HN", query)
    except Exception as e:
        print(e)

    statement = "INSERT INTO hn VALUES(?1, ?2, ?3, ?4)"
    try:
        await redis.execute("REDISQL.CREATE_STATEMENT", "HN", "insert_item", statement)
    except Exception as e:
        print(e)

async def store_on_db(redis, item_text):
    query = "INSERT INTO hn VALUES({}, {}, {}, {});"
    item = json.loads(item_text)
    try:
        await redis.execute("REDISQL.EXEC_STATEMENT", "HN", "insert_item", 
                item["id"], item.get("by", "_no_author_"), item["time"], item_text)
    except Exception as e:
        print(e)
        print(item["id"])

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    conn_co = aioredis.create_pool('redis://localhost', minsize=10, maxsize=300, loop = loop)
    redis_co = asyncio.gather(*[conn_co])
    redis = loop.run_until_complete(redis_co)
    redis = redis[0]

    conn = aiohttp.TCPConnector(limit=100)
    with aiohttp.ClientSession(connector=conn) as http:
        c = asyncio.wait([set_up(redis)])
        loop.run_until_complete(c)
        
        loop.create_task(update_max_item(redis, http))
        for i in range(10):
            loop.create_task(get_items(redis, http))
         
        loop.create_task(going_backward(redis, http))
        for i in range(100):
            loop.create_task(get_backward(redis, http))
    
        loop.run_forever()
