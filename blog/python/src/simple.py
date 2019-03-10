import json

import asyncio
import aioredis
import aiohttp

def max_item_url():
    return "https://hacker-news.firebaseio.com/v0/maxitem.json"

def get_item_url(item_id):
    return "https://hacker-news.firebaseio.com/v0/item/{0}.json".format(str(item_id))

async def get_max_item(http):
    while True:
        async with http.get(max_item_url()) as max_item:
            if 200 <= max_item.status < 300:
                return int(await max_item.text())

async def get_hn_item(http, item_id):
    while True:
        async with http.get(get_item_url(item_id)) as item:
            if 200 <= item.status < 300:
                data = await item.text()
                item = json.loads(data)
                if item:
                    return item

async def store_item(http, redis, item_id):
    item = await get_hn_item(http, item_id)
    await store_on_db(redis, item)

async def store_on_db(redis, item):
    try:
        await redis.execute("REDISQL.EXEC_STATEMENT", "HN", "insert_item", 
                item["id"], item.get("by", "_no_author_"), item["time"], json.dumps(item))
    except Exception as e:
        print(e)
        print(item["id"])

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

    statement = "INSERT INTO hn VALUES(?1, ?2, ?3, json(?4));"
    try:
        await redis.execute("REDISQL.CREATE_STATEMENT", "HN", "insert_item", statement)
    except Exception as e:
        print(e)


async def main(redis, http):
    max_item = await get_max_item(http)
    await redis.execute("SET", "max-item", max_item)
    old_max_item = max_item
    while True:
        max_item = await get_max_item(http)
        if max_item > old_max_item:
            for i in range(old_max_item, max_item):
                asyncio.ensure_future(store_item(http, redis, str(i)))
            await redis.execute("SET", "max-item", max_item)
            old_max_item = max_item
        asyncio.sleep(1)


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
        loop.create_task(main(redis, http))
        loop.run_forever()
