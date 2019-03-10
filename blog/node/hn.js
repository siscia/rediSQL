const util = require('util');
const axios = require('axios');
const redis = require('redis');

const client = redis.createClient();
const send_command = util.promisify(client.send_command).bind(client); 

const getMaxItem = async () => {
        let response = await axios.get("https://hacker-news.firebaseio.com/v0/maxitem.json");
        return response.data;
};

const getItem = async id => {
        let url = util.format("https://hacker-news.firebaseio.com/v0/item/%s.json", id);
        while (true) {
                let response = await axios.get(url);
                let data = response.data;
                if (data != "null") {
                        return data;
                }
        }
};

const storeItem = async id => {
        let item = await getItem(id);
        console.log(item);
        await send_command("REDISQL.EXEC_STATEMENT", ['HN', 'insert_item', JSON.stringify(item)])
                .catch( err => console.log(err) );
}

const setUp = async () => {
        await send_command("REDISQL.CREATE_DB", ["HN"]).catch( err => console.log(err) );
        table = "CREATE TABLE IF NOT EXISTS hn(id integer primary key, author text, time int, item text);"
        await send_command("REDISQL.EXEC", ['HN', table]).catch( err => console.log(err) );
        stmt = "INSERT INTO hn VALUES(" + 
		"json_extract(json(?1),'$.id')," +
		"json_extract(json(?1),'$.by')," +
		"json_extract(json(?1),'$.time')," +
		"json(?1));";
        await send_command("REDISQL.CREATE_STATEMENT", ['HN', 'insert_item', stmt])
                .catch( err => console.log(err) );
};

const sleep = (waitTimeInMs) => new Promise(resolve => setTimeout(resolve, waitTimeInMs));

(async () => {
        setUp();
        let maxItem = await getMaxItem();
        while (true) {
                let newMaxItem = await getMaxItem();
                for (; maxItem < newMaxItem; maxItem += 1) {
                        storeItem(maxItem);
                }
                await sleep(5 * 1000);
        }
})()
