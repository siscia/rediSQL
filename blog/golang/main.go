package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/mediocregopher/radix"
)

func getMaxItem() int {
	resp, _ := http.Get("https://hacker-news.firebaseio.com/v0/maxitem.json")
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	n, _ := strconv.Atoi(string(body))
	return n
}

func getItem(id int) string {
	for {
		url := fmt.Sprintf("https://hacker-news.firebaseio.com/v0/item/%d.json", id)
		resp, _ := http.Get(url)
		defer resp.Body.Close()
		body, _ := ioutil.ReadAll(resp.Body)
		result := string(body)
		if result != "null" {
			return result
		}
	}
}

func setUp(r radix.Client) {
	r.Do(radix.Cmd(nil, "REDISQL.CREATE_DB", "HN"))
	table := "CREATE TABLE IF NOT EXISTS hn(id integer primary key, author text, time int, item text);"
	r.Do(radix.Cmd(nil, "REDISQL.EXEC", "HN", table))
	stmt := `INSERT INTO hn VALUES(
		json_extract(json(?1),'$.id'), 
		json_extract(json(?1),'$.by'), 
		json_extract(json(?1),'$.time'), 
		json(?1));`
	r.Do(radix.Cmd(nil, "REDISQL.CREATE_STATEMENT", "HN", "insert_item", stmt))
}

func main() {

	redis, err := radix.NewPool("tcp", "localhost:6379", 10)
	if err != nil {
		fmt.Println(err)
		return
	}
	setUp(redis)

	itemIds := make(chan int, 10)
	go func() {
		oldMaxItemId := getMaxItem()
		for {
			newMaxItemId := getMaxItem()
			for ; oldMaxItemId < newMaxItemId; oldMaxItemId++ {
				itemIds <- oldMaxItemId
			}
			time.Sleep(5 * time.Second)
		}

	}()

	for itemId := range itemIds {
		go func() {
			item := getItem(itemId)
			err := redis.Do(radix.Cmd(nil, "REDISQL.EXEC_STATEMENT", "HN", "insert_item", item))
			if err != nil {
				fmt.Println(err)
			}
		}()
	}
}
