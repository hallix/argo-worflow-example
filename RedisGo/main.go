package main

import (
	"context"
	"fmt"

	// "io"
	// "net/http"
	"bufio"
	"os"

	"time"

	"github.com/go-redis/redis/v9"
)

func main() {
	cxt := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	//Dowload file
	file, err := os.Open("data.txt")

	if err != nil {
		fmt.Println("Something went wrong creating the file")
	}

	defer file.Close()

	// resp, err := http.Get("https://raw.githubusercontent.com/curran/data/gh-pages/airbnb/airbnb_session_data.txt")
	// if err != nil {
	// 	fmt.Println("Something went wrong downloading the file")
	// }
	// defer resp.Body.Close()

	// io.Copy(file, resp.Body)

	lines := []string{}
	var datasetSize = 0
	scanner := bufio.NewScanner(file)
	// optionally, resize scanner's capacity for lines over 64K, see next example
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		datasetSize++
	}

	start := time.Now()
	rdb.LPush(cxt, "dataset.observations", lines[1:])
	duration := time.Since(start)
	fmt.Println("exe time", duration)

	rdb.Set(cxt, "dataset.size", datasetSize, 0)
	rdb.Set(cxt, "dataset.features", lines[0], 0)
	fmt.Println("Dataset size:", datasetSize)
}
