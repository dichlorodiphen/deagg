package main

import (
	"deagg/pkg/deagg"
	"fmt"
	"log"
	"os"
)

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintf(os.Stderr, "usage: %s <stream name> <sequence number> <shard id>", os.Args[0])
		os.Exit(1)
	}
	streamName := os.Args[1]
	seq := os.Args[2]
	shardId := os.Args[3]

	reader := Must(deagg.NewReader(streamName))
	data := Must(reader.Read(seq, shardId))
	records := Must(deagg.Deaggregate(data))
	for _, r := range records {
		fmt.Print(string(r))
	}
}

func Must[T any](res T, err error) T {
	if err != nil {
		log.Fatal(err)
	}
	return res
}
