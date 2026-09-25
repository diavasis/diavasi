package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/diavasis/diavasi/clients/go"
)

func main() {
	args := map[string]string{}
	osArgs := os.Args[1:]
	for i := 0; i < len(osArgs); i += 2 {
		args[osArgs[i]] = osArgs[i+1]
	}
	total, _ := strconv.ParseUint(args["--total"], 10, 64)
	halt, _ := strconv.ParseUint(args["--halt-after"], 10, 32)
	maxInFlight, _ := strconv.ParseUint(args["--max-in-flight"], 10, 32)
	if maxInFlight == 0 {
		maxInFlight = 1
	}
	consumer := args["--consumer"]
	if consumer == "" {
		consumer = "go"
	}
	ctx, cancel := context.WithTimeout(context.Background(), diavasi.Timeout)
	defer cancel()
	report, err := diavasi.Consume(ctx, diavasi.Options{
		Addr:          args["--addr"],
		CAFile:        args["--ca"],
		Token:         args["--token"],
		GroupID:       args["--group"],
		ConsumerID:    consumer,
		MaxInFlight:   uint32(maxInFlight),
		HaltAfterAcks: uint32(halt),
		ExpectRecords: total,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		if protocol, ok := err.(*diavasi.ProtocolError); ok && protocol.Code >= 1 && protocol.Code <= 8 {
			os.Exit(int(protocol.Code))
		}
		os.Exit(1)
	}
	ids := make([]string, len(report.RecordIDs))
	for i, id := range report.RecordIDs {
		ids[i] = strconv.FormatUint(id, 10)
	}
	batches := make([]string, len(report.BatchIDs))
	for i, id := range report.BatchIDs {
		batches[i] = strconv.FormatUint(id, 10)
	}
	fmt.Println("record_ids " + strings.Join(ids, " "))
	fmt.Println("batch_ids " + strings.Join(batches, " "))
	fmt.Printf("go consumed %d records in %d batches\n", len(report.RecordIDs), len(report.BatchIDs))
}
