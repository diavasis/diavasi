package diavasi

import (
	"context"
	"os"
	"strconv"
	"testing"
)

func TestConsumeSkipsWithoutServer(t *testing.T) {
	addr := os.Getenv("DIAVASI_DATA_ADDR")
	ca := os.Getenv("DIAVASI_CA")
	token := os.Getenv("DIAVASI_API_TOKEN")
	if addr == "" || ca == "" || token == "" {
		t.Skip("DIAVASI_DATA_ADDR, DIAVASI_CA, and DIAVASI_API_TOKEN are unset")
	}
	group := os.Getenv("DIAVASI_GROUP")
	if group == "" {
		group = "sdk"
	}
	total := uint64(8)
	if raw := os.Getenv("DIAVASI_TOTAL"); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		total = parsed
	}
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	report, err := Consume(ctx, Options{
		Addr:          addr,
		CAFile:        ca,
		Token:         token,
		GroupID:       group,
		ConsumerID:    "go-test",
		ExpectRecords: total,
	})
	if err != nil {
		t.Fatal(err)
	}
	if uint64(len(report.RecordIDs)) != total {
		t.Fatalf("records %d", len(report.RecordIDs))
	}
}

func TestMissingGroup(t *testing.T) {
	addr := os.Getenv("DIAVASI_DATA_ADDR")
	ca := os.Getenv("DIAVASI_CA")
	token := os.Getenv("DIAVASI_API_TOKEN")
	if addr == "" || ca == "" || token == "" {
		t.Skip("DIAVASI_DATA_ADDR, DIAVASI_CA, and DIAVASI_API_TOKEN are unset")
	}
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	_, err := Consume(ctx, Options{
		Addr:       addr,
		CAFile:     ca,
		Token:      token,
		GroupID:    "sdk-missing",
		ConsumerID: "go-missing",
	})
	protocol, ok := err.(*ProtocolError)
	if !ok || protocol.Code != 5 {
		t.Fatalf("got %v", err)
	}
}
