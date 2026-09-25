package diavasi

import (
	"bytes"
	"testing"
)

func TestHelloFrame(t *testing.T) {
	if !bytes.Equal(helloFrame(), []byte{0x08, 0x01, 0x12, 0x02, 0x08, 0x01}) {
		t.Fatalf("hello %x", helloFrame())
	}
}

func TestAckFrame(t *testing.T) {
	if !bytes.Equal(ackFrame(1), []byte{0x08, 0x01, 0x3a, 0x02, 0x08, 0x01}) {
		t.Fatalf("ack %x", ackFrame(1))
	}
}

func TestBatchRoundTrip(t *testing.T) {
	record := append(fieldVarint(1, 7), fieldBytes(2, []byte("ab"))...)
	inner := append(fieldVarint(1, 1), fieldBytes(2, record)...)
	frame := envelope(6, inner)
	kind, body, err := decodeEnvelope(frame)
	if err != nil {
		t.Fatal(err)
	}
	if kind != kindBatch {
		t.Fatalf("kind %d", kind)
	}
	batch, err := decodeBatch(body)
	if err != nil {
		t.Fatal(err)
	}
	if batch.BatchID != 1 || len(batch.Records) != 1 || batch.Records[0].RecordID != 7 {
		t.Fatalf("%+v", batch)
	}
	if string(batch.Records[0].Payload) != "ab" {
		t.Fatalf("payload %q", batch.Records[0].Payload)
	}
}

func TestErrorFrame(t *testing.T) {
	inner := append(fieldVarint(1, 5), fieldString(2, "group is not running")...)
	kind, body, err := decodeEnvelope(envelope(11, inner))
	if err != nil {
		t.Fatal(err)
	}
	if kind != kindError {
		t.Fatalf("kind %d", kind)
	}
	code, message := decodeError(body)
	if code != 5 || message != "group is not running" {
		t.Fatalf("%d %s", code, message)
	}
}
