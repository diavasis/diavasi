// Package diavasi is a thin client for the diavasi.data.v1 Consume stream.
// The caller acks by batch id. This package does not store a cursor.
package diavasi

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type rawCodec struct{}

func (rawCodec) Marshal(v any) ([]byte, error) {
	switch body := v.(type) {
	case []byte:
		return body, nil
	case *[]byte:
		return *body, nil
	default:
		return nil, fmt.Errorf("raw codec wants []byte, got %T", v)
	}
}

func (rawCodec) Unmarshal(data []byte, v any) error {
	target, ok := v.(*[]byte)
	if !ok {
		return fmt.Errorf("raw codec wants *[]byte, got %T", v)
	}
	*target = append([]byte(nil), data...)
	return nil
}

func (rawCodec) Name() string { return "proto" }

// Record is one element of a batch. record_id may be 0.
type Record struct {
	RecordID uint64
	Payload  []byte
}

// Batch is one RecordBatch from the server.
type Batch struct {
	BatchID uint64
	Records []Record
}

// ProtocolError is an Envelope error frame, codes 1 through 8.
type ProtocolError struct {
	Code    uint32
	Message string
}

func (e *ProtocolError) Error() string {
	return fmt.Sprintf("protocol error %d: %s", e.Code, e.Message)
}

// CallError is a gRPC status, including a bad token.
type CallError struct {
	Status  string
	Message string
}

func (e *CallError) Error() string {
	return fmt.Sprintf("grpc %s: %s", e.Status, e.Message)
}

// Report is the acked record ids and batch ids from one consume.
type Report struct {
	RecordIDs []uint64
	BatchIDs  []uint64
}

// Options selects the data plane and the group.
type Options struct {
	Addr          string
	CAFile        string
	Token         string
	GroupID       string
	ConsumerID    string
	MaxInFlight   uint32
	HaltAfterAcks uint32
	ExpectRecords uint64
}

// Consume joins group, acks each batch, and leaves unless HaltAfterAcks is set.
func Consume(ctx context.Context, opts Options) (*Report, error) {
	if opts.MaxInFlight == 0 {
		opts.MaxInFlight = 1
	}
	pem, err := os.ReadFile(opts.CAFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("ca file has no certificates")
	}
	creds := credentials.NewTLS(&tls.Config{RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS12})
	conn, err := grpc.NewClient(
		opts.Addr,
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(rawCodec{})),
	)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+opts.Token)
	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{
		StreamName:    "Consume",
		ServerStreams: true,
		ClientStreams: true,
	}, "/diavasi.data.v1.DataPlane/Consume")
	if err != nil {
		return nil, callErr(err)
	}
	if err := stream.SendMsg(helloFrame()); err != nil {
		return nil, callErr(err)
	}

	report := &Report{}
	sentFlow := false
	for {
		var frame []byte
		if err := stream.RecvMsg(&frame); err != nil {
			if opts.ExpectRecords > 0 && uint64(len(report.RecordIDs)) >= opts.ExpectRecords {
				return report, nil
			}
			return nil, callErr(err)
		}
		kind, body, err := decodeEnvelope(frame)
		if err != nil {
			return nil, err
		}
		switch kind {
		case kindHelloAck:
			if err := stream.SendMsg(joinFrame(opts.GroupID, opts.ConsumerID)); err != nil {
				return nil, callErr(err)
			}
		case kindJoined:
			if !sentFlow {
				sentFlow = true
				if err := stream.SendMsg(flowFrame(opts.MaxInFlight)); err != nil {
					return nil, callErr(err)
				}
			}
		case kindBatch:
			batch, err := decodeBatch(body)
			if err != nil {
				return nil, err
			}
			for _, record := range batch.Records {
				report.RecordIDs = append(report.RecordIDs, record.RecordID)
			}
			if err := stream.SendMsg(ackFrame(batch.BatchID)); err != nil {
				return nil, callErr(err)
			}
			report.BatchIDs = append(report.BatchIDs, batch.BatchID)
			if opts.HaltAfterAcks > 0 && uint32(len(report.BatchIDs)) >= opts.HaltAfterAcks {
				var extra []byte
				_ = stream.RecvMsg(&extra)
				return report, nil
			}
			if opts.ExpectRecords > 0 && uint64(len(report.RecordIDs)) >= opts.ExpectRecords {
				if err := stream.SendMsg(leaveFrame()); err != nil {
					return nil, callErr(err)
				}
				_ = stream.CloseSend()
				return report, nil
			}
		case kindHeartbeat:
			if err := stream.SendMsg(heartbeatFrame()); err != nil {
				return nil, callErr(err)
			}
		case kindError:
			code, message := decodeError(body)
			return nil, &ProtocolError{Code: code, Message: message}
		}
	}
}

func callErr(err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return &CallError{Status: "UNKNOWN", Message: err.Error()}
	}
	return &CallError{Status: st.Code().String(), Message: st.Message()}
}

const (
	kindOther = iota
	kindHelloAck
	kindJoined
	kindBatch
	kindHeartbeat
	kindError
)

func helloFrame() []byte {
	return envelope(2, fieldVarint(1, 1))
}

func joinFrame(group, consumer string) []byte {
	inner := append(fieldString(1, group), fieldString(2, consumer)...)
	return envelope(4, inner)
}

func flowFrame(max uint32) []byte {
	return envelope(10, fieldVarint(1, uint64(max)))
}

func ackFrame(batchID uint64) []byte {
	return envelope(7, fieldVarint(1, batchID))
}

func heartbeatFrame() []byte {
	return envelope(9, nil)
}

func leaveFrame() []byte {
	return envelope(12, nil)
}

func envelope(field int, inner []byte) []byte {
	body := append(fieldVarint(1, 1), fieldBytes(field, inner)...)
	return body
}

func fieldVarint(field int, value uint64) []byte {
	return append(varint(uint64(field)<<3), varint(value)...)
}

func fieldString(field int, value string) []byte {
	return fieldBytes(field, []byte(value))
}

func fieldBytes(field int, value []byte) []byte {
	tag := varint(uint64(field)<<3 | 2)
	return append(append(tag, varint(uint64(len(value)))...), value...)
}

func varint(value uint64) []byte {
	var out []byte
	for value >= 0x80 {
		out = append(out, byte(value)|0x80)
		value >>= 7
	}
	return append(out, byte(value))
}

func decodeEnvelope(frame []byte) (int, []byte, error) {
	i := 0
	kind := kindOther
	var body []byte
	for i < len(frame) {
		key, n, err := readVarint(frame[i:])
		if err != nil {
			return 0, nil, err
		}
		i += n
		field := int(key >> 3)
		wire := key & 7
		switch wire {
		case 0:
			_, n, err := readVarint(frame[i:])
			if err != nil {
				return 0, nil, err
			}
			i += n
		case 2:
			length, n, err := readVarint(frame[i:])
			if err != nil {
				return 0, nil, err
			}
			i += n
			if uint64(len(frame[i:])) < length {
				return 0, nil, fmt.Errorf("truncated field %d", field)
			}
			payload := frame[i : i+int(length)]
			i += int(length)
			switch field {
			case 3:
				kind = kindHelloAck
			case 5:
				kind = kindJoined
			case 6:
				kind = kindBatch
				body = payload
			case 9:
				kind = kindHeartbeat
			case 11:
				kind = kindError
				body = payload
			}
		default:
			return 0, nil, fmt.Errorf("unsupported wire type %d", wire)
		}
	}
	return kind, body, nil
}

func decodeBatch(body []byte) (Batch, error) {
	batch := Batch{}
	i := 0
	for i < len(body) {
		key, n, err := readVarint(body[i:])
		if err != nil {
			return batch, err
		}
		i += n
		field := int(key >> 3)
		wire := key & 7
		if wire == 0 {
			value, n, err := readVarint(body[i:])
			if err != nil {
				return batch, err
			}
			i += n
			if field == 1 {
				batch.BatchID = value
			}
			continue
		}
		if wire != 2 {
			return batch, fmt.Errorf("bad batch wire %d", wire)
		}
		length, n, err := readVarint(body[i:])
		if err != nil {
			return batch, err
		}
		i += n
		payload := body[i : i+int(length)]
		i += int(length)
		if field == 2 {
			record, err := decodeRecord(payload)
			if err != nil {
				return batch, err
			}
			batch.Records = append(batch.Records, record)
		}
	}
	return batch, nil
}

func decodeRecord(body []byte) (Record, error) {
	record := Record{}
	i := 0
	for i < len(body) {
		key, n, err := readVarint(body[i:])
		if err != nil {
			return record, err
		}
		i += n
		field := int(key >> 3)
		wire := key & 7
		if wire == 0 {
			value, n, err := readVarint(body[i:])
			if err != nil {
				return record, err
			}
			i += n
			if field == 1 {
				record.RecordID = value
			}
			continue
		}
		length, n, err := readVarint(body[i:])
		if err != nil {
			return record, err
		}
		i += n
		payload := body[i : i+int(length)]
		i += int(length)
		if field == 2 {
			record.Payload = append([]byte(nil), payload...)
		}
	}
	return record, nil
}

func decodeError(body []byte) (uint32, string) {
	var code uint32
	var message string
	i := 0
	for i < len(body) {
		key, n, err := readVarint(body[i:])
		if err != nil {
			return code, message
		}
		i += n
		field := int(key >> 3)
		wire := key & 7
		if wire == 0 {
			value, n, err := readVarint(body[i:])
			if err != nil {
				return code, message
			}
			i += n
			if field == 1 {
				code = uint32(value)
			}
			continue
		}
		length, n, err := readVarint(body[i:])
		if err != nil {
			return code, message
		}
		i += n
		payload := body[i : i+int(length)]
		i += int(length)
		if field == 2 {
			message = string(payload)
		}
	}
	return code, message
}

func readVarint(buf []byte) (uint64, int, error) {
	var value uint64
	for i, b := range buf {
		if i > 9 {
			return 0, 0, fmt.Errorf("varint too long")
		}
		value |= uint64(b&0x7f) << (7 * i)
		if b < 0x80 {
			return value, i + 1, nil
		}
	}
	return 0, 0, fmt.Errorf("truncated varint")
}

// Timeout is the default bound for a short synthetic consume.
const Timeout = 30 * time.Second
