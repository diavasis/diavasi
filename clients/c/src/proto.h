#ifndef DIAVASI_PROTO_H
#define DIAVASI_PROTO_H

#include <stddef.h>
#include <stdint.h>

size_t diavasi_hello_frame(uint8_t *out, size_t cap);
size_t diavasi_join_frame(uint8_t *out, size_t cap, const char *group, const char *consumer);
size_t diavasi_flow_frame(uint8_t *out, size_t cap, uint32_t max_in_flight);
size_t diavasi_ack_frame(uint8_t *out, size_t cap, uint64_t batch_id);
size_t diavasi_heartbeat_frame(uint8_t *out, size_t cap);
size_t diavasi_leave_frame(uint8_t *out, size_t cap);

typedef enum diavasi_frame_kind {
    DIAVASI_FRAME_OTHER = 0,
    DIAVASI_FRAME_HELLO_ACK = 1,
    DIAVASI_FRAME_JOINED = 2,
    DIAVASI_FRAME_BATCH = 3,
    DIAVASI_FRAME_HEARTBEAT = 4,
    DIAVASI_FRAME_ERROR = 5
} diavasi_frame_kind;

typedef struct diavasi_record_view {
    uint64_t record_id;
    const uint8_t *payload;
    size_t payload_len;
} diavasi_record_view;

typedef struct diavasi_batch_view {
    uint64_t batch_id;
    diavasi_record_view records[64];
    size_t record_count;
} diavasi_batch_view;

int diavasi_decode_envelope(const uint8_t *frame, size_t len, diavasi_frame_kind *kind, const uint8_t **body, size_t *body_len);
int diavasi_decode_batch(const uint8_t *body, size_t len, diavasi_batch_view *batch);
int diavasi_decode_error(const uint8_t *body, size_t len, uint32_t *code, char *message, size_t message_cap);

#endif
