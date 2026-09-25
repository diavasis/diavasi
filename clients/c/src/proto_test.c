#include "proto.h"

#include <stdio.h>
#include <string.h>

static int fail(const char *message) {
    fprintf(stderr, "%s\n", message);
    return 1;
}

int main(void) {
    uint8_t hello[16];
    size_t n = diavasi_hello_frame(hello, sizeof hello);
    const uint8_t expect_hello[] = {0x08, 0x01, 0x12, 0x02, 0x08, 0x01};
    if (n != sizeof expect_hello || memcmp(hello, expect_hello, n) != 0) {
        return fail("hello frame");
    }
    uint8_t ack[16];
    n = diavasi_ack_frame(ack, sizeof ack, 1);
    const uint8_t expect_ack[] = {0x08, 0x01, 0x3a, 0x02, 0x08, 0x01};
    if (n != sizeof expect_ack || memcmp(ack, expect_ack, n) != 0) {
        return fail("ack frame");
    }
    uint8_t record[16];
    /* record id 7 payload ab, built as a batch via decode of a hand-made frame */
    const uint8_t batch_frame[] = {0x08, 0x01, 0x32, 0x0a, 0x08, 0x01, 0x12, 0x06, 0x08, 0x07, 0x12, 0x02, 0x61, 0x62};
    diavasi_frame_kind kind = DIAVASI_FRAME_OTHER;
    const uint8_t *body = NULL;
    size_t body_len = 0;
    if (diavasi_decode_envelope(batch_frame, sizeof batch_frame, &kind, &body, &body_len) != 0) {
        return fail("decode envelope");
    }
    if (kind != DIAVASI_FRAME_BATCH) {
        return fail("kind");
    }
    diavasi_batch_view batch;
    if (diavasi_decode_batch(body, body_len, &batch) != 0) {
        return fail("decode batch");
    }
    if (batch.batch_id != 1 || batch.record_count != 1 || batch.records[0].record_id != 7) {
        return fail("batch fields");
    }
    if (batch.records[0].payload_len != 2 || memcmp(batch.records[0].payload, "ab", 2) != 0) {
        return fail("payload");
    }
    (void)record;
    return 0;
}
