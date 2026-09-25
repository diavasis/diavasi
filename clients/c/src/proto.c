#include "proto.h"

#include <stdio.h>
#include <string.h>

static size_t put_varint(uint8_t *out, size_t cap, uint64_t value) {
    size_t n = 0;
    do {
        if (n >= cap) {
            return 0;
        }
        uint8_t byte = (uint8_t)(value & 0x7f);
        value >>= 7;
        if (value) {
            byte |= 0x80;
        }
        out[n++] = byte;
    } while (value);
    return n;
}

static size_t field_varint(uint8_t *out, size_t cap, int field, uint64_t value) {
    size_t n = put_varint(out, cap, ((uint64_t)field) << 3);
    if (n == 0) {
        return 0;
    }
    size_t m = put_varint(out + n, cap - n, value);
    if (m == 0) {
        return 0;
    }
    return n + m;
}

static size_t field_bytes(uint8_t *out, size_t cap, int field, const uint8_t *bytes, size_t len) {
    size_t n = put_varint(out, cap, (((uint64_t)field) << 3) | 2);
    if (n == 0) {
        return 0;
    }
    size_t m = put_varint(out + n, cap - n, len);
    if (m == 0 || n + m + len > cap) {
        return 0;
    }
    if (len > 0) {
        memcpy(out + n + m, bytes, len);
    }
    return n + m + len;
}

static size_t envelope(uint8_t *out, size_t cap, int field, const uint8_t *inner, size_t inner_len) {
    size_t n = field_varint(out, cap, 1, 1);
    if (n == 0) {
        return 0;
    }
    size_t m = field_bytes(out + n, cap - n, field, inner, inner_len);
    if (m == 0) {
        return 0;
    }
    return n + m;
}

size_t diavasi_hello_frame(uint8_t *out, size_t cap) {
    uint8_t inner[8];
    size_t n = field_varint(inner, sizeof inner, 1, 1);
    return envelope(out, cap, 2, inner, n);
}

size_t diavasi_join_frame(uint8_t *out, size_t cap, const char *group, const char *consumer) {
    uint8_t inner[512];
    size_t n = field_bytes(inner, sizeof inner, 1, (const uint8_t *)group, strlen(group));
    size_t m = field_bytes(inner + n, sizeof inner - n, 2, (const uint8_t *)consumer, strlen(consumer));
    return envelope(out, cap, 4, inner, n + m);
}

size_t diavasi_flow_frame(uint8_t *out, size_t cap, uint32_t max_in_flight) {
    uint8_t inner[16];
    size_t n = field_varint(inner, sizeof inner, 1, max_in_flight);
    return envelope(out, cap, 10, inner, n);
}

size_t diavasi_ack_frame(uint8_t *out, size_t cap, uint64_t batch_id) {
    uint8_t inner[16];
    size_t n = field_varint(inner, sizeof inner, 1, batch_id);
    return envelope(out, cap, 7, inner, n);
}

size_t diavasi_heartbeat_frame(uint8_t *out, size_t cap) {
    return envelope(out, cap, 9, NULL, 0);
}

size_t diavasi_leave_frame(uint8_t *out, size_t cap) {
    return envelope(out, cap, 12, NULL, 0);
}

static int read_varint(const uint8_t *buf, size_t len, uint64_t *value, size_t *used) {
    uint64_t out = 0;
    size_t i = 0;
    for (; i < len && i < 10; i++) {
        out |= (uint64_t)(buf[i] & 0x7f) << (7 * i);
        if ((buf[i] & 0x80) == 0) {
            *value = out;
            *used = i + 1;
            return 0;
        }
    }
    return -1;
}

int diavasi_decode_envelope(const uint8_t *frame, size_t len, diavasi_frame_kind *kind, const uint8_t **body, size_t *body_len) {
    size_t i = 0;
    *kind = DIAVASI_FRAME_OTHER;
    *body = NULL;
    *body_len = 0;
    while (i < len) {
        uint64_t key = 0;
        size_t used = 0;
        if (read_varint(frame + i, len - i, &key, &used) != 0) {
            return -1;
        }
        i += used;
        int field = (int)(key >> 3);
        int wire = (int)(key & 7);
        if (wire == 0) {
            uint64_t ignored = 0;
            if (read_varint(frame + i, len - i, &ignored, &used) != 0) {
                return -1;
            }
            i += used;
            continue;
        }
        if (wire != 2) {
            return -1;
        }
        uint64_t length = 0;
        if (read_varint(frame + i, len - i, &length, &used) != 0) {
            return -1;
        }
        i += used;
        if (i + length > len) {
            return -1;
        }
        const uint8_t *payload = frame + i;
        i += (size_t)length;
        switch (field) {
        case 3:
            *kind = DIAVASI_FRAME_HELLO_ACK;
            break;
        case 5:
            *kind = DIAVASI_FRAME_JOINED;
            break;
        case 6:
            *kind = DIAVASI_FRAME_BATCH;
            *body = payload;
            *body_len = (size_t)length;
            break;
        case 9:
            *kind = DIAVASI_FRAME_HEARTBEAT;
            break;
        case 11:
            *kind = DIAVASI_FRAME_ERROR;
            *body = payload;
            *body_len = (size_t)length;
            break;
        default:
            break;
        }
    }
    return 0;
}

int diavasi_decode_batch(const uint8_t *body, size_t len, diavasi_batch_view *batch) {
    memset(batch, 0, sizeof *batch);
    size_t i = 0;
    while (i < len) {
        uint64_t key = 0;
        size_t used = 0;
        if (read_varint(body + i, len - i, &key, &used) != 0) {
            return -1;
        }
        i += used;
        int field = (int)(key >> 3);
        int wire = (int)(key & 7);
        if (wire == 0) {
            uint64_t value = 0;
            if (read_varint(body + i, len - i, &value, &used) != 0) {
                return -1;
            }
            i += used;
            if (field == 1) {
                batch->batch_id = value;
            }
            continue;
        }
        uint64_t length = 0;
        if (read_varint(body + i, len - i, &length, &used) != 0) {
            return -1;
        }
        i += used;
        const uint8_t *payload = body + i;
        i += (size_t)length;
        if (field != 2 || batch->record_count >= 64) {
            continue;
        }
        diavasi_record_view *record = &batch->records[batch->record_count++];
        size_t j = 0;
        while (j < length) {
            uint64_t rkey = 0;
            if (read_varint(payload + j, (size_t)length - j, &rkey, &used) != 0) {
                return -1;
            }
            j += used;
            int rfield = (int)(rkey >> 3);
            int rwire = (int)(rkey & 7);
            if (rwire == 0) {
                uint64_t value = 0;
                if (read_varint(payload + j, (size_t)length - j, &value, &used) != 0) {
                    return -1;
                }
                j += used;
                if (rfield == 1) {
                    record->record_id = value;
                }
                continue;
            }
            uint64_t rlen = 0;
            if (read_varint(payload + j, (size_t)length - j, &rlen, &used) != 0) {
                return -1;
            }
            j += used;
            if (rfield == 2) {
                record->payload = payload + j;
                record->payload_len = (size_t)rlen;
            }
            j += (size_t)rlen;
        }
    }
    return 0;
}

int diavasi_decode_error(const uint8_t *body, size_t len, uint32_t *code, char *message, size_t message_cap) {
    *code = 0;
    if (message_cap) {
        message[0] = 0;
    }
    size_t i = 0;
    while (i < len) {
        uint64_t key = 0;
        size_t used = 0;
        if (read_varint(body + i, len - i, &key, &used) != 0) {
            return -1;
        }
        i += used;
        int field = (int)(key >> 3);
        int wire = (int)(key & 7);
        if (wire == 0) {
            uint64_t value = 0;
            if (read_varint(body + i, len - i, &value, &used) != 0) {
                return -1;
            }
            i += used;
            if (field == 1) {
                *code = (uint32_t)value;
            }
            continue;
        }
        uint64_t length = 0;
        if (read_varint(body + i, len - i, &length, &used) != 0) {
            return -1;
        }
        i += used;
        if (field == 2 && message_cap > 0) {
            size_t copy = (size_t)length;
            if (copy >= message_cap) {
                copy = message_cap - 1;
            }
            memcpy(message, body + i, copy);
            message[copy] = 0;
        }
        i += (size_t)length;
    }
    return 0;
}
