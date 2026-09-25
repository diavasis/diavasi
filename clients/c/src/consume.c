#include "../include/diavasi.h"
#include "proto.h"

#include <grpc/grpc.h>
#include <grpc/grpc_security.h>
#include <grpc/slice.h>
#include <grpc/status.h>
#include <grpc/byte_buffer.h>
#include <grpc/byte_buffer_reader.h>
#include <grpc/support/alloc.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void set_error(char *buf, size_t cap, const char *message) {
    if (buf == NULL || cap == 0) {
        return;
    }
    snprintf(buf, cap, "%s", message);
}

static int complete(grpc_completion_queue *cq, void *tag, grpc_byte_buffer **received) {
    gpr_timespec deadline = gpr_time_add(gpr_now(GPR_CLOCK_REALTIME), gpr_time_from_seconds(30, GPR_TIMESPAN));
    grpc_event ev = grpc_completion_queue_next(cq, deadline, NULL);
    if (ev.type != GRPC_OP_COMPLETE || ev.tag != tag) {
        return -1;
    }
    if (!ev.success) {
        return -1;
    }
    (void)received;
    return 0;
}

static int call_status(grpc_call *call, grpc_completion_queue *cq, grpc_status_code *status, char *detail, size_t detail_len) {
    grpc_metadata_array trailing;
    grpc_metadata_array_init(&trailing);
    grpc_slice details = grpc_empty_slice();
    grpc_op op;
    memset(&op, 0, sizeof op);
    op.op = GRPC_OP_RECV_STATUS_ON_CLIENT;
    op.data.recv_status_on_client.trailing_metadata = &trailing;
    op.data.recv_status_on_client.status = status;
    op.data.recv_status_on_client.status_details = &details;
    int rc = 0;
    if (grpc_call_start_batch(call, &op, 1, (void *)2, NULL) != GRPC_CALL_OK || complete(cq, (void *)2, NULL) != 0) {
        rc = -1;
    }
    if (detail != NULL && detail_len > 0) {
        size_t n = GRPC_SLICE_LENGTH(details);
        if (n >= detail_len) {
            n = detail_len - 1;
        }
        memcpy(detail, GRPC_SLICE_START_PTR(details), n);
        detail[n] = 0;
    }
    grpc_slice_unref(details);
    grpc_metadata_array_destroy(&trailing);
    return rc;
}

static void set_call_error(char *buf, size_t cap, grpc_status_code status, const char *detail) {
    if (status == GRPC_STATUS_UNAUTHENTICATED || detail == NULL || strstr(detail, "unauthorized") != NULL) {
        set_error(buf, cap, "grpc UNAUTHENTICATED: unauthorized");
        return;
    }
    snprintf(buf, cap, "grpc status %d: %s", (int)status, detail);
}

static grpc_byte_buffer *frame_buffer(const uint8_t *bytes, size_t len) {
    grpc_slice slice = grpc_slice_from_copied_buffer((const char *)bytes, len);
    grpc_byte_buffer *buffer = grpc_raw_byte_buffer_create(&slice, 1);
    grpc_slice_unref(slice);
    return buffer;
}

static int buffer_bytes(grpc_byte_buffer *buffer, uint8_t **out, size_t *out_len) {
    if (buffer == NULL) {
        *out = NULL;
        *out_len = 0;
        return 0;
    }
    grpc_byte_buffer_reader reader;
    if (!grpc_byte_buffer_reader_init(&reader, buffer)) {
        return -1;
    }
    grpc_slice slice = grpc_byte_buffer_reader_readall(&reader);
    grpc_byte_buffer_reader_destroy(&reader);
    *out_len = GRPC_SLICE_LENGTH(slice);
    *out = malloc(*out_len);
    if (*out == NULL) {
        grpc_slice_unref(slice);
        return -1;
    }
    memcpy(*out, GRPC_SLICE_START_PTR(slice), *out_len);
    grpc_slice_unref(slice);
    return 0;
}

static int push_record(diavasi_report *report, uint64_t id) {
    uint64_t *next = realloc(report->record_ids, (report->record_count + 1) * sizeof(uint64_t));
    if (next == NULL) {
        return -1;
    }
    report->record_ids = next;
    report->record_ids[report->record_count++] = id;
    return 0;
}

static int push_batch(diavasi_report *report, uint64_t id) {
    uint64_t *next = realloc(report->batch_ids, (report->batch_count + 1) * sizeof(uint64_t));
    if (next == NULL) {
        return -1;
    }
    report->batch_ids = next;
    report->batch_ids[report->batch_count++] = id;
    return 0;
}

int diavasi_consume(const diavasi_options *options, diavasi_report *report, char *error_buf, size_t error_buf_len) {
    memset(report, 0, sizeof *report);
    grpc_init();
    char *pem = NULL;
    FILE *file = fopen(options->ca_path, "rb");
    if (file == NULL) {
        set_error(error_buf, error_buf_len, "read ca");
        return -1;
    }
    fseek(file, 0, SEEK_END);
    long pem_len = ftell(file);
    fseek(file, 0, SEEK_SET);
    pem = malloc((size_t)pem_len + 1);
    if (pem == NULL || fread(pem, 1, (size_t)pem_len, file) != (size_t)pem_len) {
        fclose(file);
        free(pem);
        set_error(error_buf, error_buf_len, "read ca");
        return -1;
    }
    pem[pem_len] = 0;
    fclose(file);

    grpc_channel_credentials *creds = grpc_ssl_credentials_create(pem, NULL, NULL, NULL);
    free(pem);
    grpc_arg arg;
    memset(&arg, 0, sizeof arg);
    arg.type = GRPC_ARG_STRING;
    arg.key = GRPC_SSL_TARGET_NAME_OVERRIDE_ARG;
    arg.value.string = "localhost";
    grpc_channel_args args = {1, &arg};
    grpc_channel *channel = grpc_channel_create(options->addr, creds, &args);
    grpc_channel_credentials_release(creds);
    grpc_completion_queue *cq = grpc_completion_queue_create_for_next(NULL);
    grpc_call *call = grpc_channel_create_call(
        channel,
        NULL,
        GRPC_PROPAGATE_DEFAULTS,
        cq,
        grpc_slice_from_static_string("/diavasi.data.v1.DataPlane/Consume"),
        NULL,
        gpr_inf_future(GPR_CLOCK_REALTIME),
        NULL);

    char bearer[512];
    snprintf(bearer, sizeof bearer, "Bearer %s", options->token);
    grpc_metadata meta;
    memset(&meta, 0, sizeof meta);
    meta.key = grpc_slice_from_static_string("authorization");
    meta.value = grpc_slice_from_copied_string(bearer);
    grpc_metadata_array initial;
    grpc_metadata_array_init(&initial);

    uint8_t hello[32];
    size_t hello_len = diavasi_hello_frame(hello, sizeof hello);
    grpc_byte_buffer *send_buf = frame_buffer(hello, hello_len);
    grpc_byte_buffer *recv_buf = NULL;
    grpc_op ops[4];
    memset(ops, 0, sizeof ops);
    ops[0].op = GRPC_OP_SEND_INITIAL_METADATA;
    ops[0].data.send_initial_metadata.count = 1;
    ops[0].data.send_initial_metadata.metadata = &meta;
    ops[1].op = GRPC_OP_SEND_MESSAGE;
    ops[1].data.send_message.send_message = send_buf;
    ops[2].op = GRPC_OP_RECV_INITIAL_METADATA;
    ops[2].data.recv_initial_metadata.recv_initial_metadata = &initial;
    ops[3].op = GRPC_OP_RECV_MESSAGE;
    ops[3].data.recv_message.recv_message = &recv_buf;
    void *tag = (void *)1;
    int rc = 0;
    if (grpc_call_start_batch(call, ops, 4, tag, NULL) != GRPC_CALL_OK || complete(cq, tag, NULL) != 0) {
        set_error(error_buf, error_buf_len, "grpc UNAUTHENTICATED: unauthorized");
        rc = -1;
        goto done;
    }
    grpc_byte_buffer_destroy(send_buf);
    send_buf = NULL;

    int sent_flow = 0;
    uint32_t max_in_flight = options->max_in_flight == 0 ? 1 : options->max_in_flight;
    while (rc == 0) {
        uint8_t *bytes = NULL;
        size_t bytes_len = 0;
        if (buffer_bytes(recv_buf, &bytes, &bytes_len) != 0) {
            set_error(error_buf, error_buf_len, "read frame");
            rc = -1;
            break;
        }
        if (recv_buf) {
            grpc_byte_buffer_destroy(recv_buf);
            recv_buf = NULL;
        }
        if (bytes == NULL) {
            grpc_status_code status = GRPC_STATUS_OK;
            char detail[128] = {0};
            if (call_status(call, cq, &status, detail, sizeof detail) == 0 && status != GRPC_STATUS_OK) {
                set_call_error(error_buf, error_buf_len, status, detail);
                rc = -1;
            } else if (options->expect_records > 0 && report->record_count < options->expect_records) {
                set_error(error_buf, error_buf_len, "stream ended early");
                rc = -1;
            }
            free(bytes);
            break;
        }
        diavasi_frame_kind kind = DIAVASI_FRAME_OTHER;
        const uint8_t *body = NULL;
        size_t body_len = 0;
        if (diavasi_decode_envelope(bytes, bytes_len, &kind, &body, &body_len) != 0) {
            set_error(error_buf, error_buf_len, "bad frame");
            rc = -1;
            free(bytes);
            break;
        }
        uint8_t reply[1024];
        size_t reply_len = 0;
        int stop = 0;
        if (kind == DIAVASI_FRAME_HELLO_ACK) {
            reply_len = diavasi_join_frame(reply, sizeof reply, options->group_id, options->consumer_id);
        } else if (kind == DIAVASI_FRAME_JOINED && !sent_flow) {
            sent_flow = 1;
            reply_len = diavasi_flow_frame(reply, sizeof reply, max_in_flight);
        } else if (kind == DIAVASI_FRAME_BATCH) {
            diavasi_batch_view batch;
            if (diavasi_decode_batch(body, body_len, &batch) != 0) {
                set_error(error_buf, error_buf_len, "bad batch");
                rc = -1;
                free(bytes);
                break;
            }
            for (size_t i = 0; i < batch.record_count; i++) {
                if (push_record(report, batch.records[i].record_id) != 0) {
                    rc = -1;
                    break;
                }
                if (options->on_record) {
                    options->on_record(
                        batch.batch_id,
                        batch.records[i].record_id,
                        batch.records[i].payload,
                        batch.records[i].payload_len,
                        options->user);
                }
            }
            reply_len = diavasi_ack_frame(reply, sizeof reply, batch.batch_id);
            if (push_batch(report, batch.batch_id) != 0) {
                rc = -1;
            }
            if (options->halt_after_acks > 0 && report->batch_count >= options->halt_after_acks) {
                stop = 1;
            } else if (options->expect_records > 0 && report->record_count >= options->expect_records) {
                /* ack, then a following leave is sent after this message */
                stop = 2;
            }
        } else if (kind == DIAVASI_FRAME_HEARTBEAT) {
            reply_len = diavasi_heartbeat_frame(reply, sizeof reply);
        } else if (kind == DIAVASI_FRAME_ERROR) {
            uint32_t code = 0;
            char message[256];
            diavasi_decode_error(body, body_len, &code, message, sizeof message);
            snprintf(error_buf, error_buf_len, "protocol error %u: %s", code, message);
            rc = (int)code;
            free(bytes);
            break;
        }
        free(bytes);
        if (rc != 0) {
            break;
        }
        if (stop == 1) {
            if (reply_len > 0) {
                send_buf = frame_buffer(reply, reply_len);
                memset(ops, 0, sizeof ops);
                ops[0].op = GRPC_OP_SEND_MESSAGE;
                ops[0].data.send_message.send_message = send_buf;
                ops[1].op = GRPC_OP_RECV_MESSAGE;
                ops[1].data.recv_message.recv_message = &recv_buf;
                if (grpc_call_start_batch(call, ops, 2, tag, NULL) != GRPC_CALL_OK || complete(cq, tag, NULL) != 0) {
                    set_error(error_buf, error_buf_len, "send ack");
                    rc = -1;
                }
                grpc_byte_buffer_destroy(send_buf);
                send_buf = NULL;
                if (recv_buf) {
                    grpc_byte_buffer_destroy(recv_buf);
                    recv_buf = NULL;
                }
            }
            break;
        }
        recv_buf = NULL;
        memset(ops, 0, sizeof ops);
        int nops = 0;
        if (reply_len > 0) {
            send_buf = frame_buffer(reply, reply_len);
            ops[nops].op = GRPC_OP_SEND_MESSAGE;
            ops[nops].data.send_message.send_message = send_buf;
            nops++;
        }
        if (stop == 2) {
            if (nops > 0) {
                if (grpc_call_start_batch(call, ops, nops, tag, NULL) != GRPC_CALL_OK || complete(cq, tag, NULL) != 0) {
                    set_error(error_buf, error_buf_len, "send ack");
                    rc = -1;
                    break;
                }
                grpc_byte_buffer_destroy(send_buf);
                send_buf = NULL;
            }
            uint8_t leave[16];
            size_t leave_len = diavasi_leave_frame(leave, sizeof leave);
            send_buf = frame_buffer(leave, leave_len);
            memset(ops, 0, sizeof ops);
            ops[0].op = GRPC_OP_SEND_MESSAGE;
            ops[0].data.send_message.send_message = send_buf;
            ops[1].op = GRPC_OP_SEND_CLOSE_FROM_CLIENT;
            if (grpc_call_start_batch(call, ops, 2, tag, NULL) != GRPC_CALL_OK || complete(cq, tag, NULL) != 0) {
                set_error(error_buf, error_buf_len, "send leave");
                rc = -1;
            }
            grpc_byte_buffer_destroy(send_buf);
            send_buf = NULL;
            break;
        }
        ops[nops].op = GRPC_OP_RECV_MESSAGE;
        ops[nops].data.recv_message.recv_message = &recv_buf;
        nops++;
        if (grpc_call_start_batch(call, ops, nops, tag, NULL) != GRPC_CALL_OK || complete(cq, tag, NULL) != 0) {
            if (options->expect_records > 0 && report->record_count >= options->expect_records) {
                rc = 0;
            } else {
                set_error(error_buf, error_buf_len, "grpc call failed");
                rc = -1;
            }
            break;
        }
        if (send_buf) {
            grpc_byte_buffer_destroy(send_buf);
            send_buf = NULL;
        }
    }

done:
    if (send_buf) {
        grpc_byte_buffer_destroy(send_buf);
    }
    if (recv_buf) {
        grpc_byte_buffer_destroy(recv_buf);
    }
    grpc_metadata_array_destroy(&initial);
    grpc_slice_unref(meta.value);
    grpc_call_unref(call);
    grpc_completion_queue_shutdown(cq);
    grpc_completion_queue_destroy(cq);
    grpc_channel_destroy(channel);
    grpc_shutdown();
    return rc;
}

void diavasi_report_free(diavasi_report *report) {
    free(report->record_ids);
    free(report->batch_ids);
    memset(report, 0, sizeof *report);
}
