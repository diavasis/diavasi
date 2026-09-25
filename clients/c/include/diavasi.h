#ifndef DIAVASI_H
#define DIAVASI_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct diavasi_options {
    const char *addr;
    const char *ca_path;
    const char *token;
    const char *group_id;
    const char *consumer_id;
    uint32_t max_in_flight;
    uint32_t halt_after_acks;
    uint64_t expect_records;
} diavasi_options;

typedef struct diavasi_report {
    uint64_t *record_ids;
    size_t record_count;
    uint64_t *batch_ids;
    size_t batch_count;
} diavasi_report;

/* 0 on success. Protocol codes 1 through 8. -1 on a transport or auth failure.
   error_buf receives a message. The caller frees the report arrays with
   diavasi_report_free. */
int diavasi_consume(const diavasi_options *options, diavasi_report *report, char *error_buf, size_t error_buf_len);

void diavasi_report_free(diavasi_report *report);

#ifdef __cplusplus
}
#endif

#endif
