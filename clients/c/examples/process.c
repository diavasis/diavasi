#include "../include/diavasi.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const char *flag(int argc, char **argv, const char *name) {
    const char *found = NULL;
    for (int i = 1; i + 1 < argc; i++) {
        if (strcmp(argv[i], name) == 0) {
            found = argv[i + 1];
        }
    }
    return found;
}

static void on_record(uint64_t batch_id, uint64_t record_id, const uint8_t *payload, size_t payload_len, void *user) {
    (void)payload;
    (void)user;
    printf("batch %llu record %llu (%zu bytes)\n",
           (unsigned long long)batch_id,
           (unsigned long long)record_id,
           payload_len);
}

int main(int argc, char **argv) {
    diavasi_options options = {0};
    options.addr = flag(argc, argv, "--addr");
    options.ca_path = flag(argc, argv, "--ca");
    options.token = flag(argc, argv, "--token");
    options.group_id = flag(argc, argv, "--group");
    options.consumer_id = flag(argc, argv, "--consumer");
    if (options.addr == NULL) {
        options.addr = "127.0.0.1:7710";
    }
    if (options.ca_path == NULL) {
        options.ca_path = "/tmp/diavasi-sdk/dataplane-ca.crt";
    }
    if (options.token == NULL) {
        options.token = "sdk-demo";
    }
    if (options.group_id == NULL) {
        options.group_id = "demo";
    }
    if (options.consumer_id == NULL) {
        options.consumer_id = "c";
    }
    const char *total = flag(argc, argv, "--total");
    options.expect_records = total ? (uint64_t)strtoull(total, NULL, 10) : 8;
    options.max_in_flight = 1;
    options.on_record = on_record;

    diavasi_report report;
    char error[256] = {0};
    int rc = diavasi_consume(&options, &report, error, sizeof error);
    if (rc != 0) {
        if (rc >= 1 && rc <= 8) {
            fprintf(stderr, "protocol %d: %s\n", rc, error);
        } else {
            fprintf(stderr, "%s\n", error);
        }
        diavasi_report_free(&report);
        return rc > 0 ? rc : 1;
    }
    printf("record_ids");
    for (size_t i = 0; i < report.record_count; i++) {
        printf(" %llu", (unsigned long long)report.record_ids[i]);
    }
    printf("\n");
    diavasi_report_free(&report);
    return 0;
}
