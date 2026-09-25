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

int main(int argc, char **argv) {
    diavasi_options options = {0};
    options.addr = flag(argc, argv, "--addr");
    options.ca_path = flag(argc, argv, "--ca");
    options.token = flag(argc, argv, "--token");
    options.group_id = flag(argc, argv, "--group");
    options.consumer_id = flag(argc, argv, "--consumer");
    if (options.consumer_id == NULL) {
        options.consumer_id = "c";
    }
    const char *total = flag(argc, argv, "--total");
    const char *halt = flag(argc, argv, "--halt-after");
    const char *max_in_flight = flag(argc, argv, "--max-in-flight");
    options.expect_records = total ? (uint64_t)strtoull(total, NULL, 10) : 0;
    options.halt_after_acks = halt ? (uint32_t)strtoul(halt, NULL, 10) : 0;
    options.max_in_flight = max_in_flight ? (uint32_t)strtoul(max_in_flight, NULL, 10) : 1;

    diavasi_report report;
    char error[256];
    int rc = diavasi_consume(&options, &report, error, sizeof error);
    if (rc != 0) {
        fprintf(stderr, "%s\n", error);
        diavasi_report_free(&report);
        return rc > 0 ? rc : 1;
    }
    printf("record_ids");
    for (size_t i = 0; i < report.record_count; i++) {
        printf(" %llu", (unsigned long long)report.record_ids[i]);
    }
    printf("\nbatch_ids");
    for (size_t i = 0; i < report.batch_count; i++) {
        printf(" %llu", (unsigned long long)report.batch_ids[i]);
    }
    printf("\nc consumed %zu records in %zu batches\n", report.record_count, report.batch_count);
    diavasi_report_free(&report);
    return 0;
}
