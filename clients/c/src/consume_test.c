#include "../include/diavasi.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(void) {
    const char *addr = getenv("DIAVASI_DATA_ADDR");
    const char *ca = getenv("DIAVASI_CA");
    const char *token = getenv("DIAVASI_API_TOKEN");
    if (addr == NULL || ca == NULL || token == NULL) {
        return 0;
    }
    const char *group = getenv("DIAVASI_GROUP");
    if (group == NULL) {
        group = "sdk";
    }
    const char *total_env = getenv("DIAVASI_TOTAL");
    uint64_t total = total_env ? (uint64_t)strtoull(total_env, NULL, 10) : 8;
    diavasi_options options = {0};
    options.addr = addr;
    options.ca_path = ca;
    options.token = token;
    options.group_id = group;
    options.consumer_id = "c-test";
    options.max_in_flight = 1;
    options.expect_records = total;
    diavasi_report report;
    char error[256];
    int rc = diavasi_consume(&options, &report, error, sizeof error);
    if (rc != 0 || report.record_count != total) {
        fprintf(stderr, "consume failed rc=%d records=%zu %s\n", rc, report.record_count, error);
        diavasi_report_free(&report);
        return 1;
    }
    diavasi_report_free(&report);
    return 0;
}
