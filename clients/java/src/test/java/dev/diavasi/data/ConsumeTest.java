package dev.diavasi.data;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assumptions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConsumeTest {
    @Test
    void consumeAcksEveryBatch() throws Exception {
        String addr = System.getenv("DIAVASI_DATA_ADDR");
        String ca = System.getenv("DIAVASI_CA");
        String token = System.getenv("DIAVASI_API_TOKEN");
        Assumptions.assumeTrue(addr != null && ca != null && token != null);
        String group = System.getenv().getOrDefault("DIAVASI_GROUP", "sdk");
        long total = Long.parseLong(System.getenv().getOrDefault("DIAVASI_TOTAL", "8"));
        DiavasiClient.Options options = new DiavasiClient.Options();
        options.addr = addr;
        options.ca = ca;
        options.token = token;
        options.groupId = group;
        options.consumerId = "java-test";
        options.expectRecords = total;
        DiavasiClient.Report report = DiavasiClient.consume(options);
        assertEquals(total, report.recordIds.size());
    }

    @Test
    void missingGroupIsNotRunning() {
        String addr = System.getenv("DIAVASI_DATA_ADDR");
        String ca = System.getenv("DIAVASI_CA");
        String token = System.getenv("DIAVASI_API_TOKEN");
        Assumptions.assumeTrue(addr != null && ca != null && token != null);
        DiavasiClient.Options options = new DiavasiClient.Options();
        options.addr = addr;
        options.ca = ca;
        options.token = token;
        options.groupId = "sdk-missing";
        options.consumerId = "java-missing";
        DiavasiClient.ProtocolException error = assertThrows(
                DiavasiClient.ProtocolException.class, () -> DiavasiClient.consume(options));
        assertEquals(5, error.code);
    }
}
