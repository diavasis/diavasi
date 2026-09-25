package dev.diavasi.data;

/**
 * Command-line consumer for {@code diavasi.data.v1}.
 *
 * <p>Joins one group, acks every batch, and prints {@code record_ids} and
 * {@code batch_ids}. A protocol error exits with that code. Any other failure
 * exits 1. An unknown flag exits 2. The last occurrence of a repeated flag wins.
 *
 * <p>Flags: {@code --addr}, {@code --ca}, {@code --token}, {@code --group},
 * {@code --consumer} (default {@code java}), {@code --total},
 * {@code --max-in-flight} (default 1), {@code --halt-after}.
 */
public final class Consume {
    private Consume() {}

    /**
     * Parses flags, consumes the group, and prints the report.
     *
     * @param args flag and value pairs, for example {@code --addr 127.0.0.1:7710}
     */
    public static void main(String[] args) {
        DiavasiClient.Options options = new DiavasiClient.Options();
        options.consumerId = "java";
        for (int i = 0; i < args.length; i += 2) {
            String value = args[i + 1];
            switch (args[i]) {
                case "--addr" -> options.addr = value;
                case "--ca" -> options.ca = value;
                case "--token" -> options.token = value;
                case "--group" -> options.groupId = value;
                case "--consumer" -> options.consumerId = value;
                case "--total" -> options.expectRecords = Long.parseLong(value);
                case "--max-in-flight" -> options.maxInFlight = Integer.parseInt(value);
                case "--halt-after" -> options.haltAfterAcks = Integer.parseInt(value);
                default -> {
                    System.err.println("unknown argument " + args[i]);
                    System.exit(2);
                }
            }
        }
        try {
            DiavasiClient.Report report = DiavasiClient.consume(options);
            StringBuilder records = new StringBuilder();
            for (Long id : report.recordIds) {
                if (records.length() > 0) {
                    records.append(' ');
                }
                records.append(id);
            }
            StringBuilder batches = new StringBuilder();
            for (Long id : report.batchIds) {
                if (batches.length() > 0) {
                    batches.append(' ');
                }
                batches.append(id);
            }
            System.out.println("record_ids " + records);
            System.out.println("batch_ids " + batches);
            System.out.println("java consumed " + report.recordIds.size() + " records in " + report.batchIds.size() + " batches");
        } catch (DiavasiClient.ProtocolException exc) {
            System.err.println(exc.getMessage());
            System.exit(exc.code >= 1 && exc.code <= 8 ? exc.code : 1);
        } catch (Exception exc) {
            System.err.println(exc.getMessage());
            System.exit(1);
        }
    }
}
