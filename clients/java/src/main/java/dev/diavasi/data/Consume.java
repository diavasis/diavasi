package dev.diavasi.data;

public final class Consume {
    private Consume() {}

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
