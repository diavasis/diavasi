package dev.diavasi.data;

import diavasi.data.v1.Data;
import diavasi.data.v1.DataPlaneGrpc;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.TlsChannelCredentials;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;

import java.io.File;
import java.util.ArrayList;
import java.util.function.Consumer;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Blocking client for {@code DataPlane.Consume}.
 *
 * <p>{@link #consume(Options)} opens TLS, sends {@code Hello} version 1, joins
 * the group, acks each batch, and returns the record ids and batch ids.
 * Payloads are not retained. The client stores no cursor.
 */
public final class DiavasiClient {
    private DiavasiClient() {}

    /**
     * An Envelope error frame. {@code code} is 1 through 8: bad version, bad
     * state, unknown ack, duplicate ack, group not running, unsupported,
     * internal, heartbeat timeout.
     */
    public static final class ProtocolException extends Exception {
        /** Protocol error code from the Envelope. */
        public final int code;

        /**
         * @param code protocol code, 1 through 8
         * @param message server message, included in {@link #getMessage()}
         */
        public ProtocolException(int code, String message) {
            super("protocol error " + code + ": " + message);
            this.code = code;
        }
    }

    /**
     * A gRPC status from the data plane, including a bad token
     * ({@code UNAUTHENTICATED} / {@code unauthorized}).
     */
    public static final class CallException extends Exception {
        /** gRPC status name, for example {@code UNAUTHENTICATED}. */
        public final String status;

        /**
         * @param status gRPC status name
         * @param message status detail, included in {@link #getMessage()}
         */
        public CallException(String status, String message) {
            super("grpc " + status + ": " + message);
            this.status = status;
        }
    }

    /** Record ids and batch ids acked by one {@link #consume(Options)} call. */
    public static final class Report {
        /** Record ids in delivery order. {@code record_id} can be 0. */
        public final List<Long> recordIds = new ArrayList<>();
        /** Batch ids in ack order. */
        public final List<Long> batchIds = new ArrayList<>();
    }

    /**
     * Where to connect and how far to read.
     *
     * <p>{@code maxInFlight} defaults to 1. {@code haltAfterAcks} of 0 is ignored.
     * A positive value closes after that many acks and does not send Leave.
     * {@code expectRecords} of 0 reads until the stream ends. A positive value
     * sends Leave once that many records are acked.
     */
    public static final class Options {
        /** Data-plane address, {@code host:port}, without a scheme. */
        public String addr;
        /** PEM file for the data-plane CA. The TLS name is {@code localhost}. */
        public String ca;
        /** Bearer token. A bad token raises {@link CallException}. */
        public String token;
        /** Consumer group to join. */
        public String groupId;
        /** Consumer id. Reconnect with the same id to replay unacked batches. */
        public String consumerId;
        /** Batches the server may have in flight. 0 is treated as 1. */
        public int maxInFlight = 1;
        /** Stop after this many acks without Leave. 0 disables the limit. */
        public int haltAfterAcks;
        /** Send Leave after this many records. 0 reads until the stream ends. */
        public long expectRecords;
    }

    /**
     * Joins {@code options.groupId}, acks each batch, and returns the ids.
     *
     * @param options data-plane address, credentials, group, and stop condition
     * @return record ids and batch ids that were acked
     * @throws ProtocolException the server sent an Envelope error frame
     * @throws CallException the stream failed, including a bad token or an early end
     * @throws Exception the CA file could not be read
     */
    public static Report consume(Options options) throws Exception {
        ChannelCredentials creds = TlsChannelCredentials.newBuilder()
                .trustManager(new File(options.ca))
                .build();
        ManagedChannel channel = Grpc.newChannelBuilder(options.addr, creds)
                .overrideAuthority("localhost")
                .build();
        try {
            Metadata headers = new Metadata();
            headers.put(
                    Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER),
                    "Bearer " + options.token);
            DataPlaneGrpc.DataPlaneStub stub = DataPlaneGrpc.newStub(channel)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
            ArrayBlockingQueue<Object> inbound = new ArrayBlockingQueue<>(64);
            StreamObserver<Data.Envelope> responses = new StreamObserver<>() {
                @Override
                public void onNext(Data.Envelope value) {
                    inbound.offer(value);
                }

                @Override
                public void onError(Throwable t) {
                    inbound.offer(t);
                }

                @Override
                public void onCompleted() {
                    inbound.offer(DONE);
                }
            };
            StreamObserver<Data.Envelope> requests = stub.consume(responses);
            requests.onNext(Data.Envelope.newBuilder()
                    .setVersion(1)
                    .setHello(Data.Hello.newBuilder().setProtocolVersion(1))
                    .build());
            Report report = new Report();
            boolean sentFlow = false;
            while (true) {
                Object event = inbound.poll(30, TimeUnit.SECONDS);
                if (event == null) {
                    throw new CallException("DEADLINE_EXCEEDED", "timed out waiting for a frame");
                }
                if (event == DONE) {
                    if (options.expectRecords > 0 && report.recordIds.size() < options.expectRecords) {
                        throw new CallException("UNAVAILABLE", "stream ended early");
                    }
                    return report;
                }
                if (event instanceof Throwable thrown) {
                    if (options.expectRecords > 0 && report.recordIds.size() >= options.expectRecords) {
                        return report;
                    }
                    if (thrown instanceof StatusRuntimeException status) {
                        throw new CallException(status.getStatus().getCode().name(), status.getStatus().getDescription());
                    }
                    throw new CallException(Status.UNKNOWN.getCode().name(), thrown.getMessage());
                }
                Data.Envelope env = (Data.Envelope) event;
                switch (env.getBodyCase()) {
                    case HELLO_ACK -> requests.onNext(Data.Envelope.newBuilder()
                            .setVersion(1)
                            .setJoinGroup(Data.JoinGroup.newBuilder()
                                    .setGroupId(options.groupId)
                                    .setConsumerId(options.consumerId))
                            .build());
                    case JOINED -> {
                        if (!sentFlow) {
                            sentFlow = true;
                            requests.onNext(Data.Envelope.newBuilder()
                                    .setVersion(1)
                                    .setFlowControl(Data.FlowControl.newBuilder()
                                            .setMaxInFlight(options.maxInFlight))
                                    .build());
                        }
                    }
                    case RECORD_BATCH -> {
                        Data.RecordBatch batch = env.getRecordBatch();
                        for (Data.Record record : batch.getRecordsList()) {
                            report.recordIds.add(record.getRecordId());
                        }
                        requests.onNext(Data.Envelope.newBuilder()
                                .setVersion(1)
                                .setAck(Data.Ack.newBuilder().setBatchId(batch.getBatchId()))
                                .build());
                        report.batchIds.add(batch.getBatchId());
                        if (options.haltAfterAcks > 0 && report.batchIds.size() >= options.haltAfterAcks) {
                            inbound.poll(5, TimeUnit.SECONDS);
                            return report;
                        }
                        if (options.expectRecords > 0 && report.recordIds.size() >= options.expectRecords) {
                            requests.onNext(Data.Envelope.newBuilder()
                                    .setVersion(1)
                                    .setLeave(Data.Leave.newBuilder())
                                    .build());
                            requests.onCompleted();
                        }
                    }
                    case HEARTBEAT -> requests.onNext(Data.Envelope.newBuilder()
                            .setVersion(1)
                            .setHeartbeat(Data.Heartbeat.newBuilder())
                            .build());
                    case ERROR -> throw new ProtocolException(env.getError().getCode(), env.getError().getMessage());
                    default -> {
                    }
                }
            }
        } finally {
            channel.shutdownNow();
        }
    }

    private static final Object DONE = new Object();
}
