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
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/** Thin client for diavasi.data.v1. The caller acks by batch id. */
public final class DiavasiClient {
    private DiavasiClient() {}

    public static final class ProtocolException extends Exception {
        public final int code;

        public ProtocolException(int code, String message) {
            super("protocol error " + code + ": " + message);
            this.code = code;
        }
    }

    public static final class CallException extends Exception {
        public final String status;

        public CallException(String status, String message) {
            super("grpc " + status + ": " + message);
            this.status = status;
        }
    }

    public static final class Report {
        public final List<Long> recordIds = new ArrayList<>();
        public final List<Long> batchIds = new ArrayList<>();
    }

    public static final class Options {
        public String addr;
        public String ca;
        public String token;
        public String groupId;
        public String consumerId;
        public int maxInFlight = 1;
        public int haltAfterAcks;
        public long expectRecords;
    }

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
