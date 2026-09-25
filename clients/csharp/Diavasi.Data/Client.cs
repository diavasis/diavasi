using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using Diavasi.Data.V1;
using Grpc.Core;
using Grpc.Net.Client;

namespace Diavasi.Data;

public sealed class ProtocolException : Exception
{
    public uint Code { get; }

    public ProtocolException(uint code, string message) : base($"protocol error {code}: {message}")
    {
        Code = code;
    }
}

public sealed class CallException : Exception
{
    public string Status { get; }

    public CallException(string status, string message) : base($"grpc {status}: {message}")
    {
        Status = status;
    }
}

public sealed class Report
{
    public List<ulong> RecordIds { get; } = new();
    public List<ulong> BatchIds { get; } = new();
}

public sealed class Options
{
    public required string Addr { get; init; }
    public required string Ca { get; init; }
    public required string Token { get; init; }
    public required string GroupId { get; init; }
    public required string ConsumerId { get; init; }
    public uint MaxInFlight { get; init; } = 1;
    public uint HaltAfterAcks { get; init; }
    public ulong ExpectRecords { get; init; }
}

public static class DiavasiClient
{
    public static async Task<Report> ConsumeAsync(Options options, CancellationToken cancellation = default)
    {
        using var channel = GrpcChannel.ForAddress("https://" + options.Addr, new GrpcChannelOptions
        {
            HttpHandler = Handler(options.Ca),
        });
        var client = new DataPlane.DataPlaneClient(channel);
        var headers = new Metadata { { "authorization", "Bearer " + options.Token } };
        using var call = client.Consume(headers, cancellationToken: cancellation);
        await call.RequestStream.WriteAsync(new Envelope
        {
            Version = 1,
            Hello = new Hello { ProtocolVersion = 1 },
        }, cancellation);

        var report = new Report();
        var sentFlow = false;
        var maxInFlight = options.MaxInFlight == 0 ? 1u : options.MaxInFlight;
        try
        {
            while (await call.ResponseStream.MoveNext(cancellation))
            {
                var env = call.ResponseStream.Current;
                switch (env.BodyCase)
                {
                    case Envelope.BodyOneofCase.HelloAck:
                        await call.RequestStream.WriteAsync(new Envelope
                        {
                            Version = 1,
                            JoinGroup = new JoinGroup
                            {
                                GroupId = options.GroupId,
                                ConsumerId = options.ConsumerId,
                            },
                        }, cancellation);
                        break;
                    case Envelope.BodyOneofCase.Joined:
                        if (!sentFlow)
                        {
                            sentFlow = true;
                            await call.RequestStream.WriteAsync(new Envelope
                            {
                                Version = 1,
                                FlowControl = new FlowControl { MaxInFlight = maxInFlight },
                            }, cancellation);
                        }
                        break;
                    case Envelope.BodyOneofCase.RecordBatch:
                        var batch = env.RecordBatch;
                        foreach (var record in batch.Records)
                        {
                            report.RecordIds.Add(record.RecordId);
                        }
                        await call.RequestStream.WriteAsync(new Envelope
                        {
                            Version = 1,
                            Ack = new Ack { BatchId = batch.BatchId },
                        }, cancellation);
                        report.BatchIds.Add(batch.BatchId);
                        if (options.HaltAfterAcks > 0 && report.BatchIds.Count >= options.HaltAfterAcks)
                        {
                            try
                            {
                                using var extra = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                                await call.ResponseStream.MoveNext(extra.Token);
                            }
                            catch (OperationCanceledException)
                            {
                            }
                            return report;
                        }
                        if (options.ExpectRecords > 0 && (ulong)report.RecordIds.Count >= options.ExpectRecords)
                        {
                            await call.RequestStream.WriteAsync(new Envelope
                            {
                                Version = 1,
                                Leave = new Leave(),
                            }, cancellation);
                            await call.RequestStream.CompleteAsync();
                            return report;
                        }
                        break;
                    case Envelope.BodyOneofCase.Heartbeat:
                        await call.RequestStream.WriteAsync(new Envelope
                        {
                            Version = 1,
                            Heartbeat = new Heartbeat(),
                        }, cancellation);
                        break;
                    case Envelope.BodyOneofCase.Error:
                        throw new ProtocolException(env.Error.Code, env.Error.Message);
                    default:
                        break;
                }
            }
        }
        catch (RpcException) when (options.ExpectRecords > 0 && (ulong)report.RecordIds.Count >= options.ExpectRecords)
        {
            return report;
        }
        catch (RpcException exc)
        {
            throw new CallException(exc.StatusCode.ToString(), exc.Status.Detail);
        }

        if (options.ExpectRecords > 0 && (ulong)report.RecordIds.Count < options.ExpectRecords)
        {
            throw new CallException("Unavailable", "stream ended early");
        }
        return report;
    }

    private static SocketsHttpHandler Handler(string caPath)
    {
        var roots = new X509Certificate2Collection();
        roots.ImportFromPem(File.ReadAllText(caPath));
        return new SocketsHttpHandler
        {
            SslOptions = new SslClientAuthenticationOptions
            {
                TargetHost = "localhost",
                RemoteCertificateValidationCallback = (_, cert, _, _) =>
                {
                    if (cert is null)
                    {
                        return false;
                    }
                    using var chain = new X509Chain();
                    chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
                    chain.ChainPolicy.CustomTrustStore.AddRange(roots);
                    chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
                    return chain.Build(new X509Certificate2(cert));
                },
            },
        };
    }
}
