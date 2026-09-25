using Diavasi.Data;

public class ConsumeTests
{
    [Fact]
    public async Task ConsumeAcksEveryBatch()
    {
        var addr = Environment.GetEnvironmentVariable("DIAVASI_DATA_ADDR");
        var ca = Environment.GetEnvironmentVariable("DIAVASI_CA");
        var token = Environment.GetEnvironmentVariable("DIAVASI_API_TOKEN");
        if (string.IsNullOrEmpty(addr) || string.IsNullOrEmpty(ca) || string.IsNullOrEmpty(token))
        {
            return;
        }
        var total = ulong.Parse(Environment.GetEnvironmentVariable("DIAVASI_TOTAL") ?? "8");
        var report = await DiavasiClient.ConsumeAsync(new Options
        {
            Addr = addr,
            Ca = ca,
            Token = token,
            GroupId = Environment.GetEnvironmentVariable("DIAVASI_GROUP") ?? "sdk",
            ConsumerId = "csharp-test",
            ExpectRecords = total,
        });
        Assert.Equal(total, (ulong)report.RecordIds.Count);
    }

    [Fact]
    public async Task MissingGroupIsNotRunning()
    {
        var addr = Environment.GetEnvironmentVariable("DIAVASI_DATA_ADDR");
        var ca = Environment.GetEnvironmentVariable("DIAVASI_CA");
        var token = Environment.GetEnvironmentVariable("DIAVASI_API_TOKEN");
        if (string.IsNullOrEmpty(addr) || string.IsNullOrEmpty(ca) || string.IsNullOrEmpty(token))
        {
            return;
        }
        var error = await Assert.ThrowsAsync<ProtocolException>(() => DiavasiClient.ConsumeAsync(new Options
        {
            Addr = addr,
            Ca = ca,
            Token = token,
            GroupId = "sdk-missing",
            ConsumerId = "csharp-missing",
        }));
        Assert.Equal(5u, error.Code);
    }
}
