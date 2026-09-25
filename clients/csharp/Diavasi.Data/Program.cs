using Diavasi.Data;

var parsed = argsMap(args);
var halt = uint.TryParse(Get(parsed, "--halt-after"), out var parsedHalt) ? parsedHalt : 0u;
var total = ulong.TryParse(Get(parsed, "--total"), out var parsedTotal) ? parsedTotal : 0ul;
var max = uint.TryParse(Get(parsed, "--max-in-flight"), out var parsedMax) ? parsedMax : 1u;
try
{
    var report = await DiavasiClient.ConsumeAsync(new Options
    {
        Addr = Get(parsed, "--addr") ?? "",
        Ca = Get(parsed, "--ca") ?? "",
        Token = Get(parsed, "--token") ?? "",
        GroupId = Get(parsed, "--group") ?? "",
        ConsumerId = Get(parsed, "--consumer") ?? "csharp",
        MaxInFlight = max,
        HaltAfterAcks = halt,
        ExpectRecords = total,
    });
    Console.WriteLine("record_ids " + string.Join(' ', report.RecordIds));
    Console.WriteLine("batch_ids " + string.Join(' ', report.BatchIds));
    Console.WriteLine($"csharp consumed {report.RecordIds.Count} records in {report.BatchIds.Count} batches");
}
catch (ProtocolException exc)
{
    Console.Error.WriteLine(exc.Message);
    Environment.Exit(exc.Code is >= 1 and <= 8 ? (int)exc.Code : 1);
}
catch (Exception exc)
{
    Console.Error.WriteLine(exc.Message);
    Environment.Exit(1);
}

static Dictionary<string, string> argsMap(string[] argv)
{
    var map = new Dictionary<string, string>();
    for (var i = 0; i + 1 < argv.Length; i += 2)
    {
        map[argv[i]] = argv[i + 1];
    }
    return map;
}

static string? Get(Dictionary<string, string> map, string key) =>
    map.TryGetValue(key, out var value) ? value : null;
