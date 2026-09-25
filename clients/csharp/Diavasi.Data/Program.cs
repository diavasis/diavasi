using Diavasi.Data;

var parsed = argsMap(args);
var halt = uint.TryParse(Get(parsed, "--halt-after"), out var parsedHalt) ? parsedHalt : 0u;
var total = ulong.TryParse(Get(parsed, "--total"), out var parsedTotal) ? parsedTotal : 0ul;
var max = uint.TryParse(Get(parsed, "--max-in-flight"), out var parsedMax) ? parsedMax : 1u;
try
{
    var recordIds = new List<ulong>();
    var batchIds = new List<ulong>();
    await foreach (var batch in DiavasiClient.BatchesAsync(new Options
    {
        Addr = Get(parsed, "--addr") ?? "",
        Ca = Get(parsed, "--ca") ?? "",
        Token = Get(parsed, "--token") ?? "",
        GroupId = Get(parsed, "--group") ?? "",
        ConsumerId = Get(parsed, "--consumer") ?? "csharp",
        MaxInFlight = max,
        HaltAfterAcks = halt,
        ExpectRecords = total,
    }))
    {
        foreach (var record in batch.Records)
        {
            Console.WriteLine($"batch {batch.BatchId} record {record.RecordId} ({record.Payload.Length} bytes)");
            recordIds.Add(record.RecordId);
        }
        batchIds.Add(batch.BatchId);
    }
    Console.WriteLine("record_ids " + string.Join(' ', recordIds));
    Console.WriteLine("batch_ids " + string.Join(' ', batchIds));
    Console.WriteLine($"csharp consumed {recordIds.Count} records in {batchIds.Count} batches");
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
