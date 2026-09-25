"""Connect, read each batch, print each record, and report protocol or gRPC errors."""

from __future__ import annotations

import sys

from diavasi_data import CallError, ProtocolError, consume


def main() -> int:
    try:
        for batch in consume(
            addr="127.0.0.1:7710",
            ca="/tmp/diavasi-sdk/dataplane-ca.crt",
            token="sdk-demo",
            group_id="demo",
            consumer_id="python",
            expect_records=8,
        ):
            for record in batch.records:
                print(
                    f"batch {batch.batch_id} record {record.record_id} ({len(record.payload)} bytes)"
                )
    except ProtocolError as err:
        print(f"protocol {err.code}: {err.message}", file=sys.stderr)
        return err.code if 1 <= err.code <= 8 else 1
    except CallError as err:
        print(f"grpc {err.status}: {err.message}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
