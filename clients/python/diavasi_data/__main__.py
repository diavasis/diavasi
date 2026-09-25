"""Example: consume a group and print batch ids."""

from __future__ import annotations

import argparse
import queue
import sys

from diavasi_data.client import CallError, ProtocolError, Session


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--addr", required=True)
    parser.add_argument("--ca", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--group", required=True)
    parser.add_argument("--consumer", default="python")
    parser.add_argument("--total", type=int, required=True)
    parser.add_argument("--max-in-flight", type=int, default=1)
    parser.add_argument("--halt-after", type=int, default=0)
    args = parser.parse_args()

    session = Session(
        addr=args.addr,
        ca=args.ca,
        token=args.token,
        group_id=args.group,
        consumer_id=args.consumer,
        max_in_flight=args.max_in_flight,
    )
    record_ids: list[int] = []
    batch_ids: list[int] = []
    try:
        session.connect()
        while len(record_ids) < args.total:
            batch = session.next_batch()
            if batch is None:
                break
            record_ids.extend(record.record_id for record in batch.records)
            session.ack(batch.batch_id)
            batch_ids.append(batch.batch_id)
            if args.halt_after and len(batch_ids) >= args.halt_after:
                try:
                    session.next_batch(timeout=5)
                except queue.Empty:
                    pass
                break
        else:
            session.leave()
        if args.halt_after and len(batch_ids) >= args.halt_after:
            pass
        elif len(record_ids) < args.total:
            sys.stderr.write(
                f"incomplete consume records={len(record_ids)} batches={len(batch_ids)}\n"
            )
            return 1
    except ProtocolError as exc:
        sys.stderr.write(f"{exc}\n")
        return exc.code if 1 <= exc.code <= 8 else 1
    except CallError as exc:
        sys.stderr.write(f"{exc}\n")
        return 1
    finally:
        session.close()

    print("record_ids " + " ".join(str(record_id) for record_id in record_ids))
    print("batch_ids " + " ".join(str(batch_id) for batch_id in batch_ids))
    print(f"python consumed {len(record_ids)} records in {len(batch_ids)} batches")
    return 0


if __name__ == "__main__":
    sys.exit(main())
