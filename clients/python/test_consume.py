"""Skip unless a data plane is configured in the environment."""

from __future__ import annotations

import os
import unittest

from diavasi_data import ProtocolError, Session


def _env() -> tuple[str, str, str] | None:
    addr = os.environ.get("DIAVASI_DATA_ADDR")
    ca = os.environ.get("DIAVASI_CA")
    token = os.environ.get("DIAVASI_API_TOKEN")
    if not addr or not ca or not token:
        return None
    return addr, ca, token


class ConsumeTest(unittest.TestCase):
    def test_consume_acks_every_batch(self) -> None:
        found = _env()
        if found is None:
            self.skipTest("DIAVASI_DATA_ADDR, DIAVASI_CA, and DIAVASI_API_TOKEN are unset")
        addr, ca, token = found
        group = os.environ.get("DIAVASI_GROUP", "sdk")
        total = int(os.environ.get("DIAVASI_TOTAL", "8"))
        session = Session(addr, ca, token, group, "python-test")
        records = 0
        try:
            session.connect()
            while records < total:
                batch = session.next_batch()
                self.assertIsNotNone(batch)
                assert batch is not None
                records += len(batch.records)
                session.ack(batch.batch_id)
            session.leave()
        finally:
            session.close()
        self.assertEqual(records, total)

    def test_missing_group_is_not_running(self) -> None:
        found = _env()
        if found is None:
            self.skipTest("DIAVASI_DATA_ADDR, DIAVASI_CA, and DIAVASI_API_TOKEN are unset")
        addr, ca, token = found
        session = Session(addr, ca, token, "sdk-missing", "python-missing")
        with self.assertRaises(ProtocolError) as raised:
            session.connect()
        self.assertEqual(raised.exception.code, 5)


if __name__ == "__main__":
    unittest.main()
