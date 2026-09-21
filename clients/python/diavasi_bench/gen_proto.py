"""Generate Python protobuf stubs from the shared bench.proto."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def main() -> int:
    root = Path(__file__).resolve().parents[3]
    proto = root / "crates" / "diavasi" / "proto" / "bench.proto"
    out = Path(__file__).resolve().parent
    cmd = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"-I{proto.parent}",
        f"--python_out={out}",
        str(proto),
    ]
    print(" ".join(cmd))
    subprocess.check_call(cmd)
    # Rename for package import.
    generated = out / "bench_pb2.py"
    if not generated.exists():
        # proto package path may nest
        candidates = list(out.rglob("bench_pb2.py"))
        if not candidates:
            raise SystemExit("bench_pb2.py not generated")
        generated = candidates[0]
    target = out / "bench_pb2.py"
    if generated != target:
        target.write_bytes(generated.read_bytes())
    print(f"wrote {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
