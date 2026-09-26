"""Convert the one-line label map to a splittable CSV without loading it in RAM.

The source has the shape {"target": {"transaction_id": "Yes|No", ...}}.
This is a structural conversion only: every ID/label pair is preserved.
"""

from __future__ import annotations

import argparse
import os
import re
from pathlib import Path


PAIR_PATTERN = re.compile(rb'"([0-9]+)"\s*:\s*"(Yes|No)"')
CHUNK_SIZE = 4 * 1024 * 1024
OVERLAP_SIZE = 256


def convert_labels(source: Path, destination: Path) -> tuple[int, int, int]:
    if not source.is_file():
        raise FileNotFoundError(f"Label source does not exist: {source}")

    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    total = yes_count = no_count = 0
    buffer = b""

    try:
        with source.open("rb") as input_stream, temporary.open("wb") as output_stream:
            output_stream.write(b"transaction_id,label\n")

            while chunk := input_stream.read(CHUNK_SIZE):
                buffer += chunk
                safe_end = max(0, len(buffer) - OVERLAP_SIZE)
                consumed_until = 0

                for match in PAIR_PATTERN.finditer(buffer):
                    if match.end() > safe_end:
                        break
                    transaction_id, label = match.groups()
                    output_stream.write(transaction_id + b"," + label + b"\n")
                    total += 1
                    yes_count += label == b"Yes"
                    no_count += label == b"No"
                    consumed_until = match.end()

                if consumed_until:
                    buffer = buffer[consumed_until:]
                elif len(buffer) > CHUNK_SIZE + OVERLAP_SIZE:
                    raise ValueError("Could not parse label JSON near a chunk boundary")

            for match in PAIR_PATTERN.finditer(buffer):
                transaction_id, label = match.groups()
                output_stream.write(transaction_id + b"," + label + b"\n")
                total += 1
                yes_count += label == b"Yes"
                no_count += label == b"No"

        if total == 0 or yes_count == 0 or total != yes_count + no_count:
            raise ValueError(
                f"Invalid conversion result: total={total}, yes={yes_count}, no={no_count}"
            )

        os.replace(temporary, destination)
        return total, yes_count, no_count
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    if (
        not args.force
        and args.output.is_file()
        and args.output.stat().st_mtime >= args.input.stat().st_mtime
    ):
        print(f"[labels] Up-to-date, skipping: {args.output}")
        return

    total, yes_count, no_count = convert_labels(args.input, args.output)
    print(
        f"[labels] Converted {total:,} labels: "
        f"Yes={yes_count:,}, No={no_count:,}, output={args.output}"
    )


if __name__ == "__main__":
    main()

