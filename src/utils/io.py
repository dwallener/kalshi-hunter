from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd


def ensure_dir(path: str | Path) -> Path:
    target = Path(path)
    target.mkdir(parents=True, exist_ok=True)
    return target


def write_json(path: str | Path, payload: object) -> Path:
    target = Path(path)
    ensure_dir(target.parent)
    target.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return target


def write_jsonl(path: str | Path, rows: Iterable[dict]) -> Path:
    target = Path(path)
    ensure_dir(target.parent)
    with target.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, default=str))
            handle.write("\n")
    return target


def write_csv(path: str | Path, rows: Sequence[dict], columns: Sequence[str] | None = None) -> Path:
    target = Path(path)
    ensure_dir(target.parent)
    frame = pd.DataFrame(rows, columns=list(columns) if columns is not None else None)
    frame.to_csv(target, index=False)
    return target


def load_jsonl(path: str | Path) -> list[dict]:
    rows: list[dict] = []
    with Path(path).open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def list_jsonl_files(path: str | Path) -> list[Path]:
    return sorted(Path(path).glob("*.jsonl"))
