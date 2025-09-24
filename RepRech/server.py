import json
import math
import os
import sqlite3
import time
import unicodedata
from typing import Iterable, List, Sequence, Tuple

from flask import Flask, abort, jsonify, request, send_from_directory

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DB_PATH = os.path.join(BASE_DIR, "data.db")
RESULT_PATH = os.path.join(BASE_DIR, "result.rpt")

COLUMNS: Tuple[str, ...] = (
    "EmpRUC",
    "EmpRazonSocial",
    "EmpNom",
    "RepFecha",
    "RepLiqEstadoConsulta",
    "RepDetalleRechazo",
)
FALLBACK_POSITIONS: Tuple[int, ...] = (0, 12, 124, 165, 189, 209)
IGNORED_DETAIL_PREFIXES_RAW: Tuple[str, ...] = (
    "En la fecha de resumen del reporte la condici\u00f3n de emisor electr\u00f3nico no estaba vigente",
)

app = Flask(__name__, static_folder=".", static_url_path="")
app.config["JSON_AS_ASCII"] = False


def normalize_detail(text: str) -> str:
    if not text:
        return ""
    normalized = unicodedata.normalize("NFD", text)
    stripped = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
    return stripped.lower().strip()


def fix_text(value: str | None) -> str:
    if not value:
        return ""
    if "?" in value or "?" in value or "?" in value:
        try:
            return value.encode("latin-1").decode("utf-8")
        except UnicodeDecodeError:
            return value
    return value


IGNORED_DETAIL_PREFIXES: Tuple[str, ...] = tuple(
    normalize_detail(text) for text in IGNORED_DETAIL_PREFIXES_RAW
)


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_database() -> None:
    conn = get_connection()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS rows (
                id TEXT PRIMARY KEY,
                EmpRUC TEXT,
                EmpRazonSocial TEXT,
                EmpNom TEXT,
                RepFecha TEXT,
                RepFechaDate TEXT,
                RepLiqEstadoConsulta TEXT,
                RepDetalleRechazo TEXT,
                detail_normalized TEXT,
                resolved INTEGER NOT NULL DEFAULT 0,
                sources TEXT,
                created_at INTEGER NOT NULL
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def maybe_load_initial_data() -> None:
    if not os.path.exists(RESULT_PATH):
        return

    conn = get_connection()
    try:
        has_rows = conn.execute("SELECT 1 FROM rows LIMIT 1").fetchone() is not None
    finally:
        conn.close()

    if has_rows:
        return

    try:
        with open(RESULT_PATH, "r", encoding="utf-8", errors="ignore") as handler:
            content = handler.read()
    except OSError:
        return

    records = parse_rpt(content)
    if records:
        insert_records(records, "result.rpt")


def parse_rpt(content: str) -> List[dict]:
    if not content:
        return []

    records: List[dict] = []
    lines = content.splitlines()
    column_positions: Sequence[int] | None = None

    for raw_line in lines:
        sanitized_line = raw_line.replace("\x00", "")
        trimmed = sanitized_line.strip()

        if not trimmed:
            continue

        if trimmed.startswith("EmpRUC"):
            column_positions = detect_column_positions(sanitized_line)
            continue

        if trimmed.startswith("---"):
            continue

        if column_positions is None:
            column_positions = FALLBACK_POSITIONS

        record = parse_fixed_width_line(sanitized_line, column_positions)
        if record:
            records.append(record)

    return records


def detect_column_positions(header_line: str) -> Sequence[int]:
    positions: List[int] = []
    for label in COLUMNS:
        index = header_line.find(label)
        if index < 0:
            return FALLBACK_POSITIONS
        positions.append(index)
    return positions


def parse_fixed_width_line(line: str, positions: Sequence[int]) -> dict | None:
    values: List[str] = []
    for idx, start in enumerate(positions):
        end = positions[idx + 1] if idx + 1 < len(positions) else len(line)
        if start >= len(line):
            slice_value = ""
        else:
            slice_value = line[start:end]
        values.append(fix_text(slice_value.strip()))

    if not values or not values[0]:
        return None

    return {column: values[idx] if idx < len(values) else "" for idx, column in enumerate(COLUMNS)}


def build_key(record: dict) -> str:
    return "||".join((record.get(column, "") or "").strip() for column in COLUMNS)


def chunked(sequence: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    for index in range(0, len(sequence), size):
        yield sequence[index : index + size]


def extract_date(rep_fecha: str | None) -> str | None:
    if not rep_fecha:
        return None
    candidate = rep_fecha.strip()[:10]
    if len(candidate) != 10:
        return None
    try:
        year, month, day = candidate.split("-")
    except ValueError:
        return None
    if len(year) != 4:
        return None
    return candidate


def insert_records(records: Sequence[dict], source: str) -> Tuple[int, int, int]:
    if not records:
        return 0, 0, 0

    prepared: List[tuple] = []
    keys: List[str] = []
    ignored = 0

    for record in records:
        detail = fix_text(record.get("RepDetalleRechazo", ""))
        record = {column: fix_text(record.get(column, "")) for column in COLUMNS}
        record["RepDetalleRechazo"] = detail

        detail_normalized = normalize_detail(detail)
        if any(detail_normalized.startswith(prefix) for prefix in IGNORED_DETAIL_PREFIXES):
            ignored += 1
            continue

        key = build_key(record)
        if not key:
            continue

        prepared.append((record, key, detail_normalized))
        keys.append(key)

    if not prepared:
        return 0, 0, ignored

    conn = get_connection()
    added = 0
    duplicates = 0

    try:
        existing: dict[str, sqlite3.Row] = {}
        for chunk in chunked(keys, 500):
            placeholders = ",".join("?" for _ in chunk)
            query = f"SELECT id, sources FROM rows WHERE id IN ({placeholders})"
            for row in conn.execute(query, chunk):
                existing[row["id"]] = row

        now = int(time.time() * 1000)
        inserts: List[tuple] = []

        for index, (record, key, detail_normalized) in enumerate(prepared):
            if key in existing:
                duplicates += 1
                sources_raw = existing[key]["sources"] if existing[key] else None
                sources: List[str] = json.loads(sources_raw) if sources_raw else []
                if source not in sources:
                    sources.append(source)
                    conn.execute(
                        "UPDATE rows SET sources = ? WHERE id = ?",
                        (json.dumps(sources, ensure_ascii=False), key),
                    )
                continue

            rep_date = extract_date(record.get("RepFecha"))
            sources = json.dumps([source], ensure_ascii=False)
            inserts.append(
                (
                    key,
                    record.get("EmpRUC", ""),
                    record.get("EmpRazonSocial", ""),
                    record.get("EmpNom", ""),
                    record.get("RepFecha", ""),
                    rep_date,
                    record.get("RepLiqEstadoConsulta", ""),
                    record.get("RepDetalleRechazo", ""),
                    detail_normalized,
                    sources,
                    now + index,
                )
            )
            added += 1

        if inserts:
            conn.executemany(
                """
                INSERT INTO rows (
                    id,
                    EmpRUC,
                    EmpRazonSocial,
                    EmpNom,
                    RepFecha,
                    RepFechaDate,
                    RepLiqEstadoConsulta,
                    RepDetalleRechazo,
                    detail_normalized,
                    sources,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                inserts,
            )
        conn.commit()
    finally:
        conn.close()

    return added, duplicates, ignored


@app.route("/")
def root() -> "str":
    return send_from_directory(BASE_DIR, "index.html")


@app.route("/api/rows", methods=["GET"])
def api_rows():
    params: List[str] = []
    filters: List[str] = []

    from_date = request.args.get("from")
    to_date = request.args.get("to")
    detail = request.args.get("detail", "").strip()
    page_param = request.args.get("page")
    page_size_param = request.args.get("page_size")

    if from_date:
        filters.append("RepFechaDate >= ?")
        params.append(from_date)
    if to_date:
        filters.append("RepFechaDate <= ?")
        params.append(to_date)
    if detail:
        filters.append("detail_normalized LIKE ?")
        params.append(f"%{normalize_detail(detail)}%")

    where_clause = f" WHERE {' AND '.join(filters)}" if filters else ""

    try:
        page = int(page_param) if page_param is not None else 1
    except ValueError:
        page = 1
    page = max(1, page)

    try:
        page_size = int(page_size_param) if page_size_param is not None else 20
    except ValueError:
        page_size = 20
    page_size = max(1, min(page_size, 200))

    conn = get_connection()
    try:
        total_query = f"SELECT COUNT(*) FROM rows{where_clause}"
        total = conn.execute(total_query, params).fetchone()[0]
        total_pages = max(1, math.ceil(total / page_size)) if total else 1
        if page > total_pages:
            page = total_pages

        offset = (page - 1) * page_size

        rows_query = (
            "SELECT id, EmpRUC, EmpRazonSocial, EmpNom, RepFecha, RepLiqEstadoConsulta, "
            "RepDetalleRechazo, resolved, sources, RepFechaDate FROM rows"
            f"{where_clause} ORDER BY created_at, id LIMIT ? OFFSET ?"
        )
        query_params = list(params) + [page_size, offset]
        rows = conn.execute(rows_query, query_params).fetchall()

        count_all = conn.execute("SELECT COUNT(*) FROM rows").fetchone()[0]
        resolved_count = conn.execute("SELECT COUNT(*) FROM rows WHERE resolved = 1").fetchone()[0]
        min_max = conn.execute(
            "SELECT MIN(RepFechaDate), MAX(RepFechaDate) FROM rows "
            "WHERE RepFechaDate IS NOT NULL AND RepFechaDate != ''"
        ).fetchone()

        bounds = None
        if min_max and (min_max[0] or min_max[1]):
            bounds = {"min": min_max[0], "max": min_max[1]}

        payload = {
            "rows": [
                {
                    "id": row["id"],
                    "EmpRUC": fix_text(row["EmpRUC"]),
                    "EmpRazonSocial": fix_text(row["EmpRazonSocial"]),
                    "EmpNom": fix_text(row["EmpNom"]),
                    "RepFecha": fix_text(row["RepFecha"]),
                    "RepLiqEstadoConsulta": fix_text(row["RepLiqEstadoConsulta"]),
                    "RepDetalleRechazo": fix_text(row["RepDetalleRechazo"]),
                    "resolved": bool(row["resolved"]),
                    "sources": [fix_text(source) for source in (json.loads(row["sources"]) if row["sources"] else [])],
                    "rep_date": row["RepFechaDate"],
                }
                for row in rows
            ],
            "total": total,
            "count_all": count_all,
            "resolved_count": resolved_count,
            "bounds": bounds,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
        }

        return jsonify(payload)
    finally:
        conn.close()


@app.route("/api/upload", methods=["POST"])
def api_upload():
    files = request.files.getlist("files")
    if not files:
        abort(400, description="No se recibi? ning?n archivo.")

    summaries: List[dict] = []
    totals = {"total": 0, "added": 0, "duplicates": 0, "ignored": 0}

    for file_storage in files:
        filename = file_storage.filename or "archivo.rpt"
        raw_bytes = file_storage.read()
        try:
            text = raw_bytes.decode("utf-8")
        except UnicodeDecodeError:
            text = raw_bytes.decode("latin-1", errors="ignore")

        records = parse_rpt(text)
        added, duplicates, ignored = insert_records(records, filename)

        summaries.append(
            {
                "name": filename,
                "total": len(records),
                "added": added,
                "duplicates": duplicates,
                "ignored": ignored,
            }
        )

        totals["total"] += len(records)
        totals["added"] += added
        totals["duplicates"] += duplicates
        totals["ignored"] += ignored

    return jsonify({"files": summaries, "totals": totals})


@app.route("/api/rows/<path:row_id>/toggle", methods=["POST"])
def api_toggle(row_id: str):
    conn = get_connection()
    try:
        row = conn.execute("SELECT resolved FROM rows WHERE id = ?", (row_id,)).fetchone()
        if row is None:
            abort(404, description="Fila no encontrada.")
        new_value = 0 if row["resolved"] else 1
        conn.execute("UPDATE rows SET resolved = ? WHERE id = ?", (new_value, row_id))
        conn.commit()
        return jsonify({"id": row_id, "resolved": bool(new_value)})
    finally:
        conn.close()


@app.route("/api/rows/resolved", methods=["DELETE"])
def api_delete_resolved():
    conn = get_connection()
    try:
        cursor = conn.execute("DELETE FROM rows WHERE resolved = 1")
        conn.commit()
        return jsonify({"deleted": cursor.rowcount})
    finally:
        conn.close()


@app.errorhandler(400)
def handle_400(error):
    return jsonify({"error": error.description}), 400


@app.errorhandler(404)
def handle_404(error):
    return jsonify({"error": error.description}), 404


ensure_database()
maybe_load_initial_data()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    app.run(host="0.0.0.0", port=port, debug=False)
