from __future__ import annotations

import csv
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path


# Update this path if you want the script to target a different text file.
INPUT_FILE = Path("/Users/taozeng/Documents/Membership Stat/PRT Alight 20260127.txt")

# Update this path if you want the CSV files saved somewhere else.
OUTPUT_DIR = Path("/Users/taozeng/Documents/Membership Stat")

# Update this prefix if you want different CSV filenames.
OUTPUT_PREFIX = "text_file_statistics"

NULL_VALUES = {"", "NULL", "null", None}


def clean_value(value: str | None, fallback: str = "UNKNOWN") -> str:
    if value in NULL_VALUES:
        return fallback
    text = value.strip()
    return text or fallback


def parse_float(value: str | None) -> float | None:
    if value in NULL_VALUES:
        return None
    text = value.strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_date(value: str | None) -> date | None:
    if value in NULL_VALUES:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


def calculate_age_years(birth_date: date, as_of_date: date) -> float:
    days = (as_of_date - birth_date).days
    return days / 365.2425


def resolve_paths() -> tuple[Path, Path]:
    input_path = Path(sys.argv[1]).expanduser() if len(sys.argv) > 1 else INPUT_FILE
    output_dir = Path(sys.argv[2]).expanduser() if len(sys.argv) > 2 else OUTPUT_DIR
    return input_path, output_dir


def build_payload(input_path: Path) -> dict[str, object]:
    status_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    gender_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    benefit_totals: dict[str, float] = defaultdict(float)
    benefit_counts: dict[str, int] = defaultdict(int)
    age_totals: dict[str, float] = defaultdict(float)
    age_counts: dict[str, int] = defaultdict(int)

    with input_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="|")

        for row in reader:
            gac_number = clean_value(row.get("GAC_Case_Number"))
            status = clean_value(row.get("Status"))
            gender = clean_value(row.get("Participant_Gender"))

            status_counts[gac_number][status] += 1
            gender_counts[gac_number][gender] += 1

            benefit_amount = parse_float(row.get("Benefit_Amount"))
            if benefit_amount is not None:
                benefit_totals[gac_number] += benefit_amount
                benefit_counts[gac_number] += 1

            birth_date = parse_date(row.get("Participant_Date_of_Birth"))
            run_date = parse_date(row.get("Run_Date")) or date.today()
            if birth_date is not None and birth_date <= run_date:
                age_totals[gac_number] += calculate_age_years(birth_date, run_date)
                age_counts[gac_number] += 1

    gac_numbers = sorted(
        set(status_counts)
        | set(gender_counts)
        | set(benefit_counts)
        | set(age_counts)
    )

    status_rows = [
        {"gac_number": gac_number, "status": status, "count": count}
        for gac_number in sorted(status_counts)
        for status, count in sorted(status_counts[gac_number].items())
    ]
    gender_rows = [
        {"gac_number": gac_number, "gender": gender, "count": count}
        for gac_number in sorted(gender_counts)
        for gender, count in sorted(gender_counts[gac_number].items())
    ]
    benefit_rows = [
        {
            "gac_number": gac_number,
            "average_benefit": benefit_totals[gac_number] / benefit_counts[gac_number],
            "record_count": benefit_counts[gac_number],
        }
        for gac_number in sorted(benefit_counts)
    ]
    age_rows = [
        {
            "gac_number": gac_number,
            "average_age": age_totals[gac_number] / age_counts[gac_number],
            "record_count": age_counts[gac_number],
        }
        for gac_number in sorted(age_counts)
    ]
    summary_rows = [
        {
            "gac_number": gac_number,
            "total_records": sum(status_counts[gac_number].values()),
            "distinct_statuses": len(status_counts[gac_number]),
            "distinct_genders": len(gender_counts[gac_number]),
            "average_benefit": (
                benefit_totals[gac_number] / benefit_counts[gac_number]
                if benefit_counts[gac_number]
                else None
            ),
            "benefit_record_count": benefit_counts[gac_number],
            "average_age": (
                age_totals[gac_number] / age_counts[gac_number] if age_counts[gac_number] else None
            ),
            "age_record_count": age_counts[gac_number],
        }
        for gac_number in gac_numbers
    ]

    return {
        "metadata": {
            "input_file": str(input_path),
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "total_records": sum(row["count"] for row in status_rows),
            "gac_count": len(gac_numbers),
        },
        "summary_rows": summary_rows,
        "status_rows": status_rows,
        "gender_rows": gender_rows,
        "benefit_rows": benefit_rows,
        "age_rows": age_rows,
    }


def write_csv(output_path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def export_csv_files(payload: dict[str, object], output_dir: Path) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    metadata = payload["metadata"]
    summary_rows = payload["summary_rows"]
    status_rows = payload["status_rows"]
    gender_rows = payload["gender_rows"]
    benefit_rows = payload["benefit_rows"]
    age_rows = payload["age_rows"]

    output_files = [
        (
            output_dir / f"{OUTPUT_PREFIX}_summary.csv",
            [
                "gac_number",
                "total_records",
                "distinct_statuses",
                "distinct_genders",
                "average_benefit",
                "benefit_record_count",
                "average_age",
                "age_record_count",
            ],
            summary_rows,
        ),
        (
            output_dir / f"{OUTPUT_PREFIX}_status_counts.csv",
            ["gac_number", "status", "count"],
            status_rows,
        ),
        (
            output_dir / f"{OUTPUT_PREFIX}_gender_counts.csv",
            ["gac_number", "gender", "count"],
            gender_rows,
        ),
        (
            output_dir / f"{OUTPUT_PREFIX}_average_benefit.csv",
            ["gac_number", "average_benefit", "record_count"],
            benefit_rows,
        ),
        (
            output_dir / f"{OUTPUT_PREFIX}_average_age.csv",
            ["gac_number", "average_age", "record_count"],
            age_rows,
        ),
        (
            output_dir / f"{OUTPUT_PREFIX}_metadata.csv",
            ["input_file", "generated_at", "total_records", "gac_count"],
            [metadata],
        ),
    ]

    for output_path, fieldnames, rows in output_files:
        write_csv(output_path, fieldnames, rows)

    return [item[0] for item in output_files]


def main() -> None:
    input_path, output_dir = resolve_paths()

    if not input_path.exists():
        raise FileNotFoundError(
            "Input file was not found. Update INPUT_FILE near the top of the script "
            "or pass a file path as the first command-line argument."
        )

    payload = build_payload(input_path)
    output_files = export_csv_files(payload, output_dir)

    print(f"Input file path: {input_path}")
    print(f"CSV output folder: {output_dir}")
    for output_file in output_files:
        print(f"Created: {output_file}")


if __name__ == "__main__":
    main()
