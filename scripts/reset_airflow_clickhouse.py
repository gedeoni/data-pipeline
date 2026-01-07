#!/usr/bin/env python3
import subprocess
import sys
from typing import List
import json


def run(cmd: List[str]) -> None:
    print("+ " + " ".join(cmd))
    subprocess.run(cmd, check=True)


def main() -> int:
    reset_value = "1970-01-01 00:00:00"
    run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "airflow-webserver",
            "airflow",
            "variables",
            "import",
            "/opt/airflow/dags/variables.json",
        ]
    )
    # Reset any per-company watermark variables by pulling keys from Postgres.
    keys_cmd = [
        "docker",
        "compose",
        "exec",
        "-T",
        "airflow-postgres",
        "psql",
        "-U",
        "airflow",
        "-d",
        "airflow",
        "-t",
        "-A",
        "-c",
        "SELECT key FROM variable WHERE key LIKE 'odoo_watermark__%_%';",
    ]
    keys_raw = subprocess.check_output(keys_cmd).decode().strip()
    per_company = sorted({k for k in keys_raw.splitlines() if k})
    for key in per_company:
        run(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "airflow-webserver",
                "airflow",
                "variables",
                "set",
                key,
                reset_value,
            ]
        )
    truncate_cmd = (
        "clickhouse-client --user default --query "
        "\"SELECT name FROM system.tables WHERE database = 'default'\" "
        "--format=TSVRaw | while read -r t; do "
        "clickhouse-client --user default --query "
        "\"TRUNCATE TABLE default.\\`$t\\`\"; "
        "done"
    )
    run(["docker", "compose", "exec", "-T", "clickhouse", "sh", "-lc", truncate_cmd])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
