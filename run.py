import argparse
import csv
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import yaml


class JobError(Exception):
    """Raised for expected, user-facing batch job failures."""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": utc_now_iso(),
            "level": record.levelname.lower(),
            "message": record.getMessage(),
        }
        fields = getattr(record, "fields", None)
        if isinstance(fields, dict):
            payload.update(fields)
        return json.dumps(payload, ensure_ascii=True)


def setup_logger(log_file: str) -> logging.Logger:
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("batch_job")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.handlers.clear()

    file_handler = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    file_handler.setFormatter(JsonLogFormatter())
    logger.addHandler(file_handler)
    return logger


def log_info(logger: logging.Logger, message: str, **fields: Any) -> None:
    logger.info(message, extra={"fields": fields})


def log_error(logger: logging.Logger, message: str, **fields: Any) -> None:
    logger.error(message, extra={"fields": fields})


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch processing job")
    parser.add_argument("--input", required=True, help="Path to input CSV file")
    parser.add_argument("--config", required=True, help="Path to YAML config")
    parser.add_argument("--output", required=True, help="Path to output metrics JSON")
    parser.add_argument("--log-file", required=True, help="Path to structured log file")
    return parser.parse_args()


def load_config(config_path: str) -> dict[str, Any]:
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)
    except FileNotFoundError as exc:
        raise JobError(f"Config file not found: {config_path}") from exc
    except yaml.YAMLError as exc:
        raise JobError(f"Invalid YAML in config file: {exc}") from exc

    if not isinstance(raw, dict):
        raise JobError("Config must be a YAML mapping.")

    required_keys = ("seed", "window", "version")
    missing = [k for k in required_keys if k not in raw]
    if missing:
        raise JobError(f"Missing required config keys: {', '.join(missing)}")

    seed = raw["seed"]
    window = raw["window"]
    version = raw["version"]

    if not isinstance(seed, int) or isinstance(seed, bool):
        raise JobError("Config key 'seed' must be an integer.")
    if not isinstance(window, int) or isinstance(window, bool):
        raise JobError("Config key 'window' must be an integer.")
    if window <= 0:
        raise JobError("Config key 'window' must be greater than 0.")
    if not isinstance(version, str) or version.strip() == "":
        raise JobError("Config key 'version' must be a non-empty string.")

    return {"seed": seed, "window": window, "version": version}


def load_close_column(input_path: str) -> np.ndarray:
    try:
        with open(input_path, "r", encoding="utf-8", newline="") as f:
            try:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames
            except csv.Error as exc:
                raise JobError(f"Invalid CSV format: {exc}") from exc

            if not fieldnames:
                raise JobError("CSV file is empty.")
            if "close" not in fieldnames:
                raise JobError("CSV is missing required 'close' column.")

            close_values: list[float] = []
            for row_idx, row in enumerate(reader, start=2):
                raw_value = row.get("close")
                if raw_value is None or raw_value.strip() == "":
                    raise JobError(f"Missing 'close' value at row {row_idx}.")
                try:
                    close_values.append(float(raw_value))
                except ValueError as exc:
                    raise JobError(f"Non-numeric 'close' value at row {row_idx}: {raw_value}") from exc

    except FileNotFoundError as exc:
        raise JobError(f"Input CSV file not found: {input_path}") from exc
    except UnicodeDecodeError as exc:
        raise JobError(f"Invalid CSV encoding: {exc}") from exc
    except csv.Error as exc:
        raise JobError(f"Invalid CSV format: {exc}") from exc

    if not close_values:
        raise JobError("CSV file has no data rows.")

    return np.asarray(close_values, dtype=np.float64)


def compute_signal_rate(close_values: np.ndarray, window: int) -> tuple[int, float]:
    rows_processed = int(close_values.size)
    if rows_processed == 0:
        return 0, 0.0

    rolling_mean = np.full(rows_processed, np.nan, dtype=np.float64)
    if rows_processed >= window:
        cumulative = np.cumsum(close_values, dtype=np.float64)
        window_sums = cumulative[window - 1 :].copy()
        if window > 1:
            window_sums[1:] = window_sums[1:] - cumulative[:-window]
        rolling_mean[window - 1 :] = window_sums / window

    # For the first window-1 rows, rolling_mean is NaN and comparisons evaluate to False -> signal 0.
    signals = (close_values > rolling_mean).astype(np.int8)
    signal_rate = float(signals.mean()) if rows_processed > 0 else 0.0
    return rows_processed, signal_rate


def write_json(path: str, payload: dict[str, Any]) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def main() -> int:
    args = parse_args()
    start_time = time.perf_counter()
    version = "v1"

    logger = logging.getLogger("batch_job")
    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())

    try:
        logger = setup_logger(args.log_file)

        log_info(
            logger,
            "job_start",
            input=args.input,
            config=args.config,
            output=args.output,
            log_file=args.log_file,
        )

        config = load_config(args.config)
        version = config["version"]
        seed = config["seed"]
        window = config["window"]

        log_info(logger, "config_loaded", version=version, seed=seed, window=window)

        np.random.seed(seed)
        log_info(logger, "seed_set", seed=seed)

        close_values = load_close_column(args.input)
        log_info(logger, "rows_loaded", rows_loaded=int(close_values.size))

        log_info(
            logger,
            "processing_started",
            operation="rolling_mean_and_signal",
            window=window,
            warmup_policy="first_window_minus_1_rows_have_nan_rolling_mean_and_signal_0",
        )
        rows_processed, signal_rate = compute_signal_rate(close_values, window)
        log_info(logger, "processing_completed", rows_processed=rows_processed)

        latency_ms = int((time.perf_counter() - start_time) * 1000)
        metrics = {
            "version": version,
            "rows_processed": rows_processed,
            "metric": "signal_rate",
            "value": round(signal_rate, 4),
            "latency_ms": latency_ms,
            "seed": seed,
            "status": "success",
        }

        write_json(args.output, metrics)
        log_info(
            logger,
            "metrics_summary",
            rows_processed=rows_processed,
            signal_rate=metrics["value"],
            latency_ms=latency_ms,
        )
        log_info(logger, "job_end", status="success")

        print(json.dumps(metrics))
        return 0

    except Exception as exc:
        error_payload = {
            "version": version,
            "status": "error",
            "error_message": str(exc),
        }

        try:
            write_json(args.output, error_payload)
        except Exception as write_exc:
            log_error(
                logger,
                "metrics_write_failed",
                error_message=str(write_exc),
                output=args.output,
            )

        log_error(logger, "job_error", error_message=str(exc))
        log_info(logger, "job_end", status="error")
        print(json.dumps(error_payload))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
