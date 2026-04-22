# MLOps Batch Job

Minimal, production-style batch pipeline for computing a rolling signal from CSV market data.

## Features

- Reproducible runs via deterministic config values (seed, window, version)
- Structured JSON logs for operational observability
- Metrics JSON generated for both success and error outcomes
- Dockerized execution for consistent environments

## Required CLI

~~~bash
python run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log
~~~

## Processing Notes

- Rolling mean uses the configured trailing window.
- First window-1 rows have no full window, so rolling mean is treated as NaN and signal is set to 0 for those rows.
- rows_processed equals total input rows.

## Local Run

~~~powershell
Set-Location "F:\MY CODES 2.0\mlops-task"
& ".\.venv\Scripts\python.exe" -m pip install -r requirements.txt
& ".\.venv\Scripts\python.exe" run.py --input data.csv --config config.yaml --output metrics.json --log-file run.log
~~~

## Docker

~~~powershell
Set-Location "F:\MY CODES 2.0\mlops-task"
docker build -t mlops-task .
docker run --rm mlops-task
~~~

The container prints metrics JSON to stdout and writes metrics.json and run.log in /app.

## Example metrics.json

latency_ms is runtime-dependent and will vary by machine/environment.

~~~json
{
  "version": "v1",
  "rows_processed": 10,
  "metric": "signal_rate",
  "value": 0.6,
  "latency_ms": 491,
  "seed": 42,
  "status": "success"
}
~~~
"# mlops-task" 
