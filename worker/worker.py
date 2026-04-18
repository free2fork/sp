import io
import json
import logging
import os
import uuid

import pyarrow as pa
import pyarrow.flight as flight
from catalog import open_db

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("worker")

_con = None

def get_con():
    global _con
    if _con is None:
        log.info("Opening DuckLake connection...")
        _con = open_db()
    return _con

class WorkerFlightServer(flight.FlightServerBase):
    def __init__(self, host="0.0.0.0", port=8080):
        super().__init__(f"grpc://{host}:{port}")
        log.info(f"Flight server starting on {host}:{port}")

    def do_get(self, context, ticket):
        req = json.loads(ticket.ticket.decode('utf-8'))
        sql = req['sql']
        part_id = req.get('partition_id', '0')
        output_mode = req.get('output_mode', 'flight_stream')
        job_id = req.get('job_id', str(uuid.uuid4()))
        bucket = os.environ.get('BUCKET_NAME', 'duckpond-data')

        log.info(f"partition={part_id} mode={output_mode} sql={sql[:120]}")
        con = get_con()

        try:
            if output_mode == 's3_shuffle':
                out_path = f"s3://{bucket}/results/{job_id}/part-{part_id}.parquet"
                # Dump intermediate or final results directly to Tigris S3
                cursor = con.cursor()
                cursor.execute(f"COPY ({sql}) TO '{out_path}' (FORMAT 'parquet');")
                
                # Return small receipt Table using Flight
                receipt = pa.Table.from_pydict({"s3_path": [out_path], "status": ["ok"]})
                return flight.RecordBatchStream(receipt)
            else:
                # Normal mode: Return data directly over Arrow Flight IPC stream
                cursor = con.cursor()
                arrow_table = cursor.execute(sql).arrow()
                return flight.RecordBatchStream(arrow_table)
        except Exception as exc:
            log.exception("Query failed")
            raise flight.FlightServerError(str(exc))

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    # Pre-warm DuckDB in the main thread avoiding concurrent extension download segfaults!
    get_con()
    server = WorkerFlightServer(port=port)
    server.serve()
