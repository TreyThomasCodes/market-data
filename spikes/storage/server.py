"""Minimal read API: POST /sql with a SQL body -> Arrow IPC stream. ?compress=zstd for compressed buffers.
One process, one thread per request, each with its own DuckDB cursor on one shared DuckLake attachment."""
import os, sys, urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
import duckdb, pyarrow as pa, pyarrow.ipc as ipc
LOCAL = threading.local()

os.chdir(os.path.dirname(os.path.abspath(__file__)))
CON = duckdb.connect()
CON.execute("LOAD ducklake; LOAD postgres")
CON.execute("ATTACH 'ducklake:postgres:dbname=lake host=127.0.0.1 port=55432 user=postgres password=bench' "
            "AS lake (DATA_PATH 'lake/data/', READ_ONLY)")


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def do_POST(self):
        q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        sql = self.rfile.read(int(self.headers["Content-Length"])).decode()
        import time; t0 = time.perf_counter()
        try:
            cur = getattr(LOCAL, "cur", None)
            if cur is None:  # one cursor per server thread, kept
                cur = LOCAL.cur = CON.cursor()
                cur.execute("USE lake")
            reader = cur.sql(sql).to_arrow_reader(1_000_000)
            opts = ipc.IpcWriteOptions(compression=q["compress"][0] if "compress" in q else None)
            sink = pa.BufferOutputStream()
            with ipc.new_stream(sink, reader.schema, options=opts) as w:
                for b in reader:
                    w.write_batch(b)
            body = sink.getvalue()
            self.send_response(200)
            self.send_header("Content-Type", "application/vnd.apache.arrow.stream")
        except Exception as e:
            body = str(e).encode()
            self.send_response(400)
        self.send_header("X-Server-Ms", f"{(time.perf_counter()-t0)*1000:.1f}")
        self.send_header("Content-Length", str(len(body)))
        self._headers_buffer.append(b"\r\n")  # headers and body in one write
        self.wfile.write(b"".join(self._headers_buffer) + body)
        self._headers_buffer = []


if __name__ == "__main__":
    ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1]) if len(sys.argv) > 1 else 8765), H).serve_forever()
