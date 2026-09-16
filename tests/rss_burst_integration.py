# SPDX-License-Identifier: AGPL-3.0-only
"""Exercise real RSS routes using disposable Redis and a local X API fixture."""
import concurrent.futures
import contextlib
import http.server
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def get(url):
    try:
        response = urllib.request.urlopen(url, timeout=15)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        return response.status, response.headers, response.read()


def wait_ready(process, probe):
    for _ in range(200):
        if process.poll() is not None:
            raise RuntimeError(f"Test process exited with {process.returncode}")
        try:
            if probe():
                return
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.025)
    raise TimeoutError("Test service did not start")


class Fixture(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        variables = json.loads(urllib.parse.parse_qs(parsed.query)["variables"][0])
        with self.server.lock:
            self.server.active += 1
            self.server.peak = max(self.server.peak, self.server.active)
            self.server.calls += 1
        try:
            time.sleep(self.server.delay)
            username = variables.get("screen_name")
            if username == "ratelimited":
                data = {"errors": [{"code": 88, "message": "Rate limit exceeded"}]}
            elif username == "broken":
                data = {}
            elif username:
                data = {"data": {"user": {"result": {
                    "rest_id": str(sum(username.encode()) + 1000),
                    "core": {"screen_name": username, "name": username},
                }}}}
            elif "listId" in variables:
                data = {"data": {"list": {"id_str": variables["listId"],
                                           "name": "Fixture list"}}}
            else:
                timeline = {"instructions": [{"type": "TimelineAddEntries", "entries": [{
                    "entryId": "cursor-bottom-0",
                    "content": {"value": "next-" + variables.get("cursor", "first")},
                }]}]}
                if "rawQuery" in variables:
                    data = {"data": {"search": {"timeline_response": {"timeline": timeline}}}}
                else:
                    data = {"data": {"user": {"result": {"timeline": {"timeline": timeline}}}}}
            payload = json.dumps(data).encode()
            self.send_response(400 if username == "broken" else 200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("x-rate-limit-remaining", "100")
            self.send_header("x-rate-limit-limit", "1000")
            self.send_header("x-rate-limit-reset", str(int(time.time()) + 60))
            self.end_headers()
            self.wfile.write(payload)
        finally:
            with self.server.lock:
                self.server.active -= 1


@contextlib.contextmanager
def instance(max_pending=100, wait_ms=10000, delay=0.05, sessions=True):
    with tempfile.TemporaryDirectory(prefix="nitter-rss-test-") as directory:
        directory = Path(directory)
        proxy = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Fixture)
        proxy.lock = threading.Lock()
        proxy.active = proxy.peak = proxy.calls = 0
        proxy.delay = delay
        thread = threading.Thread(target=proxy.serve_forever, daemon=True)
        thread.start()
        processes = []
        try:
            with (directory / "services.log").open("w+") as log:
                try:
                    redis_port = free_port()
                    redis = subprocess.Popen([
                        "redis-server", "--bind", "127.0.0.1", "--port", str(redis_port),
                        "--save", "", "--appendonly", "no", "--dir", str(directory),
                    ], stdout=log, stderr=log)
                    processes.append(redis)

                    def redis_ready():
                        with socket.create_connection(("127.0.0.1", redis_port), timeout=1) as sock:
                            sock.sendall(b"PING\r\n")
                            return sock.recv(64).startswith(b"+PONG")

                    wait_ready(redis, redis_ready)
                    port = free_port()
                    conf = (ROOT / "nitter.example.conf").read_text()
                    for old, new in {
                        'address = "0.0.0.0"': 'address = "127.0.0.1"',
                        'port = 8080': f'port = {port}',
                        'redisPort = 6379': f'redisPort = {redis_port}',
                        'apiProxy = ""': f'apiProxy = "http://127.0.0.1:{proxy.server_port}"',
                        'disableTid = false': 'disableTid = true',
                        'hmacKey = "secretkey"': 'hmacKey = "local-test-only"',
                        'maxPendingReqs = 100': f'maxPendingReqs = {max_pending}',
                        'sessionWaitMs = 10000': f'sessionWaitMs = {wait_ms}',
                    }.items():
                        conf = conf.replace(old, new)
                    (directory / "nitter.conf").write_text(conf)
                    session = sessions if isinstance(sessions, dict) else {
                        "kind": "cookie", "auth_token": "fake", "ct0": "fake"}
                    (directory / "sessions.jsonl").write_text(json.dumps(session) if sessions else "")
                    env = dict(os.environ, NITTER_CONF_FILE=str(directory / "nitter.conf"),
                               NITTER_SESSIONS_FILE=str(directory / "sessions.jsonl"))
                    process = subprocess.Popen([str(ROOT / "nitter")], cwd=ROOT,
                                               env=env, stdout=log, stderr=log)
                    processes.append(process)
                    base = f"http://127.0.0.1:{port}"
                    wait_ready(process, lambda: get(base + "/about")[0] == 200)
                    yield base, proxy
                except BaseException:
                    log.flush()
                    print((directory / "services.log").read_text()[-6000:])
                    raise
                finally:
                    for process in reversed(processes):
                        if process.poll() is None:
                            process.terminate()
                            process.wait(timeout=5)
        finally:
            proxy.shutdown()
            proxy.server_close()
            thread.join()


def burst(base, paths):
    barrier = threading.Barrier(len(paths))

    def fetch(path):
        barrier.wait(timeout=5)
        return get(base + path)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(paths)) as executor:
        return list(executor.map(fetch, paths))


class RssBurstTests(unittest.TestCase):
    def test_fifteen_uncached_feeds_refresh_without_429(self):
        with instance() as (base, proxy):
            responses = burst(base, [f"/user{i}/rss" for i in range(15)])
            self.assertEqual([r[0] for r in responses], [200] * 15)
            for i, (_, headers, body) in enumerate(responses):
                self.assertIn("application/rss+xml", headers["Content-Type"])
                self.assertIn(f"@user{i}", ET.fromstring(body).findtext("channel/title"))
            self.assertEqual(proxy.calls, 30)
            self.assertLessEqual(proxy.peak, 2)

    def test_two_readers_share_refresh_and_subsequent_requests_use_cache(self):
        with instance() as (base, proxy):
            responses = burst(base, ["/shared/rss"] * 30)
            self.assertEqual([r[0] for r in responses], [200] * 30)
            self.assertEqual(len({r[2] for r in responses}), 1)
            self.assertEqual(proxy.calls, 2)
            self.assertEqual(get(base + "/shared/rss")[0], 200)
            self.assertEqual(proxy.calls, 2)

    def test_full_queue_returns_503_and_recovers(self):
        with instance(max_pending=0, delay=0.1) as (base, proxy):
            responses = burst(base, [f"/overflow{i}/rss" for i in range(8)])
            codes = [r[0] for r in responses]
            self.assertIn(503, codes)
            self.assertNotIn(429, codes)
            for code, headers, _ in responses:
                if code == 503:
                    self.assertEqual(headers["Retry-After"], "1")
            self.assertEqual(get(base + "/recovered/rss")[0], 200)
            self.assertLessEqual(proxy.peak, 2)

    def test_wait_timeout_returns_503_and_recovers(self):
        with instance(wait_ms=10, delay=0.1) as (base, _):
            responses = burst(base, [f"/timeout{i}/rss" for i in range(8)])
            self.assertIn(503, [r[0] for r in responses])
            self.assertNotIn(429, [r[0] for r in responses])
            self.assertEqual(get(base + "/recovered/rss")[0], 200)

    def test_upstream_rate_limit_remains_429(self):
        with instance() as (base, _):
            self.assertEqual(get(base + "/ratelimited/rss")[0], 429)

    def test_missing_credentials_remain_429(self):
        with instance(sessions=False) as (base, proxy):
            self.assertEqual(get(base + "/missing/rss")[0], 429)
            self.assertEqual(proxy.calls, 0)

    def test_invalid_credentials_do_not_leak_session_slots(self):
        session = {"kind": "cookie", "auth_token": "", "ct0": ""}
        with instance(wait_ms=10, sessions=session) as (base, proxy):
            for _ in range(5):
                self.assertEqual(get(base + "/invalid/rss")[0], 429)
            self.assertEqual(proxy.calls, 0)

    def test_upstream_errors_release_slots_for_later_requests(self):
        with instance() as (base, _):
            for _ in range(5):
                self.assertEqual(get(base + "/broken/rss")[0], 500)
            self.assertEqual(get(base + "/recovered/rss")[0], 200)

    def test_feed_variants_share_refreshes_without_merging_distinct_keys(self):
        paths = ["/variants/with_replies/rss", "/variants/media/rss",
                 "/variants/articles/rss", "/variants/search/rss?q=one",
                 "/variants/search/rss?q=two", "/search/rss?q=one",
                 "/search/rss?q=two", "/i/lists/123/rss"]
        with instance() as (base, proxy):
            for path in paths:
                with self.subTest(path=path):
                    before = proxy.calls
                    responses = burst(base, [path] * 4)
                    self.assertEqual([r[0] for r in responses], [200] * 4)
                    self.assertEqual(len({r[2] for r in responses}), 1)
                    self.assertGreater(proxy.calls, before)
                    self.assertLessEqual(proxy.calls - before, 2)
                    ET.fromstring(responses[0][2])
            first = get(base + "/variants/rss")
            before = proxy.calls
            page = get(base + "/variants/rss?cursor=second")
            self.assertEqual(page[0], 200)
            self.assertNotEqual(first[1]["Min-Id"], page[1]["Min-Id"])
            self.assertGreater(proxy.calls, before)


if __name__ == "__main__":
    unittest.main(verbosity=2)
