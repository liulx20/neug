"""Concurrent service transactions share the database execution workers."""

import socket
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from threading import Event

from neug import Database
from neug.session import Session


def test_service_shared_pool_snapshot_commit_and_rollback(tmp_path):
    db = Database(str(tmp_path / "db"), max_thread_num=4)
    loader = db.connect()
    loader.execute("CREATE NODE TABLE item(id INT64, PRIMARY KEY(id))")
    loader.execute("CREATE (:item {id:0})")
    loader.close()
    with socket.socket() as port_probe:
        port_probe.bind(("127.0.0.1", 0))
        port = port_probe.getsockname()[1]
    endpoint = db.serve(host="127.0.0.1", port=port, blocking=False)
    gate = Barrier(4)
    writes_done = Event()

    def read():
        session = Session.open(endpoint)
        try:
            session.begin_transaction(read_only=True)
            assert list(session.execute("MATCH (n:item) RETURN count(*)")) == [[1]]
            gate.wait(timeout=10)
            for _ in range(20):
                assert list(session.execute("MATCH (n:item) RETURN count(*)")) == [[1]]
            assert writes_done.wait(timeout=30)
            assert list(session.execute("MATCH (n:item) RETURN count(*)")) == [[1]]
            session.commit()
        finally:
            session.close()

    def write():
        session = Session.open(endpoint)
        try:
            gate.wait(timeout=10)
            for i in range(1, 11):
                session.begin_transaction()
                session.execute(f"CREATE (:item {{id:{i}}})")
                if i % 2:
                    session.commit()
                else:
                    session.rollback()
        finally:
            writes_done.set()
            session.close()

    try:
        with ThreadPoolExecutor(max_workers=4) as executor:
            jobs = [executor.submit(read) for _ in range(3)]
            jobs.append(executor.submit(write))
            for job in jobs:
                job.result(timeout=30)
        session = Session.open(endpoint)
        try:
            assert list(
                session.execute("MATCH (n:item) RETURN n.id ORDER BY n.id")
            ) == [[0], [1], [3], [5], [7], [9]]
        finally:
            session.close()
    finally:
        db.close()
