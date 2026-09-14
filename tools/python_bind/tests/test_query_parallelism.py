"""Per-query worker settings use the same cached plan and transaction path."""

import pytest

from neug import Database


@pytest.fixture
def parallel_conn(tmp_path):
    path = tmp_path / "input.csv"
    path.write_text("id|grp\n" + "".join(f"{i}|{i % 17}\n" for i in range(5003)))
    db = Database("", max_thread_num=4, checkpoint_on_close=False)
    conn = db.connect()
    conn.execute(
        "CREATE NODE TABLE parallel_item(id INT64, grp INT64, PRIMARY KEY(id))"
    )
    conn.execute(f'COPY parallel_item FROM "{path}"', num_threads=4)
    try:
        yield conn
    finally:
        conn.close()
        db.close()


@pytest.mark.parametrize("workers", [0, 1, 2, 4, 100])
def test_cached_plan_across_worker_counts(parallel_conn, workers):
    queries = [
        "MATCH (n:parallel_item) WHERE n.id % 3 = 0 RETURN n.id, n.grp ORDER BY n.id",
        "MATCH (a:parallel_item), (b:parallel_item) WHERE a.grp = b.grp AND b.id < 17 "
        "RETURN a.id, b.id ORDER BY a.id",
        "MATCH (n:parallel_item) RETURN n.grp, sum(n.id) ORDER BY n.grp",
        "MATCH (n:parallel_item) RETURN n.grp, sum(n.id) AS total "
        "ORDER BY total DESC, n.grp SKIP 2 LIMIT 5",
        "MATCH (n:parallel_item) RETURN n.id ORDER BY n.id SKIP 4090 LIMIT 25",
    ]
    for query in queries:
        expected = list(parallel_conn.execute(query, num_threads=1))
        assert list(parallel_conn.execute(query, num_threads=workers)) == expected
    # Query-specific overrides must not affect the next invocation.
    assert list(parallel_conn.execute(queries[0])) == list(
        parallel_conn.execute(queries[0], num_threads=1)
    )


@pytest.mark.parametrize("workers", [-1, True, 1.5, "4"])
def test_invalid_workers(parallel_conn, workers):
    with pytest.raises(ValueError, match="num_threads"):
        parallel_conn.execute("RETURN 1", num_threads=workers)
    assert list(parallel_conn.execute("RETURN 1")) == [[1]]


def test_parallel_read_transaction_and_serial_write_rollback(parallel_conn):
    conn = parallel_conn
    conn.begin_transaction(read_only=True)
    assert list(
        conn.execute("MATCH (n:parallel_item) RETURN count(n)", num_threads=4)
    ) == [[5003]]
    conn.commit()
    conn.begin_transaction()
    conn.execute("CREATE (:parallel_item {id: 6000, grp: 1})", num_threads=4)
    assert list(
        conn.execute("MATCH (n:parallel_item) RETURN count(n)", num_threads=4)
    ) == [[5004]]
    conn.rollback()
    assert list(
        conn.execute("MATCH (n:parallel_item) RETURN count(n)", num_threads=4)
    ) == [[5003]]


def test_native_binding_rejects_negative_workers(parallel_conn):
    result = parallel_conn._py_connection.execute("RETURN 1", "", {}, -1)
    assert result.status_code() != 0
    assert list(parallel_conn.execute("RETURN 1", num_threads=1)) == [[1]]


def test_profile_parallel_query(parallel_conn):
    query = "MATCH (n:parallel_item) WHERE n.id % 3 = 0 RETURN n.id"
    result = parallel_conn.execute("PROFILE " + query, num_threads=4)
    assert sorted(list(result)) == list(parallel_conn.execute(query, num_threads=1))
    metrics = result.get_profile_metrics()["operators"]
    assert metrics
    assert all(op["output_rows"] >= 0 for op in metrics)
