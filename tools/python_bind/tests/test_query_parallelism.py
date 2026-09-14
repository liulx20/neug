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


@pytest.mark.parametrize("workers", [1, 2, 4])
def test_parallel_graph_expansion(parallel_conn, tmp_path, workers):
    conn = parallel_conn
    edges = [
        (src, (src + hop + 1) % 5003, hop)
        for src in range(4500)
        for hop in range(src % 4)
    ]
    # Parallel edges preserve multiplicity; isolated vertices exercise optional output.
    edges += [(src, (src + 1) % 5003, 9) for src in range(0, 4500, 13)]
    path = tmp_path / "edges.csv"
    path.write_text(
        "src|dst|weight\n"
        + "".join(f"{src}|{dst}|{weight}\n" for src, dst, weight in edges)
    )
    conn.execute(
        "CREATE REL TABLE parallel_link(FROM parallel_item TO parallel_item, weight INT64)"
    )
    conn.execute(f'COPY parallel_link FROM "{path}"')
    outgoing = {}
    for src, dst, weight in edges:
        outgoing.setdefault(src, []).append((dst, weight))
    cases = [
        (
            "MATCH (a:parallel_item)-[e:parallel_link]->(b:parallel_item) "
            "RETURN a.id, b.id, e.weight",
            edges,
        ),
        (
            "MATCH (a:parallel_item)<-[e:parallel_link]-(b:parallel_item) "
            "RETURN a.id, b.id, e.weight",
            [(dst, src, w) for src, dst, w in edges],
        ),
        (
            "MATCH (a:parallel_item)-[e:parallel_link]-(b:parallel_item) "
            "RETURN a.id, b.id, e.weight",
            edges + [(dst, src, w) for src, dst, w in edges],
        ),
        (
            "MATCH (a:parallel_item)-[e:parallel_link]->(b:parallel_item) "
            "WHERE e.weight > 1 RETURN a.id, b.id, e.weight",
            [e for e in edges if e[2] > 1],
        ),
        (
            "MATCH (a:parallel_item)-[e:parallel_link]->(b:parallel_item) "
            "WHERE b.grp = 3 RETURN a.id, b.id, e.weight",
            [e for e in edges if e[1] % 17 == 3],
        ),
        (
            "MATCH (a:parallel_item)-[e:parallel_link]->(b:parallel_item) "
            "WHERE b.grp + e.weight > 12 RETURN a.id, b.id, e.weight",
            [e for e in edges if e[1] % 17 + e[2] > 12],
        ),
        (
            "MATCH (a:parallel_item) OPTIONAL MATCH (a)-[e:parallel_link]->(b:parallel_item) "
            "RETURN a.id, b.id, e.weight",
            edges + [(src, None, None) for src in range(5003) if src not in outgoing],
        ),
        (
            "MATCH (a:parallel_item)-[:parallel_link]->(b:parallel_item)-[:parallel_link]->(c:parallel_item) "
            "RETURN a.id, b.id, c.id",
            [
                (src, dst, end)
                for src, dst, _ in edges
                for end, _ in outgoing.get(dst, [])
            ],
        ),
        (
            "MATCH (a:parallel_item)-[:parallel_link]->(b:parallel_item) "
            "RETURN a.id, count(b)",
            [(src, len(nbrs)) for src, nbrs in outgoing.items()],
        ),
    ]
    # Vertex-only outputs exercise expansion implementations that do not keep e.
    cases += [
        (
            "MATCH (a:parallel_item)-[:parallel_link]->(b:parallel_item) "
            "RETURN a.id, b.id",
            [(src, dst) for src, dst, _ in edges],
        ),
        (
            "MATCH (a:parallel_item)-[e:parallel_link]->(b:parallel_item) "
            "WHERE e.weight > 1 RETURN a.id, b.id",
            [(src, dst) for src, dst, weight in edges if weight > 1],
        ),
        (
            "MATCH (a:parallel_item)-[:parallel_link]->(b:parallel_item) "
            "WHERE b.grp = 3 RETURN a.id, b.id",
            [(src, dst) for src, dst, _ in edges if dst % 17 == 3],
        ),
        (
            "MATCH (a:parallel_item)-[:parallel_link]->(b:parallel_item) "
            "WHERE b.grp + 1 > 12 RETURN a.id, b.id",
            [(src, dst) for src, dst, _ in edges if dst % 17 + 1 > 12],
        ),
    ]
    for query, expected in cases:
        actual = list(conn.execute(query, num_threads=workers))
        assert sorted(map(tuple, actual), key=repr) == sorted(expected, key=repr), query
    ordered = cases[0][0] + " ORDER BY a.id, b.id, e.weight SKIP 1000 LIMIT 25"
    assert list(conn.execute(ordered, num_threads=workers)) == [
        list(e) for e in sorted(edges)[1000:1025]
    ]
    profiled = conn.execute("PROFILE " + cases[0][0], num_threads=workers)
    assert len(list(profiled)) == len(edges)
    names = [op["operator_name"] for op in profiled.get_profile_metrics()["operators"]]
    assert any("EdgeExpand" in name for name in names), names


@pytest.mark.parametrize("workers", [1, 2, 4])
def test_parallel_path_expansion(parallel_conn, tmp_path, workers):
    conn = parallel_conn
    edges = [(src, (src // 3) * 3 + (src + 1) % 3, 1) for src in range(4100)]
    edges += [(src, src, 2) for src in range(0, 4100, 7)]
    edges += [(src, (src // 3) * 3 + (src + 1) % 3, 3) for src in range(0, 4100, 11)]
    path = tmp_path / "walk.csv"
    path.write_text("src|dst|weight\n" + "".join(f"{a}|{b}|{w}\n" for a, b, w in edges))
    conn.execute(
        "CREATE REL TABLE walk(FROM parallel_item TO parallel_item, weight INT64)"
    )
    conn.execute(f'COPY walk FROM "{path}"')
    adjacency = {}
    for src, dst, weight in edges:
        adjacency.setdefault(src, []).append((dst, weight))

    def walks(start, predicate=lambda weight: True):
        result = [((start,), ())]
        frontier = list(result)
        for _ in range(2):
            frontier = [
                (nodes + (dst,), weights + (weight,))
                for nodes, weights in frontier
                for dst, weight in adjacency.get(nodes[-1], [])
                if predicate(weight)
            ]
            result.extend(frontier)
        return result

    expected = [(src, nodes[-1]) for src in range(5003) for nodes, _ in walks(src)]
    query = "MATCH (a:parallel_item)-[:walk*0..2]->(b:parallel_item) RETURN a.id, b.id"
    result = conn.execute("PROFILE " + query, num_threads=workers)
    assert sorted(map(tuple, result)) == sorted(expected)
    assert "PathExpandVOpr" in {
        op["operator_name"] for op in result.get_profile_metrics()["operators"]
    }

    # Repeated input roots must not be deduplicated across work ranges.
    query = (
        "MATCH (seed:parallel_item)-[:walk]->(a:parallel_item) "
        "MATCH (a)-[:walk*0..2]->(b:parallel_item) RETURN seed.id, a.id, b.id"
    )
    expected = [
        (seed, src, nodes[-1]) for seed, src, _ in edges for nodes, _ in walks(src)
    ]
    assert sorted(map(tuple, conn.execute(query, num_threads=workers))) == sorted(
        expected
    )

    for filtered in [False, True]:
        pattern = (
            "e:walk*0..2 (r, _ | WHERE r.weight > 1)" if filtered else "e:walk*0..2"
        )
        query = f"MATCH (a:parallel_item)-[{pattern}]->(b:parallel_item) RETURN a.id, e"
        result = conn.execute("PROFILE " + query, num_threads=workers)
        actual = [
            (
                src,
                tuple(n["id"] for n in path["nodes"]),
                tuple(e["weight"] for e in path["rels"]),
            )
            for src, path in result
        ]
        expected = [
            (src, nodes, weights)
            for src in range(5003)
            for nodes, weights in walks(src, lambda w: not filtered or w > 1)
        ]
        assert sorted(actual) == sorted(expected)
        names = {
            op["operator_name"] for op in result.get_profile_metrics()["operators"]
        }
        assert ("PathExpandOprWithPred" if filtered else "PathExpandOpr") in names


@pytest.mark.parametrize("workers", [1, 2, 4])
def test_parallel_intersect_and_triangle(parallel_conn, tmp_path, workers):
    conn = parallel_conn
    edges = []
    for base in range(0, 4800, 4):
        # An empty root precedes each triangle, exposing skipped-row offsets.
        weight = 2 if base % 8 == 0 else 0
        edges.extend(
            [
                (base + 1, base + 3, weight),
                (base + 1, base + 2, 2 - weight),
                (base + 2, base + 3, 0),
            ]
        )
    path = tmp_path / "triangle.csv"
    path.write_text("src|dst|weight\n" + "".join(f"{a}|{b}|{w}\n" for a, b, w in edges))
    conn.execute(
        "CREATE REL TABLE tri(FROM parallel_item TO parallel_item, weight INT64)"
    )
    conn.execute(f'COPY tri FROM "{path}"')
    adjacency = {}
    for src, dst, weight in edges:
        adjacency.setdefault(src, []).append((dst, weight))
    triangles = [
        (a, b, c, wab, wac, wbc)
        for a in adjacency
        for b, wab in adjacency[a]
        for c, wac in adjacency[a]
        for end, wbc in adjacency.get(b, [])
        if end == c
    ]
    queries = [
        (
            "MATCH (a:parallel_item)-[:tri]->(b:parallel_item), "
            "(a)-[:tri]->(c:parallel_item), (b)-[:tri]->(c) RETURN a.id, b.id, c.id",
            [row[:3] for row in triangles],
        ),
        (
            "MATCH (a:parallel_item)-[ab:tri]->(b:parallel_item), "
            "(a)-[ac:tri]->(c:parallel_item), (b)-[bc:tri]->(c) "
            "RETURN a.id, b.id, c.id, ab.weight, ac.weight, bc.weight",
            triangles,
        ),
    ]
    names = set()
    for query, expected in queries:
        result = conn.execute("PROFILE " + query, num_threads=workers)
        assert sorted(map(tuple, result)) == sorted(expected)
        names.update(
            op["operator_name"] for op in result.get_profile_metrics()["operators"]
        )
    assert {"IntersectOprMultip", "IntersectWithEdgeOpr"} <= names, names
    # TC fusion is checked directly in the C++ scheduler test; these queries
    # may retain the unfused plan after compiler optimization.
    for compare in [">", "<"]:
        query = (
            f"MATCH (a:parallel_item)-[e:tri]->(n:parallel_item) WHERE e.weight {compare} 1 "
            "WITH a, collect(DISTINCT n) AS nbrs "
            "MATCH (a)-[:tri]->(b:parallel_item)-[:tri]->(c:parallel_item) "
            "WHERE c IN nbrs RETURN a.id, b.id, c.id"
        )
        result = conn.execute("PROFILE " + query, num_threads=workers)
        expected = [
            row[:3]
            for row in triangles
            if (row[4] > 1 if compare == ">" else row[4] < 1)
        ]
        assert sorted(map(tuple, result)) == sorted(expected)


@pytest.mark.parametrize("workers", [1, 2, 4])
def test_partition_outputs_feed_downstream_aggregates(parallel_conn, workers):
    queries = [
        (
            "MATCH (n:parallel_item) WITH DISTINCT n.id AS id, n.grp AS grp "
            "WHERE id % 3 = 0 RETURN sum(id + grp), count(*)",
            [[sum(i + i % 17 for i in range(5003) if i % 3 == 0), 1668]],
            "DedupOpr",
        ),
        (
            "MATCH (n:parallel_item) WITH n.id AS id, sum(n.grp) AS total "
            "WHERE id % 3 = 0 RETURN sum(id + total), count(*)",
            [[sum(i + i % 17 for i in range(5003) if i % 3 == 0), 1668]],
            "GroupByOpr",
        ),
    ]
    for query, expected, operator in queries:
        result = parallel_conn.execute("PROFILE " + query, num_threads=workers)
        assert list(result) == expected
        assert operator in {
            op["operator_name"] for op in result.get_profile_metrics()["operators"]
        }
