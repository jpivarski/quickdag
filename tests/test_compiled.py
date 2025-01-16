from __future__ import annotations

import quickdag._core as m


def test_construct():
    dag = m.DAG32()

    node1 = dag.new_node([1, 2, 3], b"hello")
    node2 = dag.new_node([], b"")
    node3 = dag.new_node([4, 5], b"there")

    assert node1 == 0
    assert dag.node_depends_on(node1) == [1, 2, 3]
    assert dag.node_payload(node1) == b"hello"

    assert node2 == 1
    assert dag.node_depends_on(node2) == []
    assert dag.node_payload(node2) == b""

    assert node3 == 2
    assert dag.node_depends_on(node3) == [4, 5]
    assert dag.node_payload(node3) == b"there"

    assert dag.nbytes == 60


def test_realistic():
    dag = m.DAG32()

    source1 = dag.new_node([], b"")
    source2 = dag.new_node([], b"")

    layer1_node1 = dag.new_node([source1, source2], b"")
    layer1_node2 = dag.new_node([source1, source2], b"")
    layer1_node3 = dag.new_node([source1, source2], b"")

    layer2_node1 = dag.new_node([layer1_node1, layer1_node2, layer1_node3], b"")
    layer2_node2 = dag.new_node([layer1_node1, layer1_node2, layer1_node3], b"")

    assert dag.upstream_map == [[], [], [0, 1], [0, 1], [0, 1], [2, 3, 4], [2, 3, 4]]
    assert dag.downstream_map == [[2, 3, 4], [2, 3, 4], [5, 6], [5, 6], [5, 6], [], []]
