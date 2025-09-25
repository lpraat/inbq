from dataclasses import dataclass


@dataclass
class RawLineageObject:
    id: int
    name: str
    kind: str
    nodes: list[int]


@dataclass
class RawLineageNode:
    id: int
    name: str
    source_object: int
    input: list[int]


@dataclass
class RawLineage:
    objects: list[RawLineageObject]
    lineage_nodes: list[RawLineageNode]
    output_lineage: list[int]


@dataclass
class ReadyLineageNodeInput:
    obj_name: str
    node_name: str


@dataclass
class ReadyLineageNode:
    name: str
    input: list[ReadyLineageNodeInput]


@dataclass
class ReadyLineageObject:
    name: str
    kind: str
    nodes: list[ReadyLineageNode]


@dataclass
class ReadyLineage:
    objects: list[ReadyLineageObject]


@dataclass
class Lineage:
    lineage: ReadyLineage
    raw_lineage: RawLineage | None
