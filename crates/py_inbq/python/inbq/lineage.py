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
    inputs: list[int]


@dataclass
class RawLineage:
    objects: list[RawLineageObject]
    lineage_nodes: list[RawLineageNode]
    output_lineage: list[int]


@dataclass
class ReadyLineageNodeInput:
    obj_name: str
    obj_kind: str
    node_name: str


@dataclass
class ReadyLineageNodeSideInput:
    obj_name: str
    obj_kind: str
    node_name: str
    sides: list[str]


@dataclass
class ReadyLineageNode:
    name: str
    type_: str
    inputs: list[ReadyLineageNodeInput]
    side_inputs: list[ReadyLineageNodeSideInput]


@dataclass
class ReadyLineageObject:
    name: str
    kind: str
    nodes: list[ReadyLineageNode]


@dataclass
class ReadyLineage:
    objects: list[ReadyLineageObject]


@dataclass
class ReferencedNode:
    name: str
    referenced_in: list[str]


@dataclass
class ReferencedObject:
    name: str
    kind: str
    nodes: list[ReferencedNode]


@dataclass
class ReferencedColumns:
    objects: list[ReferencedObject]


@dataclass
class Lineage:
    lineage: ReadyLineage
    raw_lineage: RawLineage | None
    referenced_columns: ReferencedColumns
