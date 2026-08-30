from dataclasses import dataclass


@dataclass(slots=True)
class RawLineageObject:
    id: int
    name: str
    kind: str
    nodes: list[int]


@dataclass(slots=True)
class RawLineageNode:
    id: int
    name: str
    source_object: int
    inputs: list[int]


@dataclass(slots=True)
class RawLineage:
    objects: list[RawLineageObject]
    lineage_nodes: list[RawLineageNode]
    output_lineage: list[int]


@dataclass(slots=True)
class ReadyLineageNodeInput:
    obj_name: str
    obj_kind: str
    node_name: str


@dataclass(slots=True)
class ReadyLineageNodeSideInput:
    obj_name: str
    obj_kind: str
    node_name: str
    sides: list[str]


@dataclass(slots=True)
class ReadyLineageNode:
    name: str
    type_: str
    inputs: list[ReadyLineageNodeInput]
    side_inputs: list[ReadyLineageNodeSideInput]


@dataclass(slots=True)
class ReadyLineageObject:
    name: str
    kind: str
    nodes: list[ReadyLineageNode]


@dataclass(slots=True)
class ReadyLineage:
    objects: list[ReadyLineageObject]


@dataclass(slots=True)
class ReferencedNode:
    name: str
    referenced_in: list[str]


@dataclass(slots=True)
class ReferencedObject:
    name: str
    kind: str
    nodes: list[ReferencedNode]


@dataclass(slots=True)
class ReferencedColumns:
    objects: list[ReferencedObject]


@dataclass(slots=True)
class Lineage:
    lineage: ReadyLineage
    raw_lineage: RawLineage | None
    referenced_columns: ReferencedColumns
