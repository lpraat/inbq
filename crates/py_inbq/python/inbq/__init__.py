from dataclasses import dataclass
from typing import Generic, Optional, TypeVar

from inbq import ast_nodes as ast_nodes
from inbq import lineage as lineage
from inbq._inbq import parse_sql as parse_sql
from inbq._inbq import parse_sql_to_dict as parse_sql_to_dict
from inbq._inbq import parse_sqls as parse_sqls
from inbq._inbq import parse_sqls_and_extract_lineage as parse_sqls_and_extract_lineage
from inbq._inbq import run_pipeline as run_pipeline

PipelineOutputType = TypeVar("PipelineOutputType")


@dataclass
class PipelineError:
    error: str


@dataclass
class PipelineParsingOutput:
    asts: list[ast_nodes.Ast | PipelineError]


@dataclass
class PipelineParsingLineageOutput:
    asts: list[ast_nodes.Ast | PipelineError]
    lineages: list[lineage.Lineage | PipelineError]


class Pipeline(Generic[PipelineOutputType]):
    _spec: dict

    def __init__(self, spec: Optional[dict] = None) -> None:
        self._spec = spec if spec is not None else {}

    def config(
        self, raise_exception_on_error: bool = False, parallel: bool = True
    ) -> "Pipeline[None]":
        if "config" in self._spec:
            raise ValueError("`config()` was already called.")
        new_spec = {**self._spec}
        new_spec["config"] = {
            "raise_exception_on_error": raise_exception_on_error,
            "parallel": parallel,
        }
        return Pipeline(spec=new_spec)

    def parse(self) -> "Pipeline[PipelineParsingOutput]":
        if "config" not in self._spec:
            raise ValueError("`config()` must be called before `parse()`.")
        if "parse" in self._spec:
            raise ValueError("`parse()` was already called.")
        new_spec = {**self._spec}
        new_spec["parse"] = {}
        return Pipeline(spec=new_spec)

    def extract_lineage(
        self, catalog: dict, include_raw: bool = False
    ) -> "Pipeline[PipelineParsingLineageOutput]":
        if "parse" not in self._spec:
            raise ValueError(
                "Extracting lineage requires parsing. Call `parse()` first."
            )
        if "extract_lineage" in self._spec:
            raise ValueError("`extract_lineage()` was already called.")
        new_spec = {**self._spec}
        new_spec["extract_lineage"] = {"catalog": catalog, "include_raw": include_raw}
        return Pipeline(spec=new_spec)

    @property
    def spec(self) -> dict:
        return self._spec

    def __repr__(self) -> str:
        return f"Pipeline(spec={self._spec!r})"
