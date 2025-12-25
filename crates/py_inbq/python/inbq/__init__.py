from dataclasses import dataclass
from typing import Optional

from inbq import ast_nodes as ast_nodes
from inbq import lineage as lineage
from inbq._inbq import parse_sql as parse_sql
from inbq._inbq import parse_sqls as parse_sqls
from inbq._inbq import parse_sql_to_dict as parse_sql_to_dict
from inbq._inbq import parse_sqls_and_extract_lineage as parse_sqls_and_extract_lineage
from inbq._inbq import run_pipeline as run_pipeline


class Pipeline:
    _spec: dict

    def __init__(self, spec: Optional[dict] = None) -> None:
        self._spec = spec if spec is not None else {}

    def config(
        self, raise_exception_on_error: bool = False, parallel: bool = True
    ) -> "Pipeline":
        new_spec = {**self._spec}
        new_spec["config"] = {
            "raise_exception_on_error": raise_exception_on_error,
            "parallel": parallel,
        }
        return Pipeline(spec=new_spec)

    def parse(self) -> "Pipeline":
        new_spec = {**self._spec}
        new_spec["parse"] = {}
        return Pipeline(spec=new_spec)

    def extract_lineage(self, catalog: dict, include_raw: bool = False) -> "Pipeline":
        if "parse" not in self._spec:
            raise ValueError(
                "Extracting lineage requires parsing. Call `parse()` first."
            )
        new_spec = {**self._spec}
        new_spec["extract_lineage"] = {"catalog": catalog, "include_raw": include_raw}
        return Pipeline(spec=new_spec)

    @property
    def spec(self) -> dict:
        return self._spec

    def __repr__(self) -> str:
        return f"Pipeline(spec={self._spec!r})"


@dataclass
class PipelineError:
    error: str


@dataclass
class PipelineOutput:
    asts: list[ast_nodes.Ast | PipelineError]
    lineages: Optional[list[dict | PipelineError]]
