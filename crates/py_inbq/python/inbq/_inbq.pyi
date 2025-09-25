from inbq import Pipeline, PipelineOutput
from inbq.ast_nodes import Ast
from inbq.lineage import Lineage

def parse_sql(sql: str) -> Ast:
    """Parse a BigQuery SQL.

    Args:
        sql (str): the SQL to parse.

    Returns:
        inbq.Ast: SQL abstract syntax tree.
    """
    ...

def parse_sqls(sqls: list[str], parallel: bool = True) -> list[Ast]:
    """Parse a list of BigQuery SQLs.

    Args:
        sqls (list[str]): the SQLs to parse.
        parallel (bool, optional): whether to parse SQLs in parallel. Defaults to False.

    Returns:
        list[inbq.Ast]: list of SQL abstract syntax trees.
    """
    ...

def parse_sql_to_dict(sql: str) -> dict:
    """Parse a BigQuery SQL.

    Args:
        sql (str): the SQL to parse.

    Returns:
        dict: dict representation of the SQL abstract syntax tree.
    """
    ...

def parse_sqls_and_extract_lineage(
    sqls: list[str],
    catalog: dict,
    include_raw: bool = False,
    parallel: bool = True,
) -> tuple[list[Ast], list[Lineage]]:
    """Parse and extract lineage from one or more BigQuery SQLs.

    Args:
        sqls (list[str]): the SQLs to parse.
        catalog (dict): catalog information with schema.
        include_raw (bool, optional): whether to include raw lineage objects in the output. Defaults to False.
        parallel (bool, optional): whether to process SQLs in parallel. Defaults to False.

    Returns:
        tuple[list[Ast], list[Lineage]]: tuple containing a list of abstract syntax trees and a list of extracted lineages, one per input SQL.
    """
    ...

def run_pipeline(sqls: list[str], pipeline: Pipeline) -> PipelineOutput:
    """Runs a configured Pipeline on a list of BigQuery SQLs.

    If the `pipeline` is configured with `raise_exception_on_error=False`, any error that occurs during
    parsing or lineage extraction is captured and returned as a `inbq.PipelineError` in the `inbq.PipelineOutput`.

    The current Pipeline supports a two‑step workflow: parsing the SQL into an AST followed by lineage extraction against a catalog.
    Future releases may allow additional stages to be chained in the same pipeline.

    Args:
        sqls (list[str]): the SQLs to process.
        pipeline (Pipeline): configured pipeline instance.

    Returns:
        `inbq.PipelineOutput`: pipeline output.
    """
