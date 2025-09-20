from inbq.ast_nodes import Ast

def parse_sql(sql: str) -> Ast:
    """Parse a BigQuery SQL.

    Args:
        sql (str): the SQL to parse.

    Returns:
        inbq.Ast: SQL abstract syntax tree.
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
    sqls: list[str], catalog: dict, include_raw: bool = False
) -> tuple[list[Ast], list[dict]]:
    """Parse and extract lineage from one or more BigQuery SQLs.

    Args:
        sqls (list[str]): the SQLs to parse.
        catalog (dict): catalog information with schema.
        include_raw (bool, optional): whether to include raw lineage objects in the output. Defaults to False.

    Returns:
        tuple[list[Ast], list[dict]]: tuple containing a list of abstract syntax trees and a list of extracted lineages, one per input SQL.
    """
    ...
