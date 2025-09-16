from inbq.ast_nodes import Ast

def parse_sql(sql: str) -> Ast:
    """Parse a BigQuery SQL using the inbq Rust backend

    Args:
        sql (str): the sql to parse

    Returns:
        inbq.Ast: SQL abstract syntax tree
    """
    ...

def parse_sql_out_json(sql: str) -> Ast:
    """Parse a BigQuery SQL using the inbq Rust backend

    Args:
        sql (str): the sql to parse

    Returns:
        str: JSON string representation of the SQL abstract syntax tree
    """
    ...
