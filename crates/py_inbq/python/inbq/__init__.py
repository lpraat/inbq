from inbq._inbq import rs_parse_sql
from inbq.ast_nodes import *


def parse_sql(sql: str) -> Ast:
    """Parse a BigQuery SQL

    Args:
        sql (str): the sql to parse

    Returns:
        str: JSON string representation of the SQL abstract syntax tree
    """
    return Ast.from_json_str(rs_parse_sql(sql))
