import os
import pytest

import tomllib
from typing import Literal

from inbq import parse_sql, parse_sql_to_dict
from inbq.ast_nodes import Ast


@pytest.fixture()
def parsing_tests():
    tests_file = os.path.join(
        os.path.dirname(__file__), "../../../inbq/tests/parsing_tests.toml"
    )
    with open(tests_file, mode="rb") as f:
        return tomllib.load(f)["tests"]


def test_rs_instantation_matches_py_instantiation(
    parsing_tests: list[dict[Literal["sql"], str]],
):
    for test in parsing_tests:
        sql = test["sql"]
        rs_ast = parse_sql(sql)
        py_ast = Ast.from_dict(parse_sql_to_dict(sql))
        assert repr(rs_ast) == repr(py_ast)
