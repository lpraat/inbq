import pytest
from inbq import Pipeline


def test_wrong_calls_order_raises_error():
    # calling config twice
    with pytest.raises(ValueError):
        Pipeline().config().config()

    # calling parse twice
    with pytest.raises(ValueError):
        Pipeline().config().parse().extract_lineage(catalog={}).parse()

    # calling extract_lineage twice
    with pytest.raises(ValueError):
        Pipeline().config().parse().extract_lineage(catalog={}).extract_lineage(
            catalog={}
        )

    # calling parse before config
    with pytest.raises(ValueError):
        Pipeline().parse().config()

    # calling extract_lineage before parse
    with pytest.raises(ValueError):
        Pipeline().config().extract_lineage(catalog={}).parse()
