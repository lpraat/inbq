import json
import os

import inbq

this_dir = os.path.dirname(__file__)

with open(os.path.join(this_dir, "catalog.json"), mode="r") as f:
    catalog = json.load(f)

with open(os.path.join(this_dir, "query.sql"), mode="r") as f:
    query = f.read()

pipeline = (
    inbq.Pipeline()
    .config(
        # If the `pipeline` is configured with `raise_exception_on_error=False`,
        # any error that occurs during parsing or lineage extraction is
        # captured and returned as a `inbq.PipelineError`
        raise_exception_on_error=False,
        # No effect with only one query (may provide a speedup with multiple queries)
        parallel=True,
    )
    .parse()
    .extract_lineage(catalog=catalog, include_raw=False)
)
sqls = [query]
pipeline_output = inbq.run_pipeline(sqls, pipeline=pipeline)

# This loop will iterate just once as we have only one query
for i, (ast, output_lineage) in enumerate(
    zip(pipeline_output.asts, pipeline_output.lineages)
):
    assert isinstance(ast, inbq.ast_nodes.Ast), (
        f"Could not parse query `{sqls[i][:20]}...` due to: {ast.error}"
    )

    print(f"{ast=}")

    assert isinstance(output_lineage, inbq.lineage.Lineage), (
        f"Could not extract lineage from query `{sqls[i][:20]}...` due to: {output_lineage.error}"
    )

    print("\nLineage:")
    for lin_obj in output_lineage.lineage.objects:
        print("Inputs:")
        for lin_node in lin_obj.nodes:
            print(
                f"{lin_obj.name}->{lin_node.name} <- {[f'{input_node.obj_name}->{input_node.node_name}' for input_node in lin_node.inputs]}"
            )

        print("\nSide inputs:")
        for lin_node in lin_obj.nodes:
            print(
                f"""{lin_obj.name}->{lin_node.name} <- {[f"{input_node.obj_name}->{input_node.node_name} @ {','.join(input_node.sides)}" for input_node in lin_node.side_inputs]}"""
            )

    print("\nReferenced columns:")
    for ref_obj in output_lineage.referenced_columns.objects:
        for ref_node in ref_obj.nodes:
            print(
                f"{ref_obj.name}->{ref_node.name} referenced in {ref_node.referenced_in}"
            )
