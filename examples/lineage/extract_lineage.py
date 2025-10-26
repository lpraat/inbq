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
    .config(raise_exception_on_error=False, parallel=True)
    .parse()
    .extract_lineage(catalog=catalog, include_raw=False)
)
pipeline_output = inbq.run_pipeline(sqls=[query], pipeline=pipeline)

for ast, output_lineage in zip(pipeline_output.asts, pipeline_output.lineages):
    print(f"{ast=}")
    print("Lineage:")
    for object in output_lineage.lineage.objects:
        for node in object.nodes:
            print(
                f"{object.name}->{node.name} <- {[f'{input_node.obj_name}->{input_node.node_name}' for input_node in node.input]}"
            )
