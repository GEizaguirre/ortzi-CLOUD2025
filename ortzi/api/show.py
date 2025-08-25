import os
import pydot
import xml.etree.ElementTree as ET
from ortzi.logical.descriptors import (
    ReadDescriptor,
    WriteDescriptor,
    ExchangeReadDescriptor,
    ExchangeWriteDescriptor
)
from ortzi import DataFrame
from ortzi.utils import Colors

FILENAME = "dag"
FONTNAME = "Liberation Sans"


def change_font(full_filename: str):

    # Load the SVG file
    tree = ET.parse(full_filename)
    root = tree.getroot()

    # Change the value of all font-family attributes to "fontname"
    for element in root.iter():
        if "font-family" in element.attrib:
            element.attrib["font-family"] = FONTNAME

    # Save the modified SVG file
    tree.write(full_filename)


def generate_node(name, color) -> pydot.Node:

    node = pydot.Node(name, shape="box", fillcolor=color, style="filled")
    return node


def gen_filename() -> str:
    version = 0
    base_filename = "dag"
    while os.path.exists(
        f"{base_filename}{'' if version == 0 else '-' + str(version)}.svg"
    ):
        version += 1
    return f"{base_filename}{'' if version == 0 else '-' + str(version)}"


def show(df: DataFrame) -> str:

    logical_plan = df.logical_plan
    stages = logical_plan.stages

    dag = pydot.Dot(graph_type="digraph")
    stage_nodes = dict()

    exchange_creator_nodes = dict()

    for stage_id, stage in stages.items():

        stage_nodes[stage_id] = dict()

        subg = pydot.Cluster(
            str(stage_id),
            label="Stage %d" % (stage_id),
            labeljust="l"
        )
        dag.add_subgraph(subg)

        op = stage.input
        if isinstance(op, ReadDescriptor):

            node_color = Colors.READ.value
            prev_node = generate_node(op.name, node_color)

        elif isinstance(op, ExchangeReadDescriptor):

            node_color = Colors.READ_EXCHANGE.value
            prev_node = generate_node(op.name, node_color)
            for exchange_id in op.source_exchange_ids:
                dag.add_edge(pydot.Edge(
                    exchange_creator_nodes[exchange_id],
                    prev_node,
                    penwidth=2,
                    arrowsize=1.5
                ))

        subg.add_node(prev_node)

        if stage.join is not None:
            op = stage.join
            node_color = Colors.JOIN.value
            node = generate_node(op.name, node_color)
            subg.add_node(node)
            dag.add_edge(pydot.Edge(prev_node, node))
            prev_node = node

        for op in stage.transformations:
            
            node_color = Colors.TRANSFORMATION.value
            node = generate_node(op.name, node_color)
            subg.add_node(node)
            dag.add_edge(pydot.Edge(prev_node, node))
            prev_node = node

        if stage.partition is not None:
            op = stage.partition
            node_color = Colors.PARTITION.value
            node = generate_node(op.name, node_color)
            subg.add_node(node)
            dag.add_edge(pydot.Edge(prev_node, node))
            prev_node = node
        
        op = stage.output
        if op is not None:
            if isinstance(op, WriteDescriptor):
                            
                node_color = Colors.WRITE.value
                node = generate_node(op.name, node_color)

            elif isinstance(op, ExchangeWriteDescriptor):
                            
                node_color = Colors.WRITE_EXCHANGE.value
                node = generate_node(op.name, node_color)
                exchange_creator_nodes[op.exchange_id] = node

            
            subg.add_node(node)
            dag.add_edge(pydot.Edge(prev_node, node))
        
    
    fformat = "svg"
    full_filename = f"{gen_filename()}.{fformat}"
    dag.write(full_filename, format=fformat)
      
    change_font(full_filename) # Display the modified SVG file
    
    return full_filename
    
    
    
    
        
        
        
        