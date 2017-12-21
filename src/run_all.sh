#!/bin/bash
graphname='patent-analysis'
python create_graph.py $graphname
python load_vertices.py $graphname
python load_patent_assignee_edges.py $graphname