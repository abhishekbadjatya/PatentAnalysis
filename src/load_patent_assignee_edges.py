import graphdb_client
import logging 
import json
import sys

logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] %(lineno)s %(message)s',)
g = graphdb_client.gc(host = 'http://localhost:8010')
    

def load_patent_assignee_edges():
    '''function to load the patent-assignee edges in graphen'''
    print ("Loading patent-assignee edges...\n")
    edge_file_path = '../data/cleaned_patent_vertices_edges.csv'
    has_header = 1
    column_delimiter = ','

    default_source_label = "PATENT"
    default_target_label = "ASSIGNEE"
    default_edge_label = 'EDGE'
    content_type = []
    edge_column_header_map = {
                "source_id": "patent_id",
                "target_id":"assignee_id",
                "properties":content_type
        }
    
    rc = g.load_table_edge(file_path = edge_file_path,
                      has_header = has_header, 
                      column_delimiter= column_delimiter, 
                      default_source_label = default_source_label, 
                      default_target_label = default_target_label, 
                      default_edge_label = default_edge_label, 
                      column_header_map = edge_column_header_map, 
                      column_number_map={},

                      data_row_start= -1, 
                      data_row_end=-1)
    print (rc)

def main(argv):
    try:
        if argv:
            graphName = argv[0]
            graphName = graphName.lower()
            rc = g.set_current_graph(graphName)
            print (rc)
            load_patent_assignee_edges()
        else:
            raise Exception("empty arg passed!")
    except Exception as e:
        print e
        exit(-1)

if __name__ == "__main__":
    main(sys.argv[1:])