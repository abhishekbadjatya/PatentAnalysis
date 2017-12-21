import graphdb_client
import logging 
import json
import sys

logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] %(lineno)s %(message)s',)
g = graphdb_client.gc(host = 'http://localhost:8010')
    

def load_patent_vertex():
    '''function to load the patent nodes in graphen'''
    print ("\nLoading patent vertices...\n")
    has_header = 1
    column_delimiter = ','
    default_vertex_label = "PATENT"
    content_type = [{"year": ["year", "INT"]},{"month": ["month", "INT"]},{"day": ["day", "INT"]}]

    column_header_map = {
                "vertex_id": "id",
                "properties":content_type
            }

    #filtering only for these many years
    startYear = 2008
    endYear = 2017
    for year in range(startYear,endYear+1):
        vertex_file_path = '../data/cleaned_patent_vertices/merged_vertices_'+str(year)+'.csv'
        print (vertex_file_path)
        rc = g.load_table_vertex(file_path = vertex_file_path,
                        has_header = has_header,
                        column_delimiter = column_delimiter, 
                        default_vertex_label = default_vertex_label,  
                        column_header_map = column_header_map, 
                        column_number_map=[{}],
                        content_type = content_type,
                        data_row_start = -1, 
                        data_row_end = -1)
        print (rc)

def load_assignee_vertex():
    '''function to load the assignee nodes in graphen'''
    print ("\nLoading assignee vertices...\n")
    vertex_file_path='../data/cleaned_assignee_vertices.csv'
    has_header = 1
    column_delimiter = ','
    default_vertex_label = "ASSIGNEE"

    content_type = [{"Assignee": ["Assignee", "STRING"]}]

    column_header_map = {
                "vertex_id": "id",
                "properties":content_type
            }


    rc = g.load_table_vertex(file_path = vertex_file_path,
                        has_header = has_header,
                        column_delimiter = column_delimiter, 
                        default_vertex_label = default_vertex_label,  
                        column_header_map = column_header_map, 
                        column_number_map=[{}],
                        content_type = content_type,
                        data_row_start = -1, 
                        data_row_end = -1)
    print (rc)


def main(argv):
    try:
        if argv:
            graphName = argv[0]
            graphName = graphName.lower()
            rc = g.set_current_graph(graphName)
            print (rc)
            load_patent_vertex()
            load_assignee_vertex()
        else:
            raise Exception("empty arg passed!")
    except Exception as e:
        print e
        exit(-1)

if __name__ == "__main__":
    main(sys.argv[1:])