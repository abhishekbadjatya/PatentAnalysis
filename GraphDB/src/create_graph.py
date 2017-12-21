import graphdb_client
import logging 
import json
import sys

logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] %(lineno)s %(message)s',)
g = graphdb_client.gc(host = 'http://localhost:8010')

#Creating Graph
def main(argv):
    try:
        if argv:
            graphName = argv[0]
            graphName = graphName.lower()
            rc = g.create_graph(graph_name=graphName)
            print (rc)
        else:
            raise Exception("empty arg passed!")
    except Exception as e:
        print e
        exit(-1)

if __name__ == "__main__":
    main(sys.argv[1:])