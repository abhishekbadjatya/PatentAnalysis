
import sys
import requests
import json
import csv
import re
import logging

# from errorList import null_string_error
logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] %(lineno)s %(message)s')

username = ''
def query(url):
    response = requests.get(url,data=None)
    # result = json.loads(response.content)
    # result = json.dumps(result, indent = 4, sort_keys =True)
    result = response.content
    return result

def post_query(url,data):
    response = requests.post(url, data=data)
    # result = json.loads(response.content)
    # result = json.dumps(result, indent=4, sort_keys=True)
    result = response.content
    return result

def post_query_files(url, data, file_path):
    files = {'file': open(file_path, 'r')}
    response = requests.post(url, data=data, files=files)
    # result = json.loads(response.content)
    # result = json.dumps(result, indent=4, sort_keys=True)
    result = response.content
    return result

def delete_query(url, data):
    response = requests.delete(url, json=data)
    result = json.loads(response.content)
    result = json.dumps(result, indent=4, sort_keys=True)
    # result = response
    return result

def url_assemble(url,parameter):
    if len(parameter) is 1:
        url += parameter[0]
    else:
        for i in range(0, len(parameter)):
            if i != len(parameter) - 1:
                url += parameter[i] + ';'
            else:
                url += parameter[i]
    return url 

def url_dict_assemble(url,parameter):
    l = len(parameter)
    if l > 1:
        i = 0
        for key, value in parameter.items():
            if i != l - 1:
                url += key + '=' + value + '&'
                i += 1
            else:
                url += key + '=' + value
    else:
        for key, value in parameter.items():
            url += key + '=' + value
    return url

def get_neighbor_inout_url(url, edge_label, neighbor_label):
    # Get a vertex's outgoing neighbors
    if not edge_label and not neighbor_label:
        pass
    # Get a vertex's outgoing neighbors of one or more specific vertex labels
    elif neighbor_label and not edge_label:
        url += 'vertex_label='
        url = url_assemble(url,neighbor_label)
    # Get a vertex's outgoing neighbors along edges of one or more specific edge labels
    elif edge_label and not neighbor_label:
        url += 'edge_label='
        url = url_assemble(url,edge_label)
    # Get a vertex's outgoing neighbors of one or more specific vertex labels along edges of one or more specific edge labels
    elif edge_label and neighbor_label:
        url += 'vertex_label='
        url = url_assemble(url,neighbor_label)
        url += '&edge_label='
        url = url_assemble(url,edge_label)
    return url

class gc:
    def __init__(self, host):
        self.vertex_prop_name_list = []
        self.graph_name = ''
        self.root_url = host

# Graph Management
    def list_graphs(self,opened = False):
        if opened is False:
            url = root_url + '/graphs?opened=0'
            print query(url)
        elif opened is True:
            url = root_url + '/graphs?opened=1'
            print query(url)

    def print_graph(self):
        url = self.root_url + '/graphs/' + self.graph_name
        return query(url)

    def create_graph(self, graph_name, graph_type = 0, schema_path = '', schema_url = ''):
        url = self.root_url + '/graphs/' + graph_name 
        config_param = {}
        if schema_path is not '':
            url += '/schema'
            if 'server://' in schema_path:
                data = {
                    'graph_type':graph_type,
                    'schema_path': schema_path
                }
                config_param['param'] = data 
                return post_query(url, config_param)
            else:
                data = {
                'graph_type':graph_type
                }
                config_param['param'] = json.dumps(data)
                return post_query_files(url,config_param,schema_path)
                
        elif schema_url is not '':
            url += '/create'
            data = {
                'graph_type':graph_type,
                'schema_url':schema_url
                }
        else:
            url += '/create'
            data = {}
            data['graph_type'] = graph_type
        config_param['param'] = json.dumps(data)
        return post_query(url, config_param)

    def close_graph(self, graph_name):
        url = self.root_url + "/graphs/" + self.graph_name + '/close'
        print post_query(url, None)

    def close_graphs(self):
        url = self.root_url + '/graphs/close'
        print post_query(url,None)

    def delete_graph(self,graph_name):
        url = self.root_url + '/graphs/' + self.graph_name
        return delete_query(url, None)

    def set_current_graph(self,graph_name):
        self.graph_name = graph_name
        return "current graph set to " + self.graph_name

    def get_current_graph(self):
        return "current graph is " + self.graph_name
############
############ add/update/delete graph data #############
############
    def set_schema(self,file_path='', file_url=''):
        if file_path is not '':
            if 'local://' not in file_path:
                data = {'file_path': file_path}
            else:
                files = {'file':open(file_path,'r')}
                print "open file in loacal"
                return

        if file_url is not '':
            data = {'file_url':file_url}

        config_param['param'] = json.dumps(data)
        url = self.root_url + '/' + self.graph_name + '/schema'
        return post_query(url, config_param)

    def set_user(self,user):
        global username
        username = user
        user_url = root_url + "/graphs/" + username
        response = requests.get(user_url)
        return response.content

    def load_table_vertex(self, column_delimiter, has_header, default_vertex_label, column_header_map, column_number_map,
                          file_path = '', file_url ='', content_type = '', data_row_start = 0, data_row_end = 3):
        config_param = {}
        data = {}
        url = self.root_url + "/graphs/" + self.graph_name + '/table/vertex'
        if file_path is not '':
            if 'server://' in file_path:
                if has_header == 1:
                    if column_header_map.has_key('vertex_id'):
                        data = {
                            "file_path": file_path[8:],
                            "column_delimiter": column_delimiter,
                            "has_header": has_header,
                            'default_vertex_label': default_vertex_label,
                            'column_header_map': column_header_map,
                            'content_type': content_type,
                            'data_row_start': data_row_start,
                            'data_row_end': data_row_end
                        }
                    else:
                        print "column header for vertex_id is required"
                else:
                    if column_number_map.has_key('vertex_id'):
                        data = {
                            "file_path": file_path,
                            "column_delimiter": column_delimiter,
                            "has_header": has_header,
                            'default_vertex_label': default_vertex_label,
                            'column_number_map': column_number_map,
                            'content_type': content_type,
                            'data_row_start': data_row_start,
                            'data_row_end': data_row_end
                        }
                config_param['param'] = json.dumps(data)
                return post_query(url, config_param)
                
            else:
                if has_header == 1:
                    if column_header_map.has_key('vertex_id'):
                        data = {
                            "column_delimiter": column_delimiter,
                            "has_header": has_header,
                            "default_vertex_label": default_vertex_label,
                            "column_header_map": column_header_map,
                            "content_type": content_type,
                            "data_row_start": data_row_start,
                            "data_row_end": data_row_end
                        }
                        config_param['param'] = json.dumps(data)
                        return post_query_files(url, config_param, file_path)
                        
                    else:
                        print "column header for vertex_id is required"
                else:
                    if column_number_map.has_key('vertex_id'):
                        data = {
                            "column_delimiter": column_delimiter,
                            "has_header": has_header,
                            'default_vertex_label': default_vertex_label,
                            'column_number_map': column_number_map,
                            'content_type': content_type,
                            'data_row_start': data_row_start,
                            'data_row_end': data_row_end

                        }
                        config_param['param'] = json.dumps(data)
                        return post_query_files(url, config_param, file_path)
                       
                    else:
                        print "column number for vertex_id is required"

        elif file_url is not '':
            if has_header == 1:
                if column_header_map.has_key('vertex_id'):
                    data = {
                        "file_url": file_url,
                        "column_delimiter": column_delimiter,
                        "has_header": has_header,
                        'default_vertex_label': default_vertex_label,
                        'column_header_map': column_header_map,
                        'content_type': content_type,
                        'data_row_start': data_row_start,
                        'data_row_end': data_row_end
                    }
                else:
                    print "column header for vertex_id is required"
            else:
                if column_number_map.has_key('vertex_id'):
                    data = {
                        "file_url": file_url,
                        "column_delimiter": column_delimiter,
                        "has_header": has_header,
                        'default_vertex_label': default_vertex_label,
                        'column_number_map': column_number_map,
                        'content_type': content_type,
                        'data_row_start': data_row_start,
                        'data_row_end': data_row_end
                    }
            config_param['param'] = json.dumps(data)
            return post_query(url, config_param)

        else:
            pass
   


    def load_table_edge(self,column_delimiter, has_header, default_source_label, default_target_label, default_edge_label, column_header_map, column_number_map,
                        file_path='', file_url='', content_type='', data_row_start=0, data_row_end=0 ): 
        config_param ={}
        data = {}
        url = self.root_url + "/graphs/" + self.graph_name + '/table/edge'
        if file_path is not '':
            if 'server://' in file_path:
                if has_header == 1:
                    if column_header_map.has_key('source_id') and column_header_map.has_key('target_id'):
                        data = {
                            "file_path": file_path[8:],
                            "column_delimiter": column_delimiter,
                            "has_header": has_header,
                            'default_source_label': default_source_label,
                            'default_target_label': default_target_label,
                            'default_edge_label':default_edge_label,
                            'column_header_map': column_header_map,
                            'content_type': content_type,
                            'data_row_start': data_row_start,
                            'data_row_end': data_row_end
                        }
                    else:
                        print "column header for vertex_id is required"
                else:
                    if column_number_map.has_key('source_id'):
                        data = {
                            "file_path": file_path[8:],
                            "column_delimiter": column_delimiter,
                            "has_header": has_header,
                            'default_source_label': default_source_label,
                            'default_target_label': default_target_label,
                            'default_edge_label': default_edge_label,
                            'column_number_map': column_number_map,
                            'content_type': content_type,
                            'data_row_start': data_row_start,
                            'data_row_end': data_row_end
                        }
                config_param['param'] = json.dumps(data)
                return post_query(url, config_param)
            else:
                if has_header == 1:
                    if column_header_map.has_key('source_id') and column_header_map.has_key('target_id'):
                        data = {
                            "column_delimiter": column_delimiter,
                            "has_header": has_header,
                            'default_source_label': default_source_label,
                            'default_target_label': default_target_label,
                            'default_edge_label':default_edge_label,
                            'column_header_map': column_header_map,
                            'content_type': content_type,
                            'data_row_start': data_row_start,
                            'data_row_end': data_row_end
                        }
                        config_param['param'] = json.dumps(data)
                        return post_query_files(url, config_param, file_path)
                    else:
                        print "column header for vertex_id is required"
                else:
                    if column_number_map.has_key('source_id') :
                        data = {
                            "column_delimiter": column_delimiter,
                            "has_header": has_header,
                            'default_source_label': default_source_label,
                            'default_target_label': default_target_label,
                            'default_edge_label': default_edge_label,
                            'column_number_map': column_number_map,
                            'content_type': content_type,
                            'data_row_start': data_row_start,
                            'data_row_end': data_row_end
                        }
                        config_param['param'] = json.dumps(data)
                        return post_query_files(url, config_param, file_path)

        elif file_url is not '':
            if has_header == 1:
                if column_header_map.has_key('source_id')and column_header_map.has_key('target_id'):
                    data = {
                        "file_url": file_url,
                        "column_delimiter": column_delimiter,
                        "has_header": has_header,
                        'default_source_label': default_source_label,
                        'default_target_label': default_target_label,
                        'default_edge_label': default_edge_label,
                        'column_header_map': column_header_map,
                        'content_type': content_type,
                        'data_row_start': data_row_start,
                        'data_row_end': data_row_end
                    }
                else:
                    print "column header for vertex_id is required"
            else:
                if column_number_map.has_key('source_id'):
                    data = {
                        "file_url": file_url,
                        "column_delimiter": column_delimiter,
                        "has_header": has_header,
                        'default_source_label': default_source_label,
                        'default_target_label': default_target_label,
                        'default_edge_label': default_edge_label,
                        'column_number_map': column_number_map,
                        'content_type': content_type,
                        'data_row_start': data_row_start,
                        'data_row_end': data_row_end
                    }
            config_param['param'] = json.dumps(data)
            return post_query(url, config_param)
        else:
            pass

    def add_vertex(self, vertex_id, vertex_label, prop_dict=[]):
        config_param = {}
        data = {}
        data['properties'] = prop_dict
        config_param['param'] = json.dumps(data)
        url = self.root_url + '/graphs/' + self.graph_name + '/vertices/' + vertex_id + '/' + vertex_label + '/create'
        return post_query(url, config_param)
        
    def add_edge(self, source_id, source_label, target_id, target_label, edge_label, prop_dict=[]):
        config_param = {}
        data = {}
        data['properties'] = prop_dict
        config_param['param'] = json.dumps(data)
        url = self.root_url + '/graphs/' + self.graph_name + '/edges/' + source_id + '/' + source_label + '/' + target_id + '/' + target_label + '/' + edge_label + '/create'
        return post_query(url,config_param)

    def update_vertex(self,vertex_id, vertex_label, prop):
        config_param = {}
        data = {}
        data['properties'] = prop
        config_param['param'] = json.dumps(data)
        url = self.root_url + '/graphs/' + self.graph_name + '/vertices/' + vertex_id + '/' + vertex_label + '/update'
        return post_query(url, config_param)

    def update_edge(self, source_id, source_label, target_id, target_label, prop, edge_id ='', edge_label=[]):
         url = self.root_url
         if edge_id is not '' and not edge_label:
            url = root_url + '/graphs/' + self.graph_name + '/edges/' + source_id + '/' + source_label + '/' + target_id + '/' + target_label + '/' \
                  + edge_id + '/' + edge_label + '/update'
        # Update all existing edges between two existing vertices
         elif edge_id is '' and not edge_label:
            url = root_url + '/graphs/' + self.graph_name + '/edges/' +  source_id + '/' + source_label + '/' + target_id + '/' + target_label + '/update'
        # Update all existing edges of a specific label between two existing vertices
         elif edge_id is not '' and edge_label:
            url = root_url + '/graphs/' + self.graph_name + '/edges/' + source_id + '/' + source_label + '/' + target_id + '/' + target_label + '/' \
                 + edge_id + '/'
            url = url_assemble(url, edge_label)
            url = url + '/update' 
         config_param = {}
         data = {}
         data['properties'] = prop
         return post_query(url, config_param)

    def delete_vertex(self, vertex_label, vertex_id=''):
        url = self.root_url + '/graphs/' + self.graph_name + '/vertices/' + vertex_id
        if isinstance(vertex_label, basestring): 
            url += vertex_label
        elif isinstance(vertex_label, list):
            url = url_assemble(url, vertex_label)
        return delete_query(url, None)

    def delete_edge(self, source_id, source_label, target_id, target_label, edge_id='', edge_label=[]):
        # Delete all edges between two existing vertices
        if edge_id is '' and not edge_label:
            url = self.root_url + '/graphs/' + self.graph_name + '/edges/' + source_id + '/' + source_label + '/' + target_id + '/' + target_label
        # Delete a specific edge between two existing vertices
        if edge_id is not '' and edge_label:
            url = self.root_url + '/graphs/' + self.graph_name + '/edges/' + source_id + '/' + source_label + '/' + target_id + '/' + target_label + '/' + edge_id + \
                   '/' 
            url = url_assemble(url, edge_label)
        if edge_id is '' and edge_label:
            url =  self.root_url + '/graphs/' + self.graph_name + '/edges/' + source_id + '/' + source_label + '/' + target_id + '/' + target_label + '/'
            url = url_assemble(url, edge_label)
        return delete_query(url, None)

    def delete_vprop(self,vertex_id, vertex_label, prop):
         url = self.root_url + '/graphs/' + self.graph_name + '/vertices/' + vertex_id + '/' + vertex_label + '/properties'
         config_param = {}
         data = {}
         data['properties'] = prop_dict
         config_param['param'] = json.dumps(data)
         return delete_query(url, config_param)

    def delete_eprop(self, source_id, source_label, target_id, target_label, prop, edge_id = '', edge_label = []):
         if source_id is '' or source_label is '' or target_id is '' or not target_label:
             return 'null argument/s passed to function call'
         if edge_id is '' and not edge_label:
             url = root_url + '/graphs/' + self.graph_name + '/edges/' + source_id + '/' + source_label + '/' + target_id + '/' + target_label + '/properties'
         elif edge_id is not '' and edge_label:
             url = self.root_url + '/graphs/' + self.graph_name + '/edges/' + source_id + '/' + source_label + '/' + target_id + '/' + target_label + '/' + edge_id + '/'
             url = url_assemble(url, edge_label)
             url += '/properties'
         elif edge_id is '' and edge_label:
            # Delete specific properties of all edges of a specific label between two existing vertices
             url = self.root_url + '/graphs/' + self.graph_name + '/edges/' + source_id + '/' + source_label + '/' + target_id + '/' + target_label + '/' 
             url = url_assemble(url, edge_label)
             url += '/properties'
         config_param = {}
         data = {}
         data['properties'] = prop_dict
         config_param['param'] = json.dumps(data)
         return delete_query(url, config_param)

#############
##############  Get Graph Data ################
#############    
    def get_graph(self):
        url = self.root_url + "/graphs/whole/" + username
        data = None
        response = requests.post(url,data=data)
        result = json.loads(response.content)
        result = json.dumps(result, indent = 4, sort_keys =True)
        return result

    def get_schema(self):
        url = self.root_url + '/graphs/' + self.graph_name + '/schema'
        return query(url)

    # Get a vertex given id and label
    def get_vertex(self, vertex_label, vertex_id = '', prop=[]):
        # Get a vertex given id and label
         if isinstance(vertex_label, basestring):
             url = self.root_url + "/graphs/" +  self.graph_name  + "/vertices/" + vertex_id + '/' + vertex_label
        # Get all vertices given one or more labels
         elif isinstance(vertex_label, list):
             url =  self.root_url + "/graphs/" + self.graph_name + "/vertices/"
             url = url_assemble(url, vertex_label)
         if prop:
                url += '?prop='
                url = url_assemble(url,prop)  
         return query(url)

    def get_edge(self, source_id, source_label, target_id, target_label, edge_id = '', edge_label = [], prop=[]):
        url = self.root_url + '/graphs/' + self.graph_name + '/edges/' + source_id + '/' + source_label + '/' + target_id + '/' + target_label
        # Get all edges between two vertices
        # Get a specific edge between two vertices
        if edge_id is not '' and edge_label:
            url = url + '/' + edge_id + '/' 
            url = url_assemble(url,edge_label)
        # Get all edges of one or more specific labels between two vertices
        elif edge_id is '' and edge_label:
            url += '/'
            url = url_assemble(url,edge_label)
        if prop:
                url += '?prop='
                url = url_assemble(url,prop)  
        return query(url)

    def get_edge_count(self, source_id, source_label, target_id, target_label, edge_label = '' ):
         if source_id is '' or source_label is '' or target_id is '' or target_label is '':
             return null_string_error

         url = self.root_url + '/graphs/' + self.graph_name + '/edges/' + source_id + '/' + source_label + '/' + target_id + \
               '/' + target_label
        # Get all edges between two vertices
         if edge_label is list :
            url = url + '/count'
        # Get all edges of one or more specific labels between two vertices
         elif edge_id is '' and isinstance(edge_label, list):
             url = url + '/'
             url = url_assemble(url, edge_label)
             url = url + '/count'
         return query(url)

    def get_edge_out(self, vertex_id, vertex_label, edge_label=[]):
        # Get a vertex's outgoing edges
        url = self.root_url + '/graphs/' + self.graph_name + '/vertices/' + vertex_id + '/' + vertex_label + '/outE'
        # Get a vertex's outgoing edges of one or more specific edge labels
        if edge_label:
            url += '?edge_label='
            url = url_assemble(url,edge_label)
        return query(url)

    def get_edge_out_count(self, vertex_id, vertex_label, edge_label=[]):
        # Get a vertex's outgoing edges
        url = self.root_url + '/graphs/' + self.graph_name + '/vertices/' + vertex_id + '/' + vertex_label + '/outE/count'
        # Get a vertex's outgoing edges of one or more specific edge labels
        if edge_label:
            url += '?edge_label='
            url = url_assemble(url,edge_label)
        return query(url)

    def get_edge_in(self,vertex_id, vertex_label, edge_label=[]):
        if vertex_id is '' or vertex_label is '':
             return null_string_error
         # Get a vertex's incoming edges
        url = self.root_url + '/graphs/' + self.graph_name + '/vertices/' + vertex_id + '/' + vertex_label + '/inE'
         # Get a vertex's incoming edges of one or more specific edge labels
        if edge_label:
            url += '?edge_label='
            url = url_assemble(url,edge_label)
        return query(url)

    def get_edge_in_count(self,vertex_id, vertex_label, edge_label=[]):
        # Get a vertex's incoming edges
        url = self.root_url + '/graphs/' + self.graph_name + '/vertices/' + vertex_id + '/' + vertex_label + '/inE/count'
        # Get a vertex's incoming edges of one or more specific edge labels
        if edge_label:
            url += '?edge_label='
            url = url_assemble(url,edge_label)
        return query(url)

    def get_neighbor_out(self, vertex_id, vertex_label, edge_label =[], neighbor_label = [], distinct = False):
        url = self.root_url + '/graphs/' + self.graph_name + '/vertices/' + vertex_id + '/' + vertex_label + '/outV?'
        url = get_neighbor_inOut_url(url, edge_label, neighbor_label)  
        return query(url)

    def get_neighbor_out_count(self, vertex_id, vertex_label, edge_label =[], neighbor_label = [], distinct = False):
        url = self.root_url + '/graphs/' + self.graph_name + '/vertices/' + vertex_id + '/' + vertex_label + '/outV/count?'
        url = get_neighbor_inOut_url(url, edge_label, neighbor_label)
        return query(url)

    def get_neighbor_in(self, vertex_id, vertex_label, edge_label =[], neighbor_label = [], distinct = False):
        url = self.root_url + '/graphs/' + self.graph_name + '/vertices/' + vertex_id + '/' + vertex_label + '/inV?'
        url = get_neighbor_inOut_url(url, edge_label, neighbor_label)
        return query(url)

    def get_neighbor_in_count(self, vertex_id, vertex_label, edge_label=[], neighbor_label=[], distinct=False):
        url = self.root_url + '/graphs/' + self.graph_name + '/vertices/' + vertex_id + '/' + vertex_label + '/inV/count?'
        url = get_neighbor_inOut_url(url, edge_label, neighbor_label)
        return query(url)

# Search Graph
    #Find vertices that satisfy given search criteria
    def search_vertex(self, q):
        config_param = {}
        url = self.root_url + '/graphs/' + self.graph_name + '/vertices/search'
        if isinstance(q, basestring):
            config_param["param"] = q 
            return post_query(url, config_param)
        if type(q) is dict:
            url += '?'
            url = url_dict_assemble(url,q)
            return query(url)

    def search_edge(self,q):
        config_param = {}
        url = self.root_url + '/graphs/' + self.graph_name + '/edges/search'
        if isinstance(q, basestring):
            config_param["param"] = q 
            return post_query(url, config_param)
        if isinstance(q, dict):
            url += '?'
            url = url_dict_assemble(url,q)
            return query(url)
# 
######## Graph Analysis ##########
#
# 
    # Get total number of vertices
    def get_num_vertex(self, vertex_label=[]):
        url = self.root_url + '/graphs/' + self.graph_name + '/vertices/count'
        if vertex_label:
            url += '?label='
            url = url_assemble(url,vertex_label)
        return query(url)

    # Get total number of edges
    def get_num_edge(self, edge_label=[]):
        url = self.root_url + '/graphs/' + self.graph_name + '/edges/count'
        if edge_label:
            url += '?/label'
            url = url_assemble(url,edge_label)
        return query(url)

    # Get the egonet of a vertex
    def get_egonet(self, vertex_id, vertex_label, depth, edge_label = []):
        url = self.root_url + '/graphs/' + self.graph_name + '/analytics/egonet'
        config_param = {}
        data = {}
        data['vertex_ego'] = []
        if len(vertex_id) == len(vertex_label):
            for i in range(0, len(vertex_id)):
                field = {}
                field['id'] = vertex_id[i]
                field['label'] = vertex_label[i]
                data['vertex_ego'].append(field)
        data['edge_labels'] = edge_label
        data['depth'] = depth
        config_param['param'] = json.dumps(data)
        logging.debug(config_param)    
        return post_query(url,config_param)

    def get_subgraph(self, vertex_ids, vertex_labels, edge_labels):
        url = self.root_url + '/graphs/' + self.graph_name + '/analytics/subgraph' 
        config_param = {}
        data = {}
        data['vertices'] = []
        data['edge_labels'] = edge_labels
        if len(vertex_ids) == len(vertex_labels):
            for i in range(0, len(vertex_ids)):
                field = {}
                field['id'] = vertex_ids[i]
                field['label'] = vertex_labels[i]
                data['vertices'].append(field)
        config_param['param'] = json.dumps(data)
        logging.debug(config_param)    
        return post_query(url,config_param)

    def get_path(self, source_id, source_label, target_id, target_label, edge_labels, depth):
        url = self.root_url + '/graphs/' + self.graph_name + '/analytics/path'
        config_param = {}
        data = {}
        data['vertex_source'] = {}
        data['vertex_source']['id'] = source_id
        data['vertex_source']['label'] = source_label
        data['vertex_target'] = {}
        data['vertex_target']['id'] = target_id
        data['vertex_target']['label'] = target_label
        data['depth'] = depth
        data['edge_labels'] = edge_labels
        config_param['param'] = json.dumps(data)
        logging.debug(config_param)
        return post_query(url, config_param)

    def search_vertex_by_network_prop(self,query):
        url = self.root_url + '/graphs/' + self.graph_name + '/vertices/search_by_network_prop'
        config_param = {}
        data = {}
        data['query'] = query
        config_param['param'] = json.dumps(data) 
        logging.debug(config_param)
        return post_query(url,config_param)   


    def get_vertex_with_id(self, vertex_id):
        url = self.root_url + "/graphs/vertex/id/" + vertex_id + '/' + username
        print query(url)

    def get_outgoing_vertex(self, vertex_id):
        # print vertex_id
        url = self.root_url + "/graphs/vertex/outgoing/" + vertex_id + '/' + username
        print query(url)

    def get_incoming_vertex(self, vertex_id):
        url = self.root_url + "/graphs/vertex/incoming/" + vertex_id + '/' + username
        print query(url)

    def get_in_out_vertex(self, vertex_id):
        url = self.root_url + "/graphs/vertex/all/" + vertex_id + '/' + username
        print query(url)

    def get_outgoing_edge(self, vertex_id):
        url = self.root_url + "/graphs/edge/outgoing/" + vertex_id + '/' + username
        print query(url)

    def get_incoming_edge(self, vertex_id):
        url = self.root_url + "/graphs/edge/incoming/" + vertex_id + '/' + username
        print query(url)

    def get_in_out_edge(self, vertex_id):
        url = self.root_url + "/graphs/edge/all/" + vertex_id + '/' + username
        print query(url)

    def get_vertex_property(self, prop_name, prop_value):
        url = root_url + "/graphs/property/vertex/" + prop_name + '/' + prop_value + '/' + username
        print query(url)

    def get_edge_property(self, prop_name, prop_value):
        url = root_url + "/graphs/property/edge/" + prop_name + '/' + prop_value + '/' + username
        print query(url)
