from phovea_server.ns import Namespace
from phovea_server.util import jsonify

#to parse urls with / in them
#from urllib.parse import urlparse 

from requests.utils import quote, unquote

# from urllib import parse

import logging

from json import dumps

from flask import Flask, Response, request, render_template, url_for

from neo4jrestclient.client import GraphDatabase, Node

app = Namespace(__name__)
_log = logging.getLogger(__name__)


gdbPath = GraphDatabase("http://neo4j_path:7474")
gdbGot = GraphDatabase("http://neo4j_got:7474")
gdbCoauth = GraphDatabase("http://neo4j_dblp:7474")


@app.route('/', methods=['GET'])
def _func():
    return jsonify({
        'message': 'Basic Test'
    })

@app.route("/edges/<dbname>/<path:nodeID>", methods=["POST"])
def get_edges(dbname, nodeID):

    request_data = request.get_json()
    nodeID = unquote(nodeID)

    if dbname == 'got':
        db = gdbGot

    elif dbname == 'path':
        db = gdbPath

    elif dbname == 'coauth':
        db = gdbCoauth

    query = ("MATCH (root)-[edge]-(target) "
            " WHERE COALESCE (target.uuid, target.id) in {treeNodes} "
            # " and  COALESCE (root.id, root.uuid) in {treeNodes} "
            " WITH root, edge, target, endNode(edge) as endNode, startNode(edge) as startNode "
            " WHERE COALESCE (root.uuid, root.id) = {nodeID} "
            " RETURN {title: COALESCE (edge.name, edge.title), id:COALESCE (edge.uuid, edge.id), info:edge} as edge, "
            " {title: COALESCE (target.name, target.title), label:labels(target), id:COALESCE (target.uuid, target.id)} as target, "
            " {title: COALESCE (endNode.name, endNode.title), label:labels(endNode), id:COALESCE (endNode.uuid, endNode.id)} as endNode, "
            " {title: COALESCE (startNode.name, startNode.title), label:labels(startNode), id:COALESCE (startNode.uuid, startNode.id)} as startNode ")

    
    results = db.query(query,
                       params={"nodeID":request.args.get("nodeID",nodeID),"treeNodes":request_data[u'treeNodes']})

    nodes=[]
    rels = []
    


    for edge, target, endNode, startNode in results:
        newNode = {"endNode": endNode, "startNode":startNode, "edge":edge, "uuid":target['id']} 
        try:
            nodes.index(newNode)
        except ValueError:
            nodes.append(newNode)
    
    return Response(dumps({"query":query, "nodes": nodes}),
        mimetype="application/json")


@app.route("/labels/<dbname>")
def get_labels(dbname):

    if dbname == 'got':
        db = gdbGot

    elif dbname == 'path':
        db = gdbPath

    elif dbname == 'coauth':
        db = gdbCoauth

    labels = []

    labelQuery = ("CALL db.labels() YIELD label  "
                " WITH label "
                " MATCH (n) WHERE label in labels(n) "
                " WITH label, n, size((n)--()) as degree "
                " RETURN  label, collect({title: COALESCE (n.name, n.title, n.review_id, n.neighborhood_id,n.code), degree: degree, id:COALESCE (n.uuid, n.id)})[..20] AS nodes ")

    labelResults = db.query(labelQuery)

    for label , nodes in labelResults:
        labels.append({"name": label, "nodes":nodes})

    return Response(dumps({"query":labelQuery, "labels": labels}),
        mimetype="application/json")

@app.route("/filter/<dbname>", methods=["POST"])
def filter(dbname):

    request_data = request.get_json()

    searchString = request_data[u'searchString']

    if dbname == 'got':
        db = gdbGot

    elif dbname == 'path':
        db = gdbPath

    elif dbname == 'coauth':
        db = gdbCoauth

    labels = []

    query = ("CALL db.labels() YIELD label  "
                " WITH label "
                " MATCH (n) WHERE label in labels(n) AND COALESCE (n.name, n.title) =~ '(?i).*" + searchString + ".*'" 
                " WITH label, n, size((n)--()) as degree "
                " RETURN  label, collect({title: COALESCE (n.name, n.title), degree: degree, id:COALESCE (n.uuid, n.id)})[..100] AS nodes ")


    labelResults = db.query(query)

    for label , nodes in labelResults:
        labels.append({"name": label, "nodes":nodes})

    return Response(dumps({"query":query, "labels": labels}),mimetype="application/json")

@app.route("/query/<dbname>", methods=["POST"])
def query(dbname):

    request_data = request.get_json()

    searchString = request_data[u'searchString']

    if dbname == 'got':
        db = gdbGot

    elif dbname == 'path':
        db = gdbPath

    elif dbname == 'coauth':
        db = gdbCoauth

    labels = []

    query = searchString


    labelResults = db.query(query)

    for label , nodes in labelResults:
        labels.append({"name": label, "nodes":nodes})

    return Response(dumps({"query":query, "labels": labels}),mimetype="application/json")


@app.route("/property/<dbname>/<propName>", methods=["POST"])
def get_property(dbname,propName):

    request_data = request.get_json()

    if dbname == 'got':
        db = gdbGot

    elif dbname == 'path':
        db = gdbPath

    elif dbname == 'coauth':
        db = gdbCoauth

    resultNodes = []

    query = ("MATCH (n) WHERE COALESCE (n.uuid, n.id) in {treeNodes} AND {propName} in keys(n) " 
    " RETURN {uuid:COALESCE (n.uuid, n.id), prop:{propName}, value:n[{propName}]} as node ") 

    results = db.query(query,
                       params={"propName":propName, "treeNodes":request_data[u'treeNodes']})


    for node in results:
        try:
            resultNodes.index(node[0])
        except ValueError:
            resultNodes.append(node[0])

    return Response(dumps({"query":query, "results": resultNodes}),
        mimetype="application/json")

@app.route("/properties/<dbname>")
def get_properties(dbname):

    # db = undefined

    if dbname == 'got':
        db = gdbGot

    elif dbname == 'path':
        db = gdbPath

    elif dbname == 'coauth':
        db = gdbCoauth

    properties = []

    query = ("MATCH (n) WITH distinct keys(n) as keys, labels(n) as labels UNWIND keys as k UNWIND labels as l RETURN k,l")
    
    results = db.query(query)

    for k,l in results:
        newNode = {"label":l,"property":k}
        try:
            properties.index(newNode)
        except ValueError:
            properties.append(newNode)

    return Response(dumps({"query":query, "properties": properties}),
        mimetype="application/json")

@app.route("/graph/<dbname>", methods=["POST"]) 
@app.route("/graph/<dbname>/<path:rootID>", methods=["POST"])
@app.route("/graph/<dbname>/<path:rootID>/<include>", methods=["POST"])
def get_graph(dbname='got', rootID=None, include='true', methods=["POST"]):
    rootID = unquote(request.args.get("rootID", rootID))

    request_data = request.get_json()

    # existingNodes = request.GET.get('treeNode')

    # print(existingNodes)

    if dbname == 'got':
        db = gdbGot

    elif dbname == 'path':
        db = gdbPath

    elif dbname == 'coauth':
        db = gdbCoauth

    setQuery = ("MATCH (root)-[edge]-(target) WHERE COALESCE (root.uuid, root.id) = {rootID} "
                " WITH size((root)--()) as rootDegree, size((target)--()) as targetDegree, root, edge, target"
                " RETURN rootDegree, targetDegree, {title: COALESCE (root.name, root.title, root.review_id, root.neighborhood_id,root.code), label:labels(root), id:COALESCE (root.uuid, root.id)} as root, edge, {title: COALESCE (target.name, target.title, target.review_id, target.neighborhood_id,target.code), label:labels(target), id:COALESCE (target.uuid, target.id)} as target"
                " LIMIT 100 ")

    edgeQuery =  ("MATCH (root)-[edge]-(target) WHERE COALESCE (root.uuid, root.id) = {rootID} "
                " WITH collect(target) as nodes "
                " UNWIND nodes as n "
                " MATCH (n)-[edge]-(m) WHERE COALESCE (m.uuid, m.id) in {treeNodes} or m in nodes"
                " WITH startNode(edge) as n, edge, endNode(edge) as m"
                " RETURN {title: COALESCE (n.name, n.title, n.review_id, n.neighborhood_id,n.code), label:labels(n), id:COALESCE (n.uuid, n.id)} as source, edge,  {title: COALESCE (m.name, m.title, m.review_id, m.neighborhood_id,m.code), label:labels(m), id:COALESCE (m.uuid, m.id)} as target ")
                # " LIMIT 100 ")
                # " WITH * WHERE id(n) < id(m) "
                # " MATCH path = allShortestPaths( (n)-[*..1]-(m) ) "
                # " RETURN path") 
    
    setResults = db.query(setQuery,
                       params={"rootID":rootID})

    edgeResults = db.query(edgeQuery,
                       params={"rootID":rootID,"treeNodes":request_data[u'treeNodes']})

    # WHERE m.uuid in {treeNodes}
    
    nodes=[]
    rels=[]

    # Add all target nodes to array node
    for rootDegree, targetDegree, root, edge, target in setResults:

        filteredRootLabels = [item for item in root['label'] if '_' not in item]
        filteredTargetLabels = [item for item in target['label'] if '_' not in item]

        rootNode = {"title": root['title'], "label":filteredRootLabels[0], "uuid":root['id'], "graphDegree":rootDegree} 
        targetNode = {"title": target['title'], "label":filteredTargetLabels[0], "uuid":target['id'],"graphDegree":targetDegree} 
        
        try:
            nodes.index(targetNode)
        except ValueError:
            nodes.append(targetNode)

        #Only add root and edge to targets if 'include Root' is true
        if include == 'true':
            try:
                nodes.index(rootNode)
            except ValueError:
                nodes.append(rootNode)

            source = rootNode
            target = targetNode


            rel = {"source": source, "target": target, "edge":edge}
            try:
                rels.index(rel)
            except ValueError:
                rels.append(rel)


    # Add in edges that arrive/leave from set nodes
    for n, edge, m in edgeResults:

        filteredSLabels = [item for item in n['label'] if '_' not in item]
        filteredTLabels = [item for item in m['label'] if '_' not in item]

        source = {"title": n['title'], "label":filteredSLabels[0], "uuid":n['id']} 
        target = {"title": m['title'], "label":filteredTLabels[0], "uuid":m['id']} 

        rel = {"source": source, "target": target, "edge":edge} #{'attr':edge['data'],'type':edge['type']}}
        try:
            rels.index(rel)
        except ValueError:
            rels.append(rel)

    return Response(dumps({"params":request_data,"nodes": nodes, "links": rels, "root":[rootID]}),
        mimetype="application/json")

    # return Response(dumps({"params":request_data,"setQuery":setQuery, "edgeQuery":edgeQuery, "nodes": nodes, "links": rels, "root":[rootID]}),
    #     mimetype="application/json")

@app.route("/getNodes/<dbname>", methods=["POST"]) 
def get_nodes(dbname='got'):
    request_data = request.get_json()

    # existingNodes = request.GET.get('treeNode')

    # print(existingNodes)

    if dbname == 'got':
        db = gdbGot

    elif dbname == 'path':
        db = gdbPath

    elif dbname == 'coauth':
        db = gdbCoauth

    setQuery = ("MATCH (root)-[edge]-(target) WHERE COALESCE (root.uuid, root.id) in {rootNodes}  "
                " WITH size((root)--()) as rootDegree, size((target)--()) as targetDegree, root, edge, target"
                " RETURN rootDegree, targetDegree, {title: COALESCE (root.name, root.title), label:labels(root), id:COALESCE (root.uuid, root.id)} as root, edge, {title: COALESCE (target.name, target.title), label:labels(target), id:COALESCE (target.uuid, target.id)} as target")


    edgeQuery =  ("MATCH (root)-[edge]-(target) WHERE COALESCE (root.uuid, root.id) in {rootNodes}  "
                " with collect(root) as roots, collect(target) as nodes "              
                " MATCH (root)-[edge]-(m) WHERE root in roots OR root in nodes AND (COALESCE (m.uuid, m.id) in {treeNodes} or m in nodes or m in roots) "
                " WITH startNode(edge) as n, edge, endNode(edge) as m"
                " RETURN {title: COALESCE (n.name, n.title), label:labels(n), id:COALESCE (n.uuid, n.id)} as source, edge,  {title: COALESCE (m.name, m.title), label:labels(m), id:COALESCE (m.uuid, m.id)} as target ") 
                # " WITH * WHERE id(n) < id(m) "
                # " MATCH path = allShortestPaths( (n)-[*..1]-(m) ) "
                # " RETURN path") 
    
    setResults = db.query(setQuery,
                       params={"rootNodes":request_data[u'rootNodes']})

    edgeResults = db.query(edgeQuery,
                       params={"rootNodes":request_data[u'rootNodes'],"treeNodes":request_data[u'treeNodes']})

    # WHERE m.uuid in {treeNodes}
    
    nodes=[]
    rels=[]

    # Add all target nodes to array node
    for rootDegree, targetDegree, root, edge, target in setResults:

        filteredRootLabels = [item for item in root['label'] if '_' not in item]
        filteredTargetLabels = [item for item in target['label'] if '_' not in item]

        rootNode = {"title": root['title'], "label":filteredRootLabels[0], "uuid":root['id'], "graphDegree":rootDegree} 
        targetNode = {"title": target['title'], "label":filteredTargetLabels[0], "uuid":target['id'],"graphDegree":targetDegree} 
        
        try:
            nodes.index(targetNode)
        except ValueError:
            nodes.append(targetNode)


        try:
            nodes.index(rootNode)
        except ValueError:
            nodes.append(rootNode)

        source = rootNode
        target = targetNode


        rel = {"source": source, "target": target, "edge":edge}
        try:
            rels.index(rel)
        except ValueError:
            rels.append(rel)


    # Add in edges that arrive/leave from set nodes
    for n, edge, m in edgeResults:

        filteredSLabels = [item for item in n['label'] if '_' not in item]
        filteredTLabels = [item for item in m['label'] if '_' not in item]

        source = {"title": n['title'], "label":filteredSLabels[0], "uuid":n['id']} 
        target = {"title": m['title'], "label":filteredTLabels[0], "uuid":m['id']} 

        rel = {"source": source, "target": target, "edge":edge} #{'attr':edge['data'],'type':edge['type']}}
        try:
            rels.index(rel)
        except ValueError:
            rels.append(rel)

    return Response(dumps({"params":request_data,"nodes": nodes, "links": rels, "root":[request_data[u'rootNode']]}),
        mimetype="application/json")

    # return Response(dumps({"params":request_data,"setQuery":setQuery, "edgeQuery":edgeQuery, "nodes": nodes, "links": rels, "root":[rootID]}),
    #     mimetype="application/json")



@app.route("/getNode/<dbname>/<path:rootID>", methods=["POST"])
def get_node(dbname, rootID):
    rootID = unquote(request.args.get("rootID", rootID))
    # rootID2 = unquote("conf%2Fchi%2F52TomlinsonRABPCMNLPTCOSSPSMFMKBCSBGNHBS12")
    # request.args.get("rootID",rootID)
    if dbname == 'got':
        db = gdbGot

    elif dbname == 'path':
        db = gdbPath

    elif dbname == 'coauth':
        db = gdbCoauth

    setQuery = ("MATCH (root)-[edge]-(target) WHERE COALESCE (root.uuid, root.id) = {rootID} "
                " WITH size((root)--()) as rootDegree, size((target)--()) as targetDegree, root, edge, target  " 
                " RETURN {title: COALESCE (root.name, root.title), label:labels(root), id:COALESCE (root.uuid, root.id), graphDegree:rootDegree} as root, edge, {title: COALESCE (target.name, target.title), label:labels(target), id:COALESCE (target.uuid, target.id), graphDegree:targetDegree} as target")

 
    setResults = db.query(setQuery,
                       params={"rootID":rootID})

    nodes=[]
    targetNodes=[]
    rels=[]


    # Add all target nodes to array node
    for root, edge, target in setResults:

        filteredRootLabels = [item for item in root['label'] if '_' not in item]
        filteredTargetLabels = [item for item in target['label'] if '_' not in item]

        rootNode = {"title": root['title'], "label":filteredRootLabels[0], "uuid":root['id'],"graphDegree":root['graphDegree']} 
        targetNode = {"title": target['title'], "label":filteredTargetLabels[0], "uuid":target['id'], "graphDegree":target['graphDegree']} 
        
        try:
            nodes.index(rootNode)
        except ValueError:
            nodes.append(rootNode)

        try:
            targetNodes.index(targetNode)
        except ValueError:
            targetNodes.append(targetNode)

        source = rootNode
        target = targetNode

        rel = {"source": source, "target": target, "edge":edge}
        try:
           rels.index(rel)
        except ValueError:
           rels.append(rel)

    
    return Response(dumps({"query":setQuery, "nodes": nodes, "targetNodes": targetNodes, "links": rels, "root":[rootID]}),
        mimetype="application/json")


def create():
    return app


#  Sample Queries
    # query = ("MATCH (tom:Person {name: \"Tom Hanks\"})-[:ACTED_IN]->(tomHanksMovies) RETURN tom,tomHanksMovies")
    # query = ("MATCH (m:Movie)<-[:ACTED_IN]-(a:Person) "
    #          "RETURN m.title as movie, collect(a.name) as cast "
    #          "LIMIT {limit}")

    # query = ("MATCH (c:Character)-[:APPEARED_IN]->(e:Episode) "
    #          "RETURN c as movie, collect({title:e.title, uuid:e.uuid}) as cast "
    #          "LIMIT {limit}")

    # query = ("MATCH (user:_Network_Node) WHERE user.id = 'C00166' "
    #          "CALL apoc.path.subgraphAll(user, {maxLevel:2, labelFilter:'+_Network_Node'}) YIELD nodes, relationships "
    #          "RETURN nodes, relationships")

    # query = ("MATCH (c:_Network_Node)-->(e:_Network_Node) " 
    #       "RETURN {title:c.name, label:labels(c), id:c.id} as movie, collect({title:e.name, id:e.id, label:labels(e)}) as cast "
    #       "LIMIT 10")


           # query = ("MATCH (c:Character)-[:APPEARED_IN]->(e:Episode) "
        #          "RETURN {title:c.name, uuid:c.uuid} as character, collect({title:e.title, uuid:e.uuid}) as episodes "
        #          "LIMIT {limit}")

        # results = db.query(query,
        #                params={"limit": request.args.get("limit", 30)})
        # nodes = []
        # rels = []
        # i = 0

        # for character, episodes in results:
        #     nodes.append({"title": character['title'], "label": 'Actor', "uuid":character['uuid']})
        
        #     target = i
        #     i += 1
        #     for episode in episodes:
        #         actor = {"title": episode['title'], "label": 'Movie', "uuid":episode['uuid']}
        #         try:
        #             source = nodes.index(actor)
        #         except ValueError:
        #             nodes.append(actor)
        #             source = i
        #             i += 1
        #         rels.append({"source": source, "target": target})

            
    # for node2, sNode, eNode in results:
    #     filteredLabels = [item for item in node2['label'] if '_' not in item]
    #     newNode = {"title": node2['title'], "label":filteredLabels[0], "uuid":node2['id']} 
    #     try:
    #         nodes.index(newNode)
    #     except ValueError:
    #         nodes.append(newNode)

    # for node2,sNode,eNode in results:
    #     filteredSLabels = [item for item in sNode['label'] if '_' not in item]
    #     filteredELabels = [item for item in eNode['label'] if '_' not in item]
    #     # source = nodes.index({"title": sNode['title'], "label": filteredSLabels[0], "uuid":sNode['id']})
    #     # target = nodes.index({"title": eNode['title'], "label": filteredELabels[0], "uuid":eNode['id']})
    #     source = {"title": sNode['title'], "label": filteredSLabels[0], "uuid":sNode['id']}
    #     target = {"title": eNode['title'], "label": filteredELabels[0], "uuid":eNode['id']}


    #     edge = {"source": source, "target": target}
    #     try:
    #         rels.index(edge)
    #     except ValueError:
    #         rels.append(edge)

    # labelFilter:'+_Network_Node', relationshipFilter:'Edge'
        # query = ("MATCH (user) WHERE user.id = {rootID} OR user.uuid = {rootID} "
        #      "CALL apoc.path.subgraphAll(user, {maxLevel:{depth}}) YIELD nodes, relationships "
        #     " UNWIND relationships as rels " 
        #     " UNWIND nodes as node " 
        #     " WITH node, startNode(rels) as sNode, endNode(rels) as eNode "  
        #     " RETURN {title: COALESCE (node.name, node.title), label:labels(node), id:COALESCE (node.uuid, node.id)} as node2, {title: COALESCE (sNode.name, sNode.title), label:labels(sNode), id:COALESCE (sNode.uuid, sNode.id)} as sNode, {title: COALESCE (eNode.name, eNode.title), label:labels(eNode), id:COALESCE (eNode.uuid, eNode.id)} as eNode")

        # results = db.query(query,
        #                params={"rootID":request.args.get("rootID",rootID),"depth":request.args.get("depth",depth)})
        # nodes = []
        # rels = []

        # for node2, sNode, eNode in results:
        #     filteredLabels = [item for item in node2['label'] if '_' not in item]
        #     newNode = {"title": node2['title'], "label":filteredLabels[0], "uuid":node2['id']} 
        #     try:
        #         nodes.index(newNode)
        #     except ValueError:
        #         nodes.append(newNode)

        # for node2,sNode,eNode in results:
        #     filteredSLabels = [item for item in sNode['label'] if '_' not in item]
        #     filteredELabels = [item for item in eNode['label'] if '_' not in item]
        #     source = nodes.index({"title": sNode['title'], "label": filteredSLabels[0], "uuid":sNode['id']})
        #     target = nodes.index({"title": eNode['title'], "label": filteredELabels[0], "uuid":eNode['id']})

        #     edge = {"source": source, "target": target}
        #     try:
        #         rels.index(edge)
        #     except ValueError:
        #         rels.append(edge)

    
