from phovea_server.ns import Namespace
from phovea_server.util import jsonify

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

@app.route("/edges/<dbname>/<nodeID>")
def get_edges(dbname,nodeID):

    if dbname == 'got':
        db = gdbGot

    elif dbname == 'path':
        db = gdbPath

    elif dbname == 'coauth':
        db = gdbCoauth

    # labelQuery = 'CALL db.labels()'

    query = ("MATCH (root)-[edge]-(target) "
            " WITH root, edge, target, endNode(edge) as endNode, startNode(edge) as startNode "
            " WHERE root.id= {nodeID} OR root.uuid = {nodeID} "
                "RETURN {title: COALESCE (edge.name, edge.title), id:COALESCE (edge.uuid, edge.id), info:edge} as edge, "
                " {title: COALESCE (target.name, target.title), label:labels(target), id:COALESCE (target.uuid, target.id)} as target, "
                " {title: COALESCE (endNode.name, endNode.title), label:labels(endNode), id:COALESCE (endNode.uuid, endNode.id)} as endNode, "
                " {title: COALESCE (startNode.name, startNode.title), label:labels(startNode), id:COALESCE (startNode.uuid, startNode.id)} as startNode ")

    
    results = db.query(query,
                       params={"nodeID":request.args.get("nodeID",nodeID)})

    print (results)
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

    db = gdbGot

    # if dbname == 'got':
    #     db = gdbGot

    # elif dbname == 'path':
    #     db = gdbPath

    # elif dbname == 'coauth':
    #     db = gdbCoauth

    labels = []

    # labelQuery = 'CALL db.labels()'


    labelQuery = ("CALL db.labels() YIELD label  "
                " WITH label "
                " MATCH (n) WHERE label in labels(n) " 
                " WITH label, n, size((n)--()) as degree "
                " ORDER BY degree DESC " 
                " RETURN  label, collect({title: COALESCE (n.name, n.title), degree: degree, id:COALESCE (n.uuid, n.id)}) AS nodes ")

    labelResults = db.query(labelQuery)

    print (labelResults)

    for label , nodes in labelResults:
        labels.append({"name": label, "nodes":nodes})

    return Response(dumps({"query":labelQuery, "labels": labels}),
        mimetype="application/json")



@app.route("/graph/<dbname>") 
@app.route("/graph/<dbname>/<rootID>/<include>")
def get_graph(dbname = 'got',rootID = None, include = 'true'):

    if dbname == 'got':
        db = gdbGot

        if rootID is None:
            rootID = 'fb7b71da-84cb-4af5-a9fc-fc14e597f8f0' #Cercei Lannister

    elif dbname == 'path':
        db = gdbPath

        if rootID is None:
            rootID = 'C00166' #Sample Root

    elif dbname == 'coauth':
        db = gdbCoauth

    setQuery = ("MATCH (root)-[edge]-(target) WHERE root.id = {rootID} OR root.uuid = {rootID} "
                " RETURN {title: COALESCE (root.name, root.title), label:labels(root), id:COALESCE (root.uuid, root.id)} as root, {title: COALESCE (edge.name, edge.title), id:COALESCE (edge.uuid, edge.id)} as edge, {title: COALESCE (target.name, target.title), label:labels(target), id:COALESCE (target.uuid, target.id)} as target")


    edgeQuery =  ("MATCH (root)-[edge]-(target) WHERE root.id = {rootID} OR root.uuid = {rootID} "
                " WITH collect(target) as nodes "
                " UNWIND nodes as n "
                # " UNWIND nodes as m "
                " MATCH (n)-[edge]- (m) "
                " WITH startNode(edge) as n, edge, endNode(edge) as m"
                " RETURN {title: COALESCE (n.name, n.title), label:labels(n), id:COALESCE (n.uuid, n.id)} as source, edge,  {title: COALESCE (m.name, m.title), label:labels(m), id:COALESCE (m.uuid, m.id)} as target ") 
                # " WITH * WHERE id(n) < id(m) "
                # " MATCH path = allShortestPaths( (n)-[*..1]-(m) ) "
                # " RETURN path") 
    
    setResults = db.query(setQuery,
                       params={"rootID":request.args.get("rootID",rootID)})

    edgeResults = db.query(edgeQuery,
                       params={"rootID":request.args.get("rootID",rootID)})

    nodes=[]
    rels=[]

    
    # filteredRootLabels = [item for item in results[0][0]['label'] if '_' not in item] 
    # nodes = [{"title": results[0][0]['title'], "label":filteredRootLabels[0], "uuid":results[0][0]['id']}] #add root object to array of nodes

    # Add all target nodes to array node
    for root, edge, target in setResults:

        filteredRootLabels = [item for item in root['label'] if '_' not in item]
        filteredTargetLabels = [item for item in target['label'] if '_' not in item]

        rootNode = {"title": root['title'], "label":filteredRootLabels[0], "uuid":root['id']} 
        targetNode = {"title": target['title'], "label":filteredTargetLabels[0], "uuid":target['id']} 
        
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

    
    return Response(dumps({"setQuery":setQuery, "edgeQuery":edgeQuery, "nodes": nodes, "links": rels, "root":[rootID]}),
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

    
