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
# gdbCoauth = GraphDatabase("http://neo4j_coauth:7474")


@app.route('/', methods=['GET'])
def _func():
    return jsonify({
        'message': 'Basic Test'
    })


@app.route("/labels/<dbname>")
def get_labels(dbname):

    if dbname == 'got':
        db = gdbGot

    elif dbname == 'path':
        db = gdbPath

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

    for label , nodes in labelResults:
        labels.append({"name": label, "nodes":nodes})

    return Response(dumps({"labels": labels}),
        mimetype="application/json")



@app.route("/graph/<dbname>") 
@app.route("/graph/<dbname>/<rootID>/<depth>")
def get_graph(dbname = 'got',rootID = None, depth = 1):

    if dbname == 'got':
        db = gdbGot

        if rootID is None:
            rootID = 'fb7b71da-84cb-4af5-a9fc-fc14e597f8f0' #Cercei Lannister

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

    elif dbname == 'path':
        db = gdbPath

        if rootID is None:
            rootID = 'C00166' #Sample Root

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

    # elif dbname == 'coauth':
    #     db = gdbCoauth

    query = ("MATCH (root)-[edge]-(target) WHERE root.id = {rootID} OR root.uuid = {rootID} "
            " RETURN {title: COALESCE (root.name, root.title), label:labels(root), id:COALESCE (root.uuid, root.id)} as root, {title: COALESCE (edge.name, edge.title), id:COALESCE (edge.uuid, edge.id)} as edge, {title: COALESCE (target.name, target.title), label:labels(target), id:COALESCE (target.uuid, target.id)} as target")


    # query = ("MATCH (user) WHERE user.id = {rootID} OR user.uuid = {rootID} "
    #          "CALL apoc.path.subgraphAll(user, {maxLevel:{depth}}) YIELD nodes, relationships "
    #         " UNWIND relationships as rels " 
    #         " UNWIND nodes as node " 
    #         " WITH node, startNode(rels) as sNode, endNode(rels) as eNode "  
    #         " RETURN {title: COALESCE (node.name, node.title), label:labels(node), id:COALESCE (node.uuid, node.id)} as node2, {title: COALESCE (sNode.name, sNode.title), label:labels(sNode), id:COALESCE (sNode.uuid, sNode.id)} as sNode, {title: COALESCE (eNode.name, eNode.title), label:labels(eNode), id:COALESCE (eNode.uuid, eNode.id)} as eNode")

    results = db.query(query,
                       params={"rootID":request.args.get("rootID",rootID),"depth":request.args.get("depth",depth)})
    
    filteredRootLabels = [item for item in results[0][0]['label'] if '_' not in item]
    
    nodes = [{"title": results[0][0]['title'], "label":filteredRootLabels[0], "uuid":results[0][0]['id']}] #add root object to array of nodes
    # nodes=[]
    rels = []
    

    for root, edge, target in results:
        # print(results[0])
        filteredLabels = [item for item in target['label'] if '_' not in item]
        newNode = {"title": target['title'], "label":filteredLabels[0], "uuid":target['id']} 
        try:
            nodes.index(newNode)
        except ValueError:
            nodes.append(newNode)

        source = {"title": root['title'], "label": filteredRootLabels[0], "uuid":root['id']}
        target = {"title": target['title'], "label": filteredLabels[0], "uuid":target['id']}


        edge = {"source": source, "target": target}
        try:
            rels.index(edge)
        except ValueError:
            rels.append(edge)
        
    
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

    return Response(dumps({"query":query, "nodes": nodes, "links": rels, "root":[rootID]}),
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
