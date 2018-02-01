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
def get_graph(dbname,rootID = None, depth = 1):

    if dbname == 'got':
        db = gdbGot

        if rootID is None:
            rootID = 'fb7b71da-84cb-4af5-a9fc-fc14e597f8f0' #Cercei Lannister


        # query = ("MATCH (user:Character) WHERE user.uuid = 'fb7b71da-84cb-4af5-a9fc-fc14e597f8f0' "
        #      "CALL apoc.path.subgraphAll(user, {maxLevel:1}) YIELD nodes, relationships "
        #     " UNWIND relationships as rels " 
        #     " UNWIND nodes as node " 
        #     " WITH node, startNode(rels) as sNode, endNode(rels) as eNode "  
        #     " RETURN {title: COALESCE (node.name, node.title), label:labels(node), id:node.uuid} as node2, {title:COALESCE (sNode.name, sNode.title), label:labels(sNode), id:sNode.uuid} as sNode, {title:COALESCE (eNode.name, eNode.title), label:labels(eNode), id:eNode.uuid} as eNode ")

        # results = db.query(query,
        #                params={"limit": request.args.get("limit", 100)})
        # nodes = []
        # rels = []

        # for node2, sNode, eNode in results:
        #     newNode = {"title": node2['title'], "label": node2['label'][0], "uuid":node2['id']} 
        #     try:
        #         nodes.index(newNode)
        #     except ValueError:
        #         nodes.append(newNode)

        # for node2,sNode,eNode in results:
        #     source = nodes.index({"title": sNode['title'], "label": sNode['label'][0], "uuid":sNode['id']})
        #     target = nodes.index({"title": eNode['title'], "label": eNode['label'][0], "uuid":eNode['id']})

        #     edge = {"source": source, "target": target}
        #     try:
        #         rels.index(edge)
        #     except ValueError:
        #         rels.append(edge)

        query = ("MATCH (c:Character)-[:APPEARED_IN]->(e:Episode) "
                 "RETURN {title:c.name, uuid:c.uuid} as character, collect({title:e.title, uuid:e.uuid}) as episodes "
                 "LIMIT {limit}")

        results = db.query(query,
                       params={"limit": request.args.get("limit", 30)})
        nodes = []
        rels = []
        i = 0

        for character, episodes in results:
            nodes.append({"title": character['title'], "label": 'Actor', "uuid":character['uuid']})
        
            target = i
            i += 1
            for episode in episodes:
                actor = {"title": episode['title'], "label": 'Movie', "uuid":episode['uuid']}
                try:
                    source = nodes.index(actor)
                except ValueError:
                    nodes.append(actor)
                    source = i
                    i += 1
                rels.append({"source": source, "target": target})

    elif dbname == 'path':
        db = gdbPath

        if rootID is None:
            rootID = 'C00166' #Sample Root

# labelFilter:'+_Network_Node', relationshipFilter:'Edge'
        query = ("MATCH (user) WHERE user.id = {rootID} OR user.uuid = {rootID} "
             "CALL apoc.path.subgraphAll(user, {maxLevel:{depth}}) YIELD nodes, relationships "
            " UNWIND relationships as rels " 
            " UNWIND nodes as node " 
            " WITH node, startNode(rels) as sNode, endNode(rels) as eNode "  
            " RETURN {title:node.name, label:labels(node), id:node.id} as node2, {title:sNode.name, label:labels(sNode), id:sNode.id} as sNode, {title:eNode.name, label:labels(eNode), id:eNode.id} as eNode")

        results = db.query(query,
                       params={"rootID":request.args.get("rootID",rootID),"depth":request.args.get("depth",depth)})
        nodes = []
        rels = []

        for node2, sNode, eNode in results:
            newNode = {"title": node2['title'], "label": node2['label'][1], "uuid":node2['id']} 
            try:
                nodes.index(newNode)
            except ValueError:
                nodes.append(newNode)

        for node2,sNode,eNode in results:
            source = nodes.index({"title": sNode['title'], "label": sNode['label'][1], "uuid":sNode['id']})
            target = nodes.index({"title": eNode['title'], "label": eNode['label'][1], "uuid":eNode['id']})

            edge = {"source": source, "target": target}
            try:
                rels.index(edge)
            except ValueError:
                rels.append(edge)

    # elif dbname == 'coauth':
    #     db = gdbCoauth

    labelQuery = 'CALL db.labels()'
    labelResults = db.query(labelQuery)

    for lable in labelResults:
        print (lable)

    return Response(dumps({"nodes": nodes, "links": rels, "root":rootID}),
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
