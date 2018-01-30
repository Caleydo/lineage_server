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


@app.route("/graph/<dbname>")
def get_graph(dbname):
    # query = ("MATCH (tom:Person {name: \"Tom Hanks\"})-[:ACTED_IN]->(tomHanksMovies) RETURN tom,tomHanksMovies")
    # query = ("MATCH (m:Movie)<-[:ACTED_IN]-(a:Person) "
    #          "RETURN m.title as movie, collect(a.name) as cast "
    #          "LIMIT {limit}")

    # query = ("MATCH (c:Character)-[:APPEARED_IN]->(e:Episode) "
    #          "RETURN c.name as movie, collect(e.title) as cast "
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

    query = ("MATCH (user:_Network_Node) WHERE user.id = 'C00166' "
             "CALL apoc.path.subgraphAll(user, {maxLevel:3, labelFilter:'+_Network_Node', relationshipFilter:'Edge'}) YIELD nodes, relationships "
            " UNWIND relationships as rels " 
            " UNWIND nodes as node " 
            " WITH node, startNode(rels) as sNode, endNode(rels) as eNode "  
            " RETURN {title:node.name, label:labels(node), id:node.id} as node2, {title:sNode.name, label:labels(sNode), id:sNode.id} as sNode, {title:eNode.name, label:labels(eNode), id:eNode.id} as eNode")

    db = gdbPath


    # if dbname == 'got':
    #     db = gdbGot
    # elif dbname == 'coauth':
    #     db = gdbCoauth
    # elif dbname == 'path':
    #     db = gdbPath
    # db = gdbPath;
    # db = gdbPath;

    results = db.query(query,
                       params={"limit": request.args.get("limit", 100)})

    nodes = []
    rels = []
    i = 0

    print (results)

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
    # for startNode, endNode in results:
    #     source = nodes.index(startNode)
    #     target = nodes.index(endNode)
    #     rels.append({"source": source, "target": target})
        
    # for movie, cast in results:
    #     nodes.append({"title": movie['title'], "label": movie['label'], "uuid":movie['id']})
    #     target = i
    #     i += 1
    #     for name in cast:
    #         actor = {"title": name['title'], "label": name['label'], "uuid":name['id']}
    #         try:
    #             source = nodes.index(actor)
    #         except ValueError:
    #             nodes.append(actor)
    #             source = i
    #             i += 1
    #         rels.append({"source": source, "target": target})

    return Response(dumps({"nodes": nodes, "links": rels}),
                    mimetype="application/json")


def create():
    return app
