from phovea_server.ns import Namespace
from phovea_server.util import jsonify

import logging

from json import dumps

from flask import Flask, Response, request, render_template, url_for

from neo4jrestclient.client import GraphDatabase, Node

app = Namespace(__name__)
_log = logging.getLogger(__name__)


# gdb = GraphDatabase("http://0.0.0.0:7474")


@app.route('/', methods=['GET'])
def _func():
    return jsonify({
        'message': 'Basic Test'
    })

@app.route("/graph")
def get_graph():
    # query = ("MATCH (tom:Person {name: \"Tom Hanks\"})-[:ACTED_IN]->(tomHanksMovies) RETURN tom,tomHanksMovies")
    # query = ("MATCH (m:Movie)<-[:ACTED_IN]-(a:Person) "
    #          "RETURN m.title as movie, collect(a.name) as cast "
    #          "LIMIT {limit}")

    query = ("MATCH (t:Team) "
    " RETURN t.name as movie , collect(t.country) as cast "
     "LIMIT {limit}")
    results = gdb.query(query,
                        params={"limit": request.args.get("limit", 20)})
    nodes = []
    rels = []
    i = 0
    for movie, cast in results:
        nodes.append({"title": movie, "label": "movie"})
        target = i
        i += 1
        for name in cast:
            actor = {"title": name, "label": "actor"}
            try:
                source = nodes.index(actor)
            except ValueError:
                nodes.append(actor)
                source = i
                i += 1
            rels.append({"source": source, "target": target})
    return Response(dumps({"nodes": nodes, "links": rels}),
                    mimetype="application/json")


def create():
    return app
