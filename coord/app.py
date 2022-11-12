import os

from flask import Flask, request, jsonify, request, make_response
from flask_migrate import Migrate
from flask_api import status
import toml
import math
import uuid
import time, threading
# from grequests import async
import requests
from database import db

app = Flask(__name__)
app.config.from_object(os.environ['APP_SETTINGS'])
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db.init_app(app)
migrate = Migrate(app, db)

from models.objects import Object, Chunk, Shard

config = toml.load("config.toml")
max_data_shards = config['erasure_code']['max_data_shards']
parity_shards = config['erasure_code']['parity_shards']
max_shard_size = config['shard_size']['max_shard_size']
min_shard_size = config['shard_size']['min_shard_size']
max_chunk_size = max_shard_size * max_data_shards
liveness_report_period = config['liveness_check']['liveness_report_period']
report_miss_before_down = config['liveness_check']['report_miss_before_down']
stats_timeout = config['liveness_check']['stats_timeout']
class ShardNodeMap:
    def __init__(self):
        self.shard_to_nodes = {}
        self.nodemap = {}
    
    def add_node(self, node_id, node_url, shards):     
        self.nodemap[node_id] = NodeStatus(node_url, shards)

        for shard in shards:
            if not shard in self.shard_to_nodes:
                self.shard_to_nodes[shard] = set()
            self.shard_to_nodes[shard].add(node_id)
    
    def remove_node(self, node_id):
        if node_id not in self.nodemap:
            return

        for shard in self.nodemap[node_id]:
            self.shard_to_nodes[shard].remove(node_id)

        self.nodemap[node_id].timer.cancel()
        del self.nodemap[node_id]

    def add_shard(self, node_id, shard):
        self.nodemap[node_id].add(shard)

        if not shard in self.shard_to_nodes:
            self.shard_to_nodes[shard] = set()
        self.shard_to_nodes[shard].add(node_id)

    def remove_shard(self, shard, node_id):
        # when node_id is None, delete shard on all nodes
        if node_id is None:
            for node in self.shard_to_nodes[shard]:
                self.nodemap[node].remove(shard)
            del self.shard_to_nodes[shard]
        else:
            self.nodemap[node_id].remove(shard)
            self.shard_to_nodes[shard].remove(node_id)
    
    def get_shard_nodes(self, shard):
        # Return a set of nodes
        return self.shard_to_nodes[shard]

    def get_node_shards(self, node_id):
        # Return a set of shards
        return self.nodemap[node_id]

shard_node_map = ShardNodeMap()


class NodeStatus:
    def __init__(self, url, shards):
        self.url = url
        self.shards = shards
        self.disk_usage = None
        self.bandwidth = None
        self.cpu_usage = None
        self.report_miss = 0
        self.timer = threading.Timer(liveness_report_period, self.liveness_request)
        self.timer.start()

    # Periodic liveness check request
    def liveness_request(self):
        try:
            response = requests.post(self.url + "/stats", timeout=stats_timeout)
        except requests.Timeout:
            # back off and retry
            self.report_miss += 1
            # Node is down
            if self.report_miss == 3:
                self.timer.cancel()
                # remove node from map
            pass
        except requests.ConnectionError:
            pass
        finally:
            self.bandwidth = response['bandwidth']
            self.cpu_usage = response['cpu_usage']
            self.disk_usage = response['disk_usage']

def place_shards(number_shards, excluded_nodes, shard_size):
    nodes = []
    for _ in range(number_shards + parity_shards):
        node_id, node_url = place_shard(excluded_nodes, shard_size)
        excluded_nodes.append(node_id)
        nodes.append(node_url)
    return nodes

# Selection algorithm 
def place_shard(excluded_nodes, shard_size):
    while(1):
        item = shard_node_map.random_item()
        if item.key not in excluded_nodes and item.value.disk_usage > shard_size:
            return item.key, item.value.url
    

@app.route("/")
def hello():
    all_objects = Object.query.all()
    return jsonify([o.serialize() for o in all_objects])

def generic_error_response(error_type, message,
                           code=status.HTTP_500_INTERNAL_SERVER_ERROR):
    return make_response(
        {
            "type": error_type,
            "message": message,
        },
        code
    )
def json_error():
    return generic_error_response(
        "invalid_content_type",
        "Endpoint expected a Content-Type of \"application/json\"",
        status.HTTP_400_BAD_REQUEST,
    )

def ensure_json():
    return request.headers.get("Content-Type") == "application/json"

@app.route("/v0/node/<node_id>", methods = ["PUT"])
def node_register(node_id):
    if not ensure_json():
        return json_error()
    body = request.json

    # Verify other fields, e.g., list of shard hashes

    node_url = body['node_url']
    
    shards = set([i.encode() for i in body['shards']])
    shard_node_map.remove_node(node_id)
    shard_node_map.add_node(node_id, node_url, shards)

    return make_response("", status.HTTP_200_OK)

@app.route("/v0/object", methods = ["POST"])
def upload_object():
    if not ensure_json():
        return json_error()
    body = request.json
    object_size = int(body['object_size'])
    num_chunck = math.ceil(object_size / max_chunk_size)
    size_chunk = math.ceil(object_size / num_chunck)
    num_data_shards = min(math.ceil(size_chunk / min_shard_size), max_data_shards)
    size_shard = math.ceil(size_chunk / num_data_shards)
    shard_map = []
    uuid_file = uuid.uuid5(uuid.NAMESPACE_DNS, 'object.txt') #PLACEHOLDER for now
    for _ in range(num_chunck):
        chunk = []
        nodes = place_shards(num_data_shards, [], size_shard) 
        for node in nodes:
            chunk.append({
                        "ticket": "RANDOM_TICKET" ,
                        "node": node,
                    })
        shard_map.append(chunk)

    return {
        "object_size": object_size,
        "chunk_size": size_chunk,
        "shard_size": size_shard,
        "code_ratio_data": num_data_shards,
        "code_ratio_parity": parity_shards,
        "shard_map": shard_map,
        "object_id": uuid_file,
        "signature": "SIGNATURE"
    }
    
@app.route("/v0/object/<objectID>/finalize", methods = ["PUT"])
def finalize_object(objectID):
    if not ensure_json():
        return json_error()
    body = request.json

    # Defer checking of all deferrable PostgreSQL constraints
    # to allow cross-references to exist between rows of a
    # single transaction.
    db.session.execute("SET CONSTRAINTS ALL DEFERRED")

    object = Object(
        id=objectID,
        size=int(body["object_upload_map"]["object_size"]),
        chunk_size=int(body["object_upload_map"]["chunk_size"]),
        shard_size=int(body["object_upload_map"]["shard_size"]),
        code_ratio_data=int(body["object_upload_map"]["code_ratio_data"]),
        code_ratio_parity=int(body["object_upload_map"]["code_ratio_parity"])
    )
    db.session.add(object)

    # "upload_results" lists shards in order. e.g., 
    # (chunk0, shard0), (chunk0, shard1), (chunk1, shard0), (chunk1, shard1)
    upload_results= body["upload_results"]
    prev_chunk_index = None 
    for i in upload_results:
        chunk_index = int(i["chunk_index"])
        if prev_chunk_index == None or prev_chunk_index != chunk_index:
            prev_chunk_index = chunk_index
            chunk = Chunk(objectID, chunk_index)
            db.session.add(chunk)
        shard_index = int(i["shard_index"])
        digest = i["digest"].encode()
        # receipt = i["receipt"] # Ideally should check the vadility of the receipt
        shard = Shard(objectID, chunk_index, shard_index, digest)
        db.session.add(shard)

    db.session.commit()
    return make_response("", status.HTTP_200_OK)

@app.route("/v0/object/<objectID>", methods = ["GET"])
def retrieve_object(objectID):
    # Only one object row corresponding to the objectID
    object = db.session.query(Object).filter_by(id=objectID)

    chunks = db.session.query(Chunk).filter_by(object_id=objectID)  
    node_map = []
    all_chunks = []
    for chunk in chunks:
        one_chunk = []
        chunk_index = chunk.chunk_index
        shards = db.session.query(Shard).filter_by(object_id=objectID, chunk_index=chunk_index)
        for shard in shards:
            digest = shard.shard_hash
            nodemap_index = []
            nodes = shard_node_map.get_shard_nodes(digest)
            for node in nodes:
                try:
                    index = node_map.index(node)
                    nodemap_index.append(index)
                except:
                    node_map.append(node)
                    nodemap_index.append(len(node_map) - 1)                    
            one_chunk.append({"digest": digest.decode(), "nodes": nodemap_index})
        all_chunks.append(one_chunk)

    return {
        
            "object_size": object[0].size,
            "chunk_size": object[0].chunk_size,
            "shard_size": object[0].shard_size,
            "code_ratio_data": object[0].code_ratio_data,
            "code_ratio_parity": object[0].code_ratio_parity,
            "shard_map": all_chunks,
            "node_map": node_map
        
    }    
