import os

from flask import Flask, request, jsonify, request, make_response
from flask_migrate import Migrate
from flask_api import status
from randomdict import RandomDict
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
        self.nodemap = RandomDict()
    
    def add_node(self, node_id, node_url, shards):     
        self.nodemap[node_id] = NodeStatus(node_url, node_id, shards)

        for shard in shards:
            if not shard in self.shard_to_nodes:
                self.shard_to_nodes[shard] = set()
            self.shard_to_nodes[shard].add(node_id)
    
    def remove_node(self, node_id):
        if node_id not in self.nodemap:
            return

        for shard in self.nodemap[node_id].shards:
            self.shard_to_nodes[shard].remove(node_id)

        self.nodemap[node_id].timer.cancel()
        del self.nodemap[node_id]

    def add_shard(self, node_id, shard):
        self.nodemap[node_id].shards.add(shard)

        if not shard in self.shard_to_nodes:
            self.shard_to_nodes[shard] = set()
        self.shard_to_nodes[shard].add(node_id)

    def remove_shard(self, shard, node_id):
        # when node_id is None, delete shard on all nodes
        if node_id is None:
            for node in self.shard_to_nodes[shard]:
                self.nodemap[node].shards.remove(shard)
            del self.shard_to_nodes[shard]
        else:
            self.nodemap[node_id].shards.remove(shard)
            self.shard_to_nodes[shard].remove(node_id)
    
    def get_shard_nodes(self, shard):
        # Return a set of nodes
        return self.shard_to_nodes[shard]

    def get_node_shards(self, node_id):
        # Return a set of shards
        return self.nodemap[node_id].shards

shard_node_map = ShardNodeMap()


class NodeStatus:
    def __init__(self, url, node_id, shards):
        self.node_id = node_id
        self.url = url
        self.shards = shards
        self.disk_usage = None
        self.bandwidth = None
        self.cpu_usage = None
        self.load_avg = None
        self.disk_capacity = None
        self.disk_free = None
        self.report_miss = 0
        self.liveness_request()

    # Periodic liveness check request
    def liveness_request(self):
        try:
            response = requests.get(self.url + "/v0/stats", timeout=stats_timeout)
        except requests.Timeout:
            # back off and retry
            self.report_miss += 1
            # Node is down
            if self.report_miss == report_miss_before_down:
                self.timer.cancel()
                reconstruct_shards(self.shards)
                shard_node_map.remove_node(self.node_id)
                # remove node from map
            pass
        except requests.ConnectionError:
            # TODO: return error
            pass

        stats = response.json()
        self.load_avg = stats['load_avg']   # CPU + RAM + DISK I/O
        self.bandwidth = stats['bandwidth']
        self.cpu_usage = stats['cpu_usage']
        self.disk_capacity = stats['disk_capacity']
        self.disk_free = stats['disk_free']
        print('stats: ', self.bandwidth, self.cpu_usage, \
                        self.disk_usage, self.load_avg, self.disk_capacity, self.disk_free)
        if self.load_avg > 0.75:
            redistribute_shard(self.url)    # TODO: find the most popular shard to redistribute
        self.timer = threading.Timer(liveness_report_period, self.liveness_request)
        self.timer.start()

def reconstruct_shards(shards):
    for shard in shards:
        node_map = []
        excluded_nodes_id = []
        object_chunk_query = db.session.query(Shard).filter_by(shard_hash = shard)
        object_id = object_chunk_query[0].object_id
        chunk_index = object_chunk_query[0].chunk_index
        object_query = db.session.query(Object).filter_by(id = object_id)
        shards_query = db.session.query(Shard).filter_by(object_id=object_id, chunk_index=chunk_index)
        construct_shard_map = []
        for shard_query in shards_query:
            if shard_query.shard_hash == shard:
                construct_shard_map.append({None})
                continue
            nodemap_index = []
            nodes = shard_node_map.get_shard_nodes(shard_query.shard_hash)
            for node in nodes:
                excluded_nodes_id += node
                node_url = shard_node_map.node_map[node].url
                try:
                    index = node_map.index(node_url)
                    nodemap_index.append(index)
                except:
                    node_map.append(node_url)
                    nodemap_index.append(len(node_map) - 1)     
            construct_shard_map.append({
                'digest': shard_query.shard_hash.hex(),
                'nodes': nodemap_index,
                'ticket': 'TICKET'})
            
        # The node this reconstruction should take place
        node = place_shards(1, excluded_nodes_id, object_query[0].shard_size)[0] 
        payload = {
            "chunk_size": object_query[0].chunk_size,
            "shard_size": object_query[0].shard_size,
            "code_ratio_data": object_query[0].code_ratio_data,
            "code_ratio_parity": object_query[0].code_ratio_parity,
            "shard_map": construct_shard_map,
            "node_map": node_map
        }
        # send the reconstruction request
        response = requests.post(node + '/' + shard.hex() + "/reconstruct", data = payload)

def redistribute_shard(shard, source_node_url):
    excluded_nodes_id = []
    object_chunk_query = db.session.query(Shard).filter_by(shard_hash = shard)
    object_id = object_chunk_query[0].object_id
    chunk_index = object_chunk_query[0].chunk_index
    object_query = db.session.query(Object).filter_by(id = object_id)
    shards_query = db.session.query(Shard).filter_by(object_id=object_id, chunk_index=chunk_index)
    for shard_query in shards_query:
        excluded_nodes_id += shard_node_map.get_shard_nodes(shard_query.shard_hash)
    # The node this redistribution should take place
    node = place_shards(1, excluded_nodes_id, object_query[0].shard_size)[0] 
    payload = {
        "source_node": source_node_url,
        "ticket": "TICKET"
    }
    # send the reconstruction request
    response = requests.post(node + '/' + shard.hex() + "/fetch", data = payload)

def place_shards(number_shards, excluded_nodes_id, shard_size):
    nodes = []
    for _ in range(number_shards):
        node_id, node_url = place_shard(excluded_nodes_id, shard_size)
        excluded_nodes_id.append(node_id)
        nodes.append(node_url)
    return nodes

# Selection algorithm 
def place_shard(excluded_nodes_id, shard_size):
    while(1):
        item = shard_node_map.nodemap.random_item()
        if item.key not in excluded_nodes_id \
            and (item.value.disk_free - shard_size) / item.value.disk_capacity > 0.3 :
            return item[0], item[1].url
    

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
    shards = set([bytes(bytearray.fromhex(i)) for i in body['shards']])
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
    uuid_file = uuid.uuid4() #PLACEHOLDER for now
    for _ in range(num_chunck):
        chunk = []
        nodes = place_shards(num_data_shards + parity_shards, [], size_shard) 
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

    # "upload_results" lists shards in order 
    upload_results= body["upload_results"]
    for chunk_index, res_chunk in enumerate(upload_results):
        chunk_row = Chunk(objectID, chunk_index)
        db.session.add(chunk_row)
        for shard_index, res_shard in enumerate(res_chunk):
            digest = bytes(bytearray.fromhex(res_shard["digest"]))
            # receipt = i["receipt"] # Ideally should check the vadility of the receipt
            shard_row = Shard(objectID, chunk_index, shard_index, digest)
            db.session.add(shard_row)

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
                node_url = shard_node_map.node_map[node].url
                try:
                    index = node_map.index(node_url)
                    nodemap_index.append(index)
                except:
                    node_map.append(node_url)
                    nodemap_index.append(len(node_map) - 1)                    
            one_chunk.append({"digest": digest.hex(), "nodes": nodemap_index})
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
