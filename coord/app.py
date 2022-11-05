import os

from flask import Flask, request, jsonify, request, make_response
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_api import status
import toml
import math
import uuid

from database import db

app = Flask(__name__)
app.config.from_object(os.environ['APP_SETTINGS'])
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db.init_app(app)
migrate = Migrate(app, db)

from models.objects import Object

# with open("config.toml", "rb") as f:
config = toml.load("config.toml")
max_data_shards = config['erasure_code']['max_data_shards']
parity_shards = config['erasure_code']['parity_shards']
max_shard_size = config['shard_size']['max_shard_size']
min_shard_size = config['shard_size']['min_shard_size']
max_chunk_size = max_shard_size * max_data_shards

class ShardNodeMap:
    def __init__(self):
        self.shard_to_nodes = {}
        self.node_to_shards = {}
    
    def add_node(self, node_id, shards):
        if type(shards) == list:
            shards = set(shards)

        self.node_to_shards[node_id] = shards

        for shard in shards:
            if not shard in self.shard_to_nodes:
                self.shard_to_nodes[shard] = set()
            self.shard_to_nodes[shard].add(node_id)
    
    def remove_node(self, node_id):
        if node_id not in self.node_to_shards:
            return

        for shard in self.node_to_shards[node_id]:
            self.shard_to_nodes[shard].remove(node_id)

        del self.node_to_shards[node_id]

    def add_shard(self, node_id, shard):
        self.node_to_shards[node_id].add(shard)

        if not shard in self.shard_to_nodes:
            self.shard_to_nodes[shard] = set()
        self.shard_to_nodes[shard].add(node_id)

    def remove_shard(self, shard, node_id):
        # when node_id is None, delete shard on all nodes
        if node_id is None:
            for node in self.shard_to_nodes[shard]:
                self.node_to_shards[node].remove(shard)
            del self.shard_to_nodes[shard]
        else:
            self.node_to_shards[node_id].remove(shard)
            self.shard_to_nodes[shard].remove(node_id)
    
    def get_shard_nodes(self, shard):
        # Return a set of nodes
        return self.shard_to_nodes[shard]

    def get_node_shards(self, node_id):
        # Return a set of shards
        return self.node_to_shards[node_id]

shard_node_map = ShardNodeMap()

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

@app.route("/v0/node", methods = ["POST"])
def node_register():
    if not ensure_json():
        return json_error()
    body = request.json
    
    if "node_id" not in body or type(body["node_id"]) != str:
        return generic_error_response(
            "malformed_request",
            "Request body is missing \"node_id\" field or it is of an invalid type",
            status.HTTP_400_BAD_REQUEST,
        )

    # Verify other fields, e.g., list of shard hashes

    node_id = body['node_id']
    shards = set(body['shards'])
    
    shard_node_map.remove_node(node_id)
    shard_node_map.add_node(node_id, shards)

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
    node_url = "Node_URL" #PLACEHOLDER for now
    for i in range(num_chunck):
        chunk = []
        for j in range(num_data_shards):
            chunk.append({
                "ticket": "RANDOM_TICKET" ,
                "node": node_url,
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
    



