from uuid import uuid4
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy.dialects.postgresql import UUID
from database import db

SHA3_256_LENGTH = 32

class Object(db.Model):
    __tablename__ = "objects"

    id = db.Column(UUID, primary_key=True)
    size = db.Column(db.Integer, nullable=False)
    chunk_size = db.Column(db.Integer, nullable=False)
    shard_size = db.Column(db.Integer, nullable=False)
    code_ratio_data = db.Column(db.Integer, nullable=False)
    code_ratio_parity = db.Column(db.Integer, nullable=False)

    def __init__(self):
        pass

    def __repr__(self):
        return '<id {}>'.format(self.id)

    def serialize(self):
        return {
            'id': self.id,
        }

class Chunk(db.Model):
    __tablename__ = "chunks"

    object_id = db.Column(UUID, db.ForeignKey("objects.id"), primary_key=True)
    chunk_index = db.Column(db.Integer, primary_key=True, nullable=False)

class Shard(db.Model):
    __tablename__ = "shards"

    object_id = db.Column(UUID, primary_key=True)
    chunk_index = db.Column(db.Integer, primary_key=True)
    shard_index = db.Column(db.Integer, primary_key=True, nullable=False)
    shard_hash = db.Column(db.LargeBinary(length=SHA3_256_LENGTH), nullable=False)

    __table_args__ = tuple(
        ForeignKeyConstraint(
            [object_id, chunk_index],
            [Chunk.object_id, Chunk.chunk_index]
        )
    )

