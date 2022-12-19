defmodule DecodeCoord.Repo.Migrations.Base do
  use Ecto.Migration

  def change do
    create table(:objects, primary_key: false) do
      add :id, :binary_id, primary_key: true, null: false

      add :size, :bigint, null: false
      add :chunk_size, :bigint, null: false
      add :shard_size, :bigint, null: false
      add :code_ratio_data, :integer, null: false
      add :code_ratio_parity, :integer, null: false

      timestamps()
    end

    create table(:chunks, primary_key: false) do
      # Composite primary key based on object_id (FKEY) and chunk_index:

      # We'd like to use a composite primary key based on object_id (FKEY) and
      # chunk_index. While this can be modeled in EctoSQL using the following
      # migration code, Ecto itself cannot (yet) work through these relations.
      # Hence we add an explict shard_id field and define an appropriate
      # composite foreign key.
      #
      # add :object_id, references(
      # 	:objects,
      # 	column: :object_id,
      # 	type: :binary_id
      # ), primary_key: true, null: false
      # add :chunk_index, :bigint, primary_key: true, null: false

      # "Dummy" id to be used until Ecto gains composite foreign key support:
      add :id, :binary_id, primary_key: true, null: false

      add :object_id, references(:objects, type: :binary_id), null: false
      add :chunk_index, :bigint, null: false

      # TODO: unique constraint on [:object_id, :chunk_index] out lack of
      # composite primary key.
    end

    create table(:shards, primary_key: false) do
      # We'd like to use a composite primary key based on object_id+chunk_index
      # (FKEY) and shard_index. While this can be modeled in EctoSQL using the
      # following migration code, Ecto itself cannot (yet) work through these
      # relations. Hence we add an explict shard_id field and define an
      # appropriate composite foreign key.
      #
      # add :object_id, :binary_id, primary_key: true, null: false
      # add :chunk_index, references(
      # 	:chunks,
      # 	column: :chunk_index,
      # 	type: :bigint,
      # 	with: [object_id: :object_id]
      # ), primary_key: true, null: false
      # add :shard_index, :bigint, primary_key: true, null: false

      # "Dummy" id to be used until Ecto gains composite foreign key support:
      add :id, :binary_id, primary_key: true, null: false

      add :chunk_id, references(:chunks, type: :binary_id), null: false
      add :shard_index, :bigint, null: false

      # TODO: unique constraint on [:object_id, :chunk_index, :shard_index] out
      # lack of composite primary key.

      # Hex digest per shard. We create an index over this, however multiple
      # objects in our system may have identical shards, either by being the
      # identical object, having an identical chunk (rare), or just an
      # identical shard (probably very rare, but easy to construct).
      add :digest, :binary, null: false
    end
  end
end
