defmodule DecodeCoord.Objects.Object do
  use Ecto.Schema
  import Ecto.Changeset
  require Logger

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id
  schema "objects" do
    has_many :chunks, DecodeCoord.Objects.Chunk

    field :size, :integer
    field :chunk_size, :integer
    field :shard_size, :integer
    field :code_ratio_data, :integer
    field :code_ratio_parity, :integer

    timestamps()
  end

  def build_chunk_map(
        %DecodeCoord.Objects.Object{chunks: chunks} = _object,
        initial_node_maps \\ {Map.new(), Map.new()}
      ) do
    # Now, build the response. We generate the node and shard map through
    # a map-reduce operation over the returned nodes and chunks. To
    # efficiently build the data structure, we use a Map from node PIDs to
    # sequential indices and node URLs. We can later walk this data
    # structure and convert it into a list.
    chunks
    |> Enum.map_reduce(initial_node_maps, fn db_chunk, node_maps ->
      DecodeCoord.Objects.Chunk.build_shard_map(db_chunk, node_maps)
    end)
  end

  @doc false
  def changeset(object, attrs) do
    object
    |> cast(attrs, [])
    |> validate_required([])
  end
end
