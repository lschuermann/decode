defmodule DecodeCoord.Objects.Chunk do
  use Ecto.Schema
  import Ecto.Changeset
  require Logger

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id
  schema "chunks" do
    belongs_to :object, DecodeCoord.Objects.Object
    field :chunk_index, :integer

    has_many :shards, DecodeCoord.Objects.Shard
  end

  def build_shard_map(
    %DecodeCoord.Objects.Chunk{shards: shards} = _chunk,
    initial_node_maps \\ {Map.new(), Map.new()}
  ) do
    # Per chunk, go over its shards, updating the node_maps accordingly:
    shards
    |> Enum.map_reduce(initial_node_maps, fn db_shard, node_maps ->
      # Per shard, retrieve the nodes which hold it. If this is a new
      # node, resolve its URL and insert it into the node_maps:
      {shard_nodes, updated_maps} =
        DecodeCoord.ShardStore.nodes(db_shard.digest)
        |> Enum.map_reduce(node_maps,
      fn node_pid, {node_map, node_inv_map} ->
        if Map.has_key? node_map, node_pid do
          # We know this node and can simply return its index, leaving
          # the maps unchanged:
          node_idx = Map.get node_map, node_pid
          {node_idx, {node_map, node_inv_map}}
        else
          # We don't know this node. Try to resolve its URL first. If
          # that fails, don't insert the node into the map:
          url_res =
            try do
              DecodeCoord.Node.get_url node_pid
            catch
              :exit, e ->
        	Logger.warn "Failed to query URL from node: #{e}"
              :error
            end

          case url_res do
            {:ok, node_url} ->
              # Resolving the node URL worked, insert it into the node
              # map and return its index.
              node_idx = map_size node_inv_map
              new_inv_map = Map.put(node_inv_map, node_idx,
        	{node_pid, node_url})
              new_node_map = Map.put(node_map, node_pid, node_idx)
              {node_idx, {new_node_map, new_inv_map}}

              _ ->
              # There was an error resolving the node URL:
              {nil, {node_map, node_inv_map}}
          end
        end
      end)

      # Finally, we can construct the shard retrieval spec:
      {
        %{
          digest: Base.encode16(db_shard.digest),
          nodes: shard_nodes |> Enum.filter(fn n -> not is_nil(n) end),
        },
        updated_maps,
      }
    end)
  end

  @doc false
  def changeset(chunk, attrs) do
    chunk
    |> cast(attrs, [])
    |> validate_required([])
  end
end
