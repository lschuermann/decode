defmodule DecodeCoordWeb.ObjectView do
  use DecodeCoordWeb, :view
  alias DecodeCoordWeb.ObjectView

  def render("object_retrieval_map.json", %{object: object, shard_map: shard_map, node_map: node_map}) do
    %{
      object_size: object.size,
      chunk_size: object.chunk_size,
      shard_size: object.shard_size,
      code_ratio_data: object.code_ratio_data,
      code_ratio_parity: object.code_ratio_parity,
      shard_map: shard_map,
      node_map: node_map,
    }
  end
end
