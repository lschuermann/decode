defmodule DecodeCoordWeb.ObjectView do
  use DecodeCoordWeb, :view
  alias DecodeCoordWeb.ObjectView

  def render("failed_shard_list.json", %{failed_shard_set: failed_shard_set}) do
    failed_shard_set |> Enum.to_list()
  end

  def render("object_retrieval_map.json", %{
        object: object,
        shard_map: shard_map,
        node_map: node_map
      }) do
    download_map = %{
      object_size: object.size,
      chunk_size: object.chunk_size,
      shard_size: object.shard_size,
      code_ratio_data: object.code_ratio_data,
      code_ratio_parity: object.code_ratio_parity,
      shard_map: shard_map,
      node_map: node_map
    }

    IO.puts("Download map: #{inspect(download_map)}")

    download_map
  end

  def render("object_upload_map.json", assigns) do
    %{
      object_id: assigns.object_id,
      object_size: assigns.object_size,
      chunk_size: assigns.chunk_size,
      shard_size: assigns.shard_size,
      code_ratio_data: assigns.code_ratio_data,
      code_ratio_parity: assigns.code_ratio_parity,
      shard_map: assigns.shard_map,
      signature: ""
    }
  end
end
