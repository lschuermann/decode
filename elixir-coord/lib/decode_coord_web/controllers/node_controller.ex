defmodule DecodeCoordWeb.NodeController do
  use DecodeCoordWeb, :controller

  action_fallback DecodeCoordWeb.APIFallbackController

  # PUT /v0/node/:node_id
  def register(conn, %{"node_id" => node_id_str, "node_url" => node_url, "shards" => node_shards_hex} = _params) do
    # Validate that all passed parameters and fields are of the type we expect:
    case {
      UUID.info(node_id_str),
      URI.parse(node_url),
    } do
      # Invalid node id: not a valid UUID
      {{:error, _}, _} ->
	conn
	|> put_status(:bad_request)
	|> put_view(DecodeCoordWeb.ErrorView)
	|> render("bad_request.json",
	  description: "Provided `node_id` is not a valid UUID.")

      # Invalid node URL:
      {_, parsed_url} when
      parsed_url.scheme not in ["http", "https"]
      or parsed_url.host in [nil, ""] ->
	conn
	|> put_status(:bad_request)
	|> put_view(DecodeCoordWeb.ErrorView)
	|> render("bad_request.json",
	  description: "Provided `node_url` is invalid.")

      {_, _} when not is_list(node_shards_hex) ->
	conn
	|> put_status(:bad_request)
	|> put_view(DecodeCoordWeb.ErrorView)
	|> render("bad_request.json", description: "`shards` is not a list.")

      # All single fields seem good
      {{:ok, parsed_uuid}, _} ->
	node_id = Keyword.get(parsed_uuid, :binary)

	# Now try to map the list of provided node shards onto binary terms,
	# failing if they are not hex-encoded:
	case (
	   Enum.reduce_while(node_shards_hex, {:ok, 0, []}, fn shard_hex, {:ok, count, shards_bin} ->
  	    case Base.decode16(shard_hex, case: :mixed) do
	      {:ok, shard_bin} when byte_size(shard_bin) == 32 ->
		{:cont, {:ok, count + 1, [shard_bin | shards_bin]}}
	      {:ok, _shard_invalid_len} -> {:halt, :error}
	      :error -> {:halt, {:error, count}}
            end
	  end)
	) do
	  {:ok, _count, node_shards_bin} ->
	    DecodeCoord.Node.register_or_update(node_id, node_url, node_shards_bin)
	    send_resp(conn, :no_content, "")

	  {:error, index} ->
	    conn
	    |> put_status(:bad_request)
	    |> put_view(DecodeCoordWeb.ErrorView)
	    |> render("bad_request.json", description: "Failed to extract " <>
	      "32-byte digest from string at shards index #{index}.")
	end
    end
  end
end
