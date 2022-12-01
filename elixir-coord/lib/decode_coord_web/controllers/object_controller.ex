defmodule DecodeCoordWeb.ObjectController do
  use DecodeCoordWeb, :controller
  require Logger
  import Ecto.Query

  action_fallback DecodeCoordWeb.APIFallbackController

  # GET /v0/object/:object_id
  def get(conn, %{"object_id" => object_id_str} = _params) do
    case UUID.info(object_id_str) do
      {:error, _} ->
	conn
	|> put_status(:bad_request)
	|> put_view(DecodeCoordWeb.ErrorView)
	|> render("bad_request.json",
	  description: "Provided object ID is not a valid UUID.")

      {:ok, parsed_uuid} ->
	# Extract the parsed, binary UUID
	_object_id = Keyword.get parsed_uuid, :binary

	# Now we can try to query the database against it:
	#
	# TODO: order the chunks and shards by their index
	db_object = DecodeCoord.Repo.one(
	  from o in DecodeCoord.Objects.Object,
	  where: o.id == ^object_id_str,
	  preload: [chunks: :shards]
	)

	# Check if the object exists:
	if db_object == nil do
	  conn
	  |> put_status(:not_found)
	  |> put_view(DecodeCoordWeb.ErrorView)
	  |> render("not_found.json",
	    description: "The requested object was not found.")
	else
	  {shard_map, {_node_map, node_inv_map}} =
	    DecodeCoord.Objects.Object.build_chunk_map db_object

	  # node_inv_map maps an a sequential and non-sparse sequence of nodes
	  # to their PID and URL. Walk the indices and convert it into a list to
	  # return to the client:
	  node_list = for idx <- 0..(map_size(node_inv_map) - 1)//1 do
	    {_pid, url} = Map.get node_inv_map, idx
	    url
	  end

	  # Finally, construct the response:
	  conn
	  |> put_status(:ok)
	  |> render("object_retrieval_map.json",
	    object: db_object, shard_map: shard_map, node_map: node_list)
	end
    end
  end
end
