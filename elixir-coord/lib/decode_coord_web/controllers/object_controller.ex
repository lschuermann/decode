defmodule DecodeCoordWeb.ObjectController do
  use DecodeCoordWeb, :controller
  require Logger
  import Ecto.Query

  action_fallback DecodeCoordWeb.APIFallbackController

  defp ceil_div_int(a, b) do
    div(a + b - 1, b)
  end

  # GET /v0/failed_shards
  def failed_shards(conn, _params) do
    conn
    |> put_status(:ok)
    |> render("failed_shard_list.json",
      failed_shard_set: DecodeCoord.ShardStore.pending_reconstruct()
    )
  end

  # POST /v0/object/
  def post(conn, %{"object_size" => object_size} = _params) do
    object_id = UUID.uuid4()
		max_shard_size = Application.fetch_env!(:decode_coord, :max_shard_size)
    min_shard_size = Application.fetch_env!(:decode_coord, :min_shard_size)
    max_data_shards = Application.fetch_env!(:decode_coord, :max_data_shards)
    num_parity_shards = Application.fetch_env!(:decode_coord, :num_parity_shards)
    max_chunk_size = max_shard_size * max_data_shards
    num_chunk = ceil_div_int(object_size, max_chunk_size)
    size_chunk = ceil_div_int(object_size, num_chunk)
    num_data_shards = min(ceil_div_int(size_chunk, min_shard_size), max_data_shards)
    size_shard = ceil_div_int(size_chunk, num_data_shards)

    shard_map =
      Enum.map(0..(num_chunk - 1), fn _chunk_index ->
        {shards, _excluded_nodes} =
          Enum.map_reduce(
            0..(num_data_shards + num_parity_shards - 1),
            MapSet.new(),
            fn _shard_index, excluded_nodes ->
              nodes = DecodeCoord.NodeRank.get_nodes(1, excluded_nodes)

              if length(nodes) > 0 do
                [{node_pid, _metrics} | _] = nodes
                {:ok, node_url} = DecodeCoord.Node.get_url(node_pid)

                {
                  %{"ticket" => "TICKET", "node" => node_url},
                  # excluded_nodes,
                  MapSet.put(excluded_nodes, node_pid)
                }
              else
                {nil, excluded_nodes}
              end
            end
          )

        shards
      end)

    conn
    |> put_status(:ok)
    |> render("object_upload_map.json", %{
      object_id: object_id,
      object_size: object_size,
      chunk_size: size_chunk,
      shard_size: size_shard,
      code_ratio_data: num_data_shards,
      code_ratio_parity: num_parity_shards,
      shard_map: shard_map
    })
  end

  # PUT /v0/object/{objectId}/finalize
  def put(
        conn,
        %{
          "object_id" => object_id_str,
          "upload_results" => upload_results,
          "object_upload_map" => %{
            "object_size" => object_size,
            "chunk_size" => chunk_size,
            "shard_size" => shard_size,
            "code_ratio_data" => code_ratio_data,
            "code_ratio_parity" => code_ratio_parity
            # "shard_map" => shard_map
          }
        } = _params
      ) do
    # TODO: validate data types, including object_id, etc.
    # TODO: do this in a transaction
    {:ok, object} =
      %DecodeCoord.Objects.Object{
        id: object_id_str,
        size: object_size,
        chunk_size: chunk_size,
        shard_size: shard_size,
        code_ratio_data: code_ratio_data,
        code_ratio_parity: code_ratio_parity
      }
      |> DecodeCoord.Repo.insert()

    upload_results
    |> Enum.with_index()
    |> Enum.each(fn {chunk_results, chunk_idx} ->
      {:ok, chunk} =
        %DecodeCoord.Objects.Chunk{
          object_id: object.id,
          chunk_index: chunk_idx
        }
        |> DecodeCoord.Repo.insert()

      chunk_results
      |> Enum.with_index()
      |> Enum.each(fn {shard_result, shard_idx} ->
        {:ok, _shard} =
          %DecodeCoord.Objects.Shard{
            chunk_id: chunk.id,
            shard_index: shard_idx,
            digest: Base.decode16!(Map.fetch!(shard_result, "digest"), case: :mixed)
          }
          |> DecodeCoord.Repo.insert()

        node_id_str = Map.fetch!(shard_result, "receipt")
        node_id = UUID.string_to_binary!(node_id_str)
        node = DecodeCoord.Node.get_node(node_id)

        DecodeCoord.Node.add_shard(
          node,
          Base.decode16!(Map.fetch!(shard_result, "digest"), case: :mixed)
        )
      end)
    end)

    send_resp(conn, :ok, "")
  end

  # GET /v0/object/:object_id
  def get(conn, %{"object_id" => object_id_str} = _params) do
    case UUID.info(object_id_str) do
      {:error, _} ->
        conn
        |> put_status(:bad_request)
        |> put_view(DecodeCoordWeb.ErrorView)
        |> render("bad_request.json",
          description: "Provided object ID is not a valid UUID."
        )

      {:ok, parsed_uuid} ->
        # Extract the parsed, binary UUID
        _object_id = Keyword.get(parsed_uuid, :binary)

        # Now we can try to query the database against it:
        db_object =
          DecodeCoord.Repo.one(
            from o in DecodeCoord.Objects.Object,
              where: o.id == ^object_id_str,
              preload: [
                chunks:
                  ^from(
                    c in DecodeCoord.Objects.Chunk,
                    order_by: :chunk_index,
                    preload: [
                      shards:
                        ^from(
                          s in DecodeCoord.Objects.Shard,
                          order_by: :shard_index
                        )
                    ]
                  )
              ]
          )

        # Check if the object exists:
        if db_object == nil do
          conn
          |> put_status(:not_found)
          |> put_view(DecodeCoordWeb.ErrorView)
          |> render("not_found.json",
            description: "The requested object was not found."
          )
        else
          {shard_map, {_node_map, node_inv_map}} =
            DecodeCoord.Objects.Object.build_chunk_map(db_object)

          # node_inv_map maps an a sequential and non-sparse sequence of nodes
          # to their PID and URL. Walk the indices and convert it into a list to
          # return to the client:
          node_list =
            for idx <- 0..(map_size(node_inv_map) - 1)//1 do
              {_pid, url} = Map.get(node_inv_map, idx)
              url
            end

          # Finally, construct the response:
          conn
          |> put_status(:ok)
          |> render("object_retrieval_map.json",
            object: db_object,
            shard_map: shard_map,
            node_map: node_list
          )
        end
    end
  end
end
