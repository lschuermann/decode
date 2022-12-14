defmodule DecodeCoord.Node do
  require Logger
  use GenServer
  import Ecto.Query

  defstruct [
    :node_id,
    :node_url,
    :shards,
    :metrics,
    :liveness_fail_count,
    :reconstruct_queue,
    :reconstruct_tasks,
    :redistribute_queue,
    :redistribute_tasks,
    :parallel_redistribute_limit,
    :parallel_reconstruct_limit
  ]

  # GenServer client interface

  def register(node_id, node_url, shards \\ []) do
    GenServer.start_link(
      __MODULE__,
      {node_id, node_url, shards},
      name: {:via, Registry, {DecodeCoord.Node.Registry, node_id}}
    )
  end

  def register_or_update(node_id, node_url, shards \\ []) do
    register_update_payload = {:register_update, node_url, shards}

    # Try to call into an existing Node through the Node registry:
    res =
      try do
        GenServer.call(
          {:via, Registry, {DecodeCoord.Node.Registry, node_id}},
          register_update_payload
        )
      catch
        :exit, {:noproc, _} -> {:error, :noproc}
      end

    case res do
      {:error, :noproc} ->
        # The process does not exist, try to register it. If this in turn
        # returns that the node was already registered, we may have raced with
        # another process creating it. Call into the returned PID in this case,
        # forwarding any potential error to the caller (the PID might've crashed
        # with the Registry having stale data).
        case __MODULE__.register(node_id, node_url, shards) do
          {:error, {:already_registered, pid}} ->
            case GenServer.call(pid, register_update_payload) do
              :ok -> {:ok, pid}
              v -> v
            end

          {:ok, pid} ->
            {:ok, pid}
        end

      :ok ->
        # Updating an existing instance worked. Return an {:ok, pid} tuple as
        # is returned in other branches:
        {:ok, elem(hd(Registry.lookup(DecodeCoord.Node.Registry, node_id)), 0)}
    end
  end

  def get_node(node_id) do
    [{pid, _}] = Registry.lookup(DecodeCoord.Node.Registry, node_id)
    pid
  end

  def add_shard(pid, shard_digest) do
    GenServer.call(pid, {:add_shard, shard_digest})
  end

  def get_shards(pid) do
    GenServer.call(pid, :get_shards)
  end

  def get_metrics(pid) do
    GenServer.call(pid, :get_metrics)
  end

  def get_url(pid) do
    GenServer.call(pid, :get_url)
  end

  def redistribute(pid, shard_digest, opts \\ []) do
    enqueue = Keyword.get(opts, :enqueue, true)
    GenServer.call(pid, {:request_redistribute, shard_digest, enqueue})
  end

  def reconstruct(pid, shard_digest, opts \\ []) do
    enqueue = Keyword.get(opts, :enqueue, true)
    GenServer.call(pid, {:request_reconstruct, shard_digest, enqueue})
  end

  # GenServer server callbacks

  defp add_shard_int(shard_digest, state) do
    # Also populate the Shard registry with this node
    DecodeCoord.ShardStore.add_node_shard(shard_digest)

    new_shards = [shard_digest | state.shards]
    %DecodeCoord.Node{state | shards: new_shards}
  end

  defp issue_redistribute(state) do
    if MapSet.size(state.redistribute_tasks) <= state.parallel_redistribute_limit and
         :queue.len(state.redistribute_queue) > 0 do
      # Remove a shard from the queue to issue a redistribute request:
      {{:value, redistribute_shard_digest}, popped_queue} = :queue.out(state.redistribute_queue)

      started =
        if not MapSet.member?(state.redistribute_tasks, redistribute_shard_digest) do
          # Run the request in an asynchronous task:
          Task.async(fn ->
            shard_nodes = DecodeCoord.ShardStore.nodes(redistribute_shard_digest)

            if length(shard_nodes) > 0 do
              [source_node | _] = shard_nodes
              {:ok, node_url} = DecodeCoord.Node.get_url(source_node)

              request_payload = %{
                source_node: node_url,
                ticket: "TICKET"
              }

              {
                :shard_redistribute_res,
                redistribute_shard_digest,
                Finch.build(
                  :post,
                  "#{state.node_url}/v0/shard/#{Base.encode16(redistribute_shard_digest)}/fetch",
                  [],
                  Jason.encode_to_iodata!(request_payload)
                )
                |> Finch.request(DecodeCoord.NodeClient,
                  pool_timeout: 600_000,
                  receive_timeout: 600_000
                )
              }
            else
              {
                :shard_redistribute_res,
                redistribute_shard_digest,
                {:error, :no_source_node}
              }
            end
          end)

          true
        else
          false
        end

      {started,
       %DecodeCoord.Node{
         state
         | redistribute_queue: popped_queue,
           redistribute_tasks:
             MapSet.put(
               state.redistribute_tasks,
               redistribute_shard_digest
             )
       }}
    else
      {false, state}
    end
  end

  defp issue_reconstruct(state) do
    if MapSet.size(state.reconstruct_tasks) <= state.parallel_reconstruct_limit and
         :queue.len(state.reconstruct_queue) > 0 do
      # Remove a shard from the queue to issue a reconstruct request:
      {{:value, reconstruct_shard_digest}, popped_queue} = :queue.out(state.reconstruct_queue)

      # We fetch the shard of the chunk to reconstruct, in order to build its
      # shard_map as part of the reconstruct request. However, on the very
      # last chunk, the chunk size may actually be less than the chunk_size
      # parameter of the object. Hence we also join this with the next chunk
      # and if there is not successor, we need to calculate the actual chunk
      # size:
      [db_chunk, has_successor] =
        DecodeCoord.Repo.one(
          from c in DecodeCoord.Objects.Chunk,
            join: s in DecodeCoord.Objects.Shard,
            on: c.id == s.chunk_id,
            left_join: c_succ in DecodeCoord.Objects.Chunk,
            on: c_succ.object_id == c.object_id and c_succ.chunk_index == c.chunk_index + 1,
            where: s.digest == ^reconstruct_shard_digest,
            limit: 1,
            preload: [
              :object,
              shards:
                ^from(
                  s in DecodeCoord.Objects.Shard,
                  order_by: :shard_index
                )
            ],
            select: [c, not is_nil(c_succ)]
        )

      # IO.puts("DB chunk: #{inspect(db_chunk)}")

      started =
        if is_nil(db_chunk) do
          Logger.error(
            "Don't have any chunk corresponding to shard " <>
              "#{Base.encode16(reconstruct_shard_digest)}, ignoring!"
          )

          :no_db_chunk
        else
          {shard_map, {_node_map, node_inv_map}} =
            DecodeCoord.Objects.Chunk.build_shard_map(db_chunk)

          # node_inv_map maps an a sequential and non-sparse sequence of nodes
          # to their PID and URL. Walk the indices and convert it into a list to
          # return to the client:
          node_list =
            for idx <- 0..(map_size(node_inv_map) - 1)//1 do
              {_pid, url} = Map.get(node_inv_map, idx)
              url
            end

          {chunk_size, shard_size} =
            if not has_successor do
              Logger.debug(
                "Chunk #{Base.encode16(reconstruct_shard_digest)} has no " <>
                  "successor, so it may be smaller than object's chunk_size " <>
                  "of #{db_chunk.object.chunk_size}."
              )

              chunk_size =
                rem(
                  db_chunk.object.size,
                  db_chunk.object.chunk_size
                )

              if chunk_size != 0 do
                shard_size = min(chunk_size, db_chunk.object.shard_size)
                {chunk_size, shard_size}
              else
                {db_chunk.object.chunk_size, db_chunk.object.shard_size}
              end
            else
              {db_chunk.object.chunk_size, db_chunk.object.shard_size}
            end

          request_payload = %{
            chunk_size: chunk_size,
            shard_size: shard_size,
            code_ratio_data: db_chunk.object.code_ratio_data,
            code_ratio_parity: db_chunk.object.code_ratio_parity,
            shard_map: shard_map,
            node_map: node_list
          }

          IO.puts("Reconstruct request payload: #{Jason.encode!(request_payload)}")

          # Run the request in an asynchronous task:
          if not MapSet.member?(state.reconstruct_tasks, reconstruct_shard_digest) do
            Task.async(fn ->
              {
                :shard_reconstruct_res,
                reconstruct_shard_digest,
                Finch.build(
                  :post,
                  "#{state.node_url}/v0/shard/#{Base.encode16(reconstruct_shard_digest)}/reconstruct",
                  [],
                  Jason.encode_to_iodata!(request_payload)
                )
                |> Finch.request(DecodeCoord.NodeClient,
                  pool_timeout: 600_000,
                  receive_timeout: 600_000
                )
              }
            end)

            :started
          else
            :already_started
          end
        end

      {started,
       %DecodeCoord.Node{
         state
         | reconstruct_queue: popped_queue,
           reconstruct_tasks:
             MapSet.put(
               state.reconstruct_tasks,
               reconstruct_shard_digest
             )
       }}
    else
      {:limited, state}
    end
  end

  @impl true
  def init({node_id, node_url, shards}) do
    Logger.info("New node with ID #{UUID.binary_to_string!(node_id)} registered")

    # Register the inverse mapping of shards and nodes as well:
    register_res =
      Enum.reduce_while(shards, :ok, fn shard_digest, :ok ->
        case DecodeCoord.ShardStore.add_node_shard(shard_digest) do
          {:ok, _pid} -> {:cont, :ok}
          e -> {:halt, e}
        end
      end)

    # Periodically schedule liveness checks of the node
    Process.send_after(self(), :liveness_check, 5_000)

    state = %DecodeCoord.Node{
      node_id: node_id,
      node_url: node_url,
      shards: shards,
      metrics: nil,
      liveness_fail_count: 0,
      reconstruct_queue: :queue.new(),
      reconstruct_tasks: MapSet.new(),
      redistribute_queue: :queue.new(),
      redistribute_tasks: MapSet.new(),
      parallel_reconstruct_limit:
        Application.fetch_env!(:decode_coord, :parallel_reconstruct_limit),
      parallel_redistribute_limit:
        Application.fetch_env!(:decode_coord, :parallel_redistribute_limit)
    }

    case register_res do
      :ok -> {:ok, state}
      e -> {:error, e}
    end
  end

  # TODO: remove
  @impl true
  def handle_call(:ping, _from, state) do
    {:reply, :pong, state}
  end

  @impl true
  def handle_call({:register_update, node_url, shards}, _from, state) do
    # The shards list passed with register update may be slightly stale from
    # the shards we have currently. Hence we can't simply replace our internal
    # shard map with the provided one. Instead, a safe algorithm seems to be:
    #
    # - add all shards contained in the new payload but not in the current
    #   state to the state and queue up a verification that this shard is
    #   indeed present. Also add this node to the candidates holding a given
    #   shard.
    #
    # - for all shards not in the new payload but in our state, keep them
    #   in the state and queue up a verification that this shard is indeed
    #   present. Also keep this node in the candidates holding a given shard.
    #
    # However, for now we just ignore this case and blindly accept the new
    # state:
    Logger.info(
      "Updating node information of " <>
        "#{UUID.binary_to_string!(state.node_id)} based on incoming register " <>
        "request"
    )

    {:reply, :ok, %DecodeCoord.Node{state | node_url: node_url, shards: shards}}
  end

  @impl true
  def handle_call({:add_shard, shard_digest}, _from, state) do
    {:reply, :ok, add_shard_int(shard_digest, state)}
  end

  @impl true
  def handle_call(:get_shards, _from, state) do
    {:reply, {:ok, state.shards}, state}
  end

  @impl true
  def handle_call(:get_metrics, _from, state) do
    {:reply, {:ok, state.metrics}, state}
  end

  @impl true
  def handle_call(:get_url, _from, state) do
    {:reply, {:ok, state.node_url}, state}
  end

  @impl true
  def handle_call({:request_redistribute, shard_digest, enqueue}, _from, state) do
    if enqueue or MapSet.size(state.redistribute_tasks) <= state.parallel_redistribute_limit do
      enqueued_state = %DecodeCoord.Node{
        state
        | redistribute_queue:
            :queue.in(
              shard_digest,
              state.redistribute_queue
            )
      }

      {started, processed_state} = issue_redistribute(enqueued_state)

      if started do
        # TODO: are we sure that this couldn't start something else in the queue?
        {:reply, {:ok, :started}, processed_state}
      else
        {:reply, {:ok, :enqueued}, processed_state}
      end
    else
      {:reply, {:error, :rate_limited}, state}
    end
  end

  @impl true
  def handle_call({:request_reconstruct, shard_digest, enqueue}, _from, state) do
    if enqueue or MapSet.size(state.reconstruct_tasks) <= state.parallel_reconstruct_limit do
      enqueued_state = %DecodeCoord.Node{
        state
        | reconstruct_queue:
            :queue.in(
              shard_digest,
              state.reconstruct_queue
            )
      }

      {started, processed_state} = issue_reconstruct(enqueued_state)

      case started do
        :started -> {:reply, {:ok, :started}, processed_state}
        :already_started -> {:reply, {:ok, :started}, processed_state}
        :limited -> {:reply, {:ok, :enqueued}, processed_state}
        :no_db_chunk -> {:reply, {:error, :no_db_chunk}, processed_state}
      end
    else
      {:reply, {:error, :rate_limited}, state}
    end
  end

  @impl true
  def handle_info(:liveness_check, state) do
    Task.async(fn ->
      {
        :liveness_check_res,
        Finch.build(:get, "#{state.node_url}/v0/stats")
        |> Finch.request(DecodeCoord.NodeClient)
      }
    end)

    {:noreply, state}
  end

  @impl true
  def handle_info({_ref, {:liveness_check_res, res}}, state) do
    liveness_fail_max = 1

    with(
      {:ok, flinch_resp} <- res,
      {:ok, parsed_body} <- Jason.decode(flinch_resp.body),
      {:ok, load_avg} <- Map.fetch(parsed_body, "load_avg"),
      {:ok, disk_capacity} <- Map.fetch(parsed_body, "disk_capacity"),
      {:ok, disk_free} <- Map.fetch(parsed_body, "disk_free")
    ) do
      Logger.debug(
        "Node #{UUID.binary_to_string!(state.node_id)} passed " <>
          "liveness check."
      )

      # TODO: typecheck metrics

      # Report the metrics to the centralized NodeRank process.
      DecodeCoord.NodeRank.post_metrics(%DecodeCoord.NodeMetrics{
        load_avg: load_avg,
        disk_capacity: disk_capacity,
        disk_free: disk_free
      })

      # Schedule a new liveness check
      Process.send_after(self(), :liveness_check, 5_000)

      {:noreply, %DecodeCoord.Node{state | liveness_fail_count: 0}}
    else
      e ->
        Logger.warn(
          "Liveness check failed for node " <>
            "#{UUID.binary_to_string!(state.node_id)} with error #{inspect(e)}"
        )

        new_state = %DecodeCoord.Node{
          state
          | liveness_fail_count: state.liveness_fail_count + 1
        }

        if new_state.liveness_fail_count > liveness_fail_max do
          Logger.warn(
            "Node #{UUID.binary_to_string!(state.node_id)} " <>
              "exceeded maximum liveness check tries, terminating."
          )

          {:stop, :liveness, new_state}
        else
          # Schedule a new liveness check
          Process.send_after(self(), :liveness_check, 5_000)

          {:noreply, new_state}
        end
    end
  end

  @impl true
  def handle_info(
        {_ref, {:shard_redistribute_res, shard_digest, {:ok, %Finch.Response{status: 200}}}},
        state0
      ) do
    Logger.info("Redistributed shard #{Base.encode16(shard_digest)} successfully!")

    state1 = %DecodeCoord.Node{
      state0
      | redistribute_tasks:
          MapSet.delete(
            state0.redistribute_tasks,
            shard_digest
          )
    }

    state2 = add_shard_int(shard_digest, state1)

    {_, state3} = issue_redistribute(state2)

    {:noreply, state3}
  end

  @impl true
  def handle_info({_ref, {:shard_redistribute_res, shard_digest, request_result}}, state0) do
    Logger.error(
      "Redistribution of shard #{Base.encode16(shard_digest)} failed: #{inspect(request_result)}"
    )

    state1 = %DecodeCoord.Node{
      state0
      | redistribute_tasks:
          MapSet.delete(
            state0.redistribute_tasks,
            shard_digest
          )
    }

    {_, state2} = issue_redistribute(state1)

    {:noreply, state2}
  end

  @impl true
  def handle_info(
        {_ref, {:shard_reconstruct_res, shard_digest, {:ok, %Finch.Response{status: 200}}}},
        state0
      ) do
    Logger.info("Reconstructed shard #{Base.encode16(shard_digest)} successfully!")

    state1 = %DecodeCoord.Node{
      state0
      | reconstruct_tasks:
          MapSet.delete(
            state0.reconstruct_tasks,
            shard_digest
          )
    }

    state2 = add_shard_int(shard_digest, state1)

    {_, state3} = issue_reconstruct(state2)

    {:noreply, state3}
  end

  @impl true
  def handle_info({_ref, {:shard_reconstruct_res, shard_digest, request_result}}, state0) do
    Logger.error(
      "Reconstruction of shard #{Base.encode16(shard_digest)} failed: #{inspect(request_result)}"
    )

    state1 = %DecodeCoord.Node{
      state0
      | reconstruct_tasks:
          MapSet.delete(
            state0.reconstruct_tasks,
            shard_digest
          )
    }

    {_, state2} = issue_reconstruct(state1)

    {:noreply, state2}
  end

  # We ignore process down messages, which are generated by async Tasks we set off:
  @impl true
  def handle_info({:DOWN, _ref, :process, _pid, :normal}, state) do
    {:noreply, state}
  end
end
