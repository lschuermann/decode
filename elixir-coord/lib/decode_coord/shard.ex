defmodule DecodeCoord.ShardStore do
  require Logger
  use GenServer
  import Ecto.Query

  defstruct [
    # All shards which are pending a reconstruct or redistribute operation to
    # become "healthy" again, regardless of whether the operation was started.
    :pending_reconstruct_set,

    # Queue of pending (not started) reconstruct or redistribute operations.
    #
    # This queue is used for both reconstruction and redistribution of
    # shards. It holds tuples of
    #
    #     (
    #       << shard_digest >>,
    #       :reconstruct / :redistribute,
    #       [ << chunk shards >> ],
    #       required_copies,
    #     )
    #
    # to indicate which operation should be performed. If a :redistribute
    # operation failed, it shall be retried using :reconstruct.
    :pending_reconstruct_queue,

    # Map of issued reconstruction operations, mapping the shard digest to the
    # node PID on which the reconstruction was issued. This needs to be taken
    # into account when scheduling further reconstructions, as to avoid placing
    # multiple shards of a single chunk onto the same node.
    :issued_reconstruct_operations
  ]

  # GenServer client API

  def start_link() do
    GenServer.start_link(__MODULE__, nil, name: DecodeCoord.ShardStore)
  end

  # Add the caller of this method as a node holding the shard.
  def add_node_shard(shard_digest) do
    # Remove this shard from the pending_reconstruct_set and the
    # issued_reconstruct_operations.
    #
    # TODO: also integrate a mechanism to call back to the ShardStore for when
    # such an operation failed.
    #
    # TODO: actually validate that we have a sufficient number of shards now.
    GenServer.call(DecodeCoord.ShardStore, {:add_shard_node, shard_digest, self()})

    # Then add a WrappedNode pointing to this node to the internal registry:
    DecodeCoord.ShardStore.WrappedNode.start(self(), shard_digest)

    # Check whether this enables us to process any more reconstructions:
    GenServer.cast(DecodeCoord.ShardStore, :issue_reconstruct_reqs)
  end

  # Get all nodes which are known to have a shard digest. This runs in the
  # context of the calling processes and calls directly into the sharded
  # registry as to not cause congestion on the shard store.
  def nodes(shard_digest) do
    Registry.lookup(DecodeCoord.ShardStore.Registry, shard_digest)
    |> Enum.map(fn {wrapper_pid, _} ->
      try do
        DecodeCoord.ShardStore.WrappedNode.inner(wrapper_pid)
      catch
        :exit, _ -> nil
      end
    end)
    |> Enum.filter(fn pid -> pid != nil end)
    # TODO: make async
    |> Enum.map(fn pid ->
      try do
        {pid, DecodeCoord.Node.get_metrics(pid)}
      catch
        :exit, _ -> {pid, nil}
      end
    end)
    |> Enum.filter(fn result ->
      case result do
        {_pid, {:ok, _metrics}} -> true
        {_pid, _} -> false
      end
    end)
    |> Enum.sort_by(fn {_pid, {:ok, metrics}} ->
      DecodeCoord.NodeMetrics.rank_download(metrics)
    end)
    |> Enum.map(fn {pid, _} -> pid end)
  end

  # Return a set of shards which are pending reconstruction (including those for
  # which reconstruction was already started).
  def pending_reconstruct() do
    GenServer.call(DecodeCoord.ShardStore, :get_pending_reconstruct)
  end

  # Helpers called from foreign processes

  # This function is called by the WrappedNode implementation when a node is
  # down, for every shard in that node. This should be reworked to also use a
  # queue, to avoid excessive parallel requests to the database.
  def __shard_node_down(node_pid, shard_digest) do
    # TODO: make sure that the shards actually belong to a chunk which we hold
    # in our database!

    # We need to check (excluding the currently reported node's PID) whether we
    # have sufficient nodes left for this shard. If not, queue up a
    # reconstruction request on some other node:
    remain_nodes =
      Registry.lookup(DecodeCoord.ShardStore.Registry, shard_digest)
      |> Enum.map(fn {wrapper_pid, _} -> DecodeCoord.ShardStore.WrappedNode.inner(wrapper_pid) end)
      |> Enum.filter(fn pid -> pid != node_pid end)

    if length(remain_nodes) < 1 do
      Logger.warn(
        "No remaining nodes for shard #{Base.encode16(shard_digest)}, " <>
          "requesting reconstruction."
      )

      # Query other shards in this chunk to avoid placing onto a node which
      # already has copies of this shard. We need to use a subquery to retrieve
      # the chunk first, as we can have multiple shards defined over the same
      # digest. We need only perform the reconstruction once per shard digest,
      # not per database shard:
      missing_shard_chunk_query =
        from(s in DecodeCoord.Objects.Shard,
          where: s.digest == ^shard_digest,
          where: parent_as(:chunk).id == s.chunk_id,
          limit: 1
        )

      # With the subquery retrieving a single chunk of which this shard
      # (identified by its digest) is a member of, now retrieve the set of all
      # shards belonging to the selected chunk:
      chunk_shard_digests =
        DecodeCoord.Repo.all(
          from s in DecodeCoord.Objects.Shard,
            join: c in DecodeCoord.Objects.Chunk,
            as: :chunk,
            on: s.chunk_id == c.id,
            where: exists(subquery(missing_shard_chunk_query)),
            select: [s.digest]
        )
        |> List.flatten()

      GenServer.call(
        DecodeCoord.ShardStore,
        {:request_reconstruct, shard_digest, :reconstruct, chunk_shard_digests, 1}
      )
    else
      Logger.debug(
        "Remaining nodes for shard #{Base.encode16(shard_digest)}: " <>
          "#{inspect(remain_nodes)}."
      )

      # We need to use a subquery to retrieve the chunk first, as we can have
      # multiple shards defined over the same digest. We need only perform the
      # reconstruction once per shard digest, not per database shard:
      missing_shard_chunk_query =
        from(
          from s in DecodeCoord.Objects.Shard,
            where: s.digest == ^shard_digest,
            where: parent_as(:chunk).id == s.chunk_id,
            where: parent_as(:twin_shard).id != s.id,
            limit: 1
        )

      # Try to select "twin" shards of the queried chunk. We should have at
      # least as many copies as there are twin shards.
      twin_chunk_shards =
        DecodeCoord.Repo.all(
          from s in DecodeCoord.Objects.Shard,
            as: :twin_shard,
            join: c in DecodeCoord.Objects.Chunk,
            as: :chunk,
            on: s.chunk_id == c.id,
            where: s.digest == ^shard_digest,
            where: exists(subquery(missing_shard_chunk_query)),
            select: [s.id]
        )

      if length(twin_chunk_shards) > 0 do
        if length(remain_nodes) < length(twin_chunk_shards) do
          Logger.debug(
            "Shard #{Base.encode16(shard_digest)} has twins in the same " <>
              "chunk and is missing copies, requesting redistribution."
          )

          # It is possible to have a subset of the shards of a chunk be twin
          # shards, while other shards have a differnet hash. Hence we need to
          # issue one more query to get all shards of the chunk.
          #
          # TODO: this is a duplication of the code from the previous branch:
          missing_shard_chunk_query =
            from(s in DecodeCoord.Objects.Shard,
              where: s.digest == ^shard_digest,
              where: parent_as(:chunk).id == s.chunk_id,
              limit: 1
            )

          chunk_shard_digests =
            DecodeCoord.Repo.all(
              from s in DecodeCoord.Objects.Shard,
                join: c in DecodeCoord.Objects.Chunk,
                as: :chunk,
                on: s.chunk_id == c.id,
                where: exists(subquery(missing_shard_chunk_query)),
                select: [s.digest]
            )
            |> List.flatten()

          GenServer.call(
            DecodeCoord.ShardStore,
            {:request_reconstruct, shard_digest, :redistribute, chunk_shard_digests,
             length(twin_chunk_shards)}
          )
        else
          Logger.debug(
            "Shard #{Base.encode16(shard_digest)} has " <>
              "#{length(twin_chunk_shards)} twin shards in the same chunk " <>
              "which are on a sufficient number of nodes."
          )
        end
      else
        Logger.debug(
          "Shard #{Base.encode16(shard_digest)} has no twin shards in the " <>
            "same chunk, hence no need to reconstruct / redistribute."
        )
      end
    end
  end

  # GenServer server API

  @impl true
  def init(nil) do
    # Start the shard->node registry as a linked child. When it
    # crashes, it's fine to take this process with it. The shard store
    # is going to be registered under the application supervisor:
    Registry.start_link(keys: :duplicate, name: DecodeCoord.ShardStore.Registry)

    {:ok,
     %DecodeCoord.ShardStore{
       pending_reconstruct_set: MapSet.new(),
       pending_reconstruct_queue: {0, :queue.new()},
       issued_reconstruct_operations: Map.new()
     }}
  end

  # This does not actually add a shard to the registry (as we cannot register a
  # foreign process in a registry). The caller function is responsible for
  # adding a node to the registry instead. The purpose behind this message is to
  # update any pending reconstruction record for this shard.
  @impl true
  def handle_call({:add_shard_node, shard_digest, _pid}, _from, state) do
    %DecodeCoord.ShardStore{
      state
      | pending_reconstruct_set: MapSet.delete(state.pending_reconstruct_set, shard_digest),
        issued_reconstruct_operations:
          Map.delete(state.issued_reconstruct_operations, shard_digest)
    }
    |> then(fn new_state -> {:reply, :ok, new_state} end)
  end

  @impl true
  def handle_call(
        {:request_reconstruct, shard_digest, mechanism, other_chunk_shards, required_copies},
        _from,
        state
      ) do
    {len, queue} = state.pending_reconstruct_queue

    %DecodeCoord.ShardStore{
      state
      | pending_reconstruct_queue: {
          len + 1,
          :queue.in(
            {shard_digest, mechanism, other_chunk_shards, required_copies},
            queue
          )
        },
        pending_reconstruct_set: MapSet.put(state.pending_reconstruct_set, shard_digest)
    }
    |> issue_reconstruct_reqs
    |> then(fn new_state -> {:reply, :ok, new_state} end)
  end

  @impl true
  def handle_call(:get_pending_reconstruct, _from, state) do
    {:reply, state.pending_reconstruct_set, state}
  end

  @impl true
  def handle_cast(:issue_reconstruct_reqs, state) do
    {:noreply, issue_reconstruct_reqs(state)}
  end

  # Internal helpers called from the ShardStore process

  # Returns a list of nodes which either hold a shard or will potentially hold a
  # shard in the future based on currently running reconstruct operations.
  defp potential_nodes(shard_digest, state) do
    # It's fine for us to call into the ShardStore.nodes function here, as it
    # will not issue messages to the ShardStore process (ourselves). We use the
    # reduce function to efficiently prepend elements from our set of running
    # reconstruct operations to the returned list.
    nodes = DecodeCoord.ShardStore.nodes(shard_digest)

    if Map.has_key?(state.issued_reconstruct_operations, shard_digest) do
      [Map.get(state.issued_reconstruct_operations, shard_digest) | nodes]
    else
      nodes
    end
  end

  # To prevent excluding nodes from being reconstruction targets because of
  # redistribution, per shard we must only ever exclude a single node. However,
  # we must further respect the fault tolerance constraints and not exclude a
  # node which was already excluded for a different shard.
  #
  # When we walk the set of chunk shards, removing one, and selecting a single
  # node to exclude which is not already in the set of excluded nodes. This
  # process is done recursively until a single distinct node has been excluded
  # for every chunk. This retains fault tolerance while being tolerant of
  # excessive redistribution.
  defp build_excluded_nodes(_, [], excluded), do: excluded

  defp build_excluded_nodes(state, chunk_shards, excluded) do
    # chunk_shards is guaranteed to have at least one element here:
    [shard | remaining_chunk_shards] = chunk_shards

    # As this function is used to build a set of excluded nodes for scheduling
    # reconstruction or redistribution, it's important that we take currently
    # running reconstructions and redistributions into account. Hence use
    # `potential_nodes` here:
    excluded_node =
      potential_nodes(shard, state)
      |> Enum.find(fn node -> not MapSet.member?(excluded, node) end)

    new_excluded =
      if not is_nil(excluded_node) do
        MapSet.put(excluded, excluded_node)
      else
        excluded
      end

    build_excluded_nodes(state, remaining_chunk_shards, new_excluded)
  end

  defp try_schedule_reconstruct(reconstruct_node, shard_digest, method) do
    try do
      result =
        case method do
          :reconstruct ->
            DecodeCoord.Node.reconstruct(reconstruct_node, shard_digest, enqueue: false)

          :redistribute ->
            DecodeCoord.Node.redistribute(reconstruct_node, shard_digest, enqueue: false)

          _ ->
            raise ArgumentError, message: "Invalid reconstruct method: #{inspect(method)}"
        end

      case result do
        {:ok, :started} -> true
        {:error, :rate_limited} -> false
      end
    catch
      :exit, e ->
        Logger.warn(
          "Failed to schedule reconstruction on node " <>
            "#{inspect(reconstruct_node)}: #{inspect(e)}, excluding."
        )

        # This will exclude the node from scheduling another
        # reconstruction operation:
        false
    end
  end

  defp try_schedule_reconstruct_nodes([], busy_nodes, _, _) do
    {:not_started, busy_nodes}
  end

  defp try_schedule_reconstruct_nodes(nodes, busy_nodes, shard_digest, method) do
    [node | remaining_nodes] = nodes

    if try_schedule_reconstruct(node, shard_digest, method) do
      {{:started, node}, MapSet.new()}
    else
      try_schedule_reconstruct_nodes(
        remaining_nodes,
        MapSet.put(busy_nodes, node),
        shard_digest,
        method
      )
    end
  end

  defp issue_reconstruct_reqs(
         state,
         busy_reconstruct_nodes \\ MapSet.new(),
         busy_redistribute_nodes \\ MapSet.new()
       ) do
    {len, _queue} = state.pending_reconstruct_queue
    issue_reconstruct_reqs(state, busy_reconstruct_nodes, busy_redistribute_nodes, len)
  end

  defp issue_reconstruct_reqs(state, _, _, 0), do: state

  defp issue_reconstruct_reqs(
         state,
         busy_reconstruct_nodes,
         busy_redistribute_nodes,
         pending_count
       ) do
    {len, queue} = state.pending_reconstruct_queue

    case :queue.out(queue) do
      {{:value, {shard_digest, method, chunk_shards, _required_copies} = queue_entry},
       popped_queue} ->
        busy_excluded =
          case method do
            :reconstruct -> busy_reconstruct_nodes
            :redistribute -> busy_redistribute_nodes
            _ -> MapSet.new()
          end

        excluded_nodes = build_excluded_nodes(state, chunk_shards, busy_excluded)

        candidate_nodes =
          DecodeCoord.NodeRank.get_nodes(1, excluded_nodes)
          |> Enum.map(fn {node, _metrics} -> node end)

        no_nodes =
          if length(candidate_nodes) == 0 and MapSet.size(busy_excluded) == 0 do
            # Even without any busy nodes we couldn't find any nodes to start a
            # reconstruct operation on. Issue a warning:
            Logger.warn(
              "No nodes to reconstruct shard " <>
                "#{Base.encode16(shard_digest)} on, giving up."
            )

            # TODO: Ideally we'd maintain a separate set of shards we've been
            # failing to find a node to reconstruct them on (if started == false).
            true
          else
            false
          end

        {res, new_busy_nodes} =
          try_schedule_reconstruct_nodes(candidate_nodes, busy_excluded, shard_digest, method)

        new_state =
          case res do
            {:started, node} ->
              %DecodeCoord.ShardStore{
                state
                | pending_reconstruct_queue: {len - 1, popped_queue},
                  issued_reconstruct_operations:
                    Map.put(state.issued_reconstruct_operations, shard_digest, node)
              }

            :not_started ->
              if no_nodes do
                # We didn't have any nodes for this shard and won't have any
                # until a new node registers. Remove this element from the
                # workqueue:
                %DecodeCoord.ShardStore{
                  state
                  | pending_reconstruct_queue: {len - 1, popped_queue}
                }
              else
                # We can't determine that we have no nodes for this chunk here,
                # as we've excluded some nodes as busy.
                Logger.debug(
                  "Cannot schedule reconstruction for shard " <>
                    "#{Base.encode16(shard_digest)} right now, waiting until " <>
                    "a node is no longer busy."
                )

                # Reinsert them at the back of the queue. This won't result in
                # infinite recursion, as we're decreasing `pending_count`, which
                # limits us to iterating over the workqueue once:
                reinserted_queue = :queue.in_r(queue_entry, popped_queue)

                %DecodeCoord.ShardStore{
                  state
                  | pending_reconstruct_queue: {len, reinserted_queue}
                }
              end
          end

        case method do
          :reconstruct ->
            issue_reconstruct_reqs(
              new_state,
              new_busy_nodes,
              busy_redistribute_nodes,
              pending_count - 1
            )

          :redistribute ->
            issue_reconstruct_reqs(
              new_state,
              busy_reconstruct_nodes,
              new_busy_nodes,
              pending_count - 1
            )

          _ ->
            issue_reconstruct_reqs(
              new_state,
              busy_reconstruct_nodes,
              busy_redistribute_nodes,
              pending_count - 1
            )
        end

      {:empty, _queue} ->
        if pending_count != 0 do
          raise(
            ArgumentError,
            "Inconsistency between queue length (#{:queue.len(queue)}) and " <>
              "pending_count (#{pending_count})"
          )
        end

        state
    end
  end

  defmodule WrappedNode do
    use GenServer

    def start(monitored_node, shard_digest) do
      GenServer.start(__MODULE__, {monitored_node, shard_digest})
    end

    def inner(pid) do
      GenServer.call(pid, :inner)
    end

    @impl true
    def init({monitored_node, shard_digest}) do
      Process.monitor(monitored_node)
      Registry.register(DecodeCoord.ShardStore.Registry, shard_digest, nil)
      {:ok, {monitored_node, shard_digest}}
    end

    @impl true
    def handle_call(:inner, _from, {monitored_node, shard_digest}) do
      {:reply, monitored_node, {monitored_node, shard_digest}}
    end

    @impl true
    def handle_info(
          {:DOWN, _ref, :process, pid, _reason},
          {monitored_node, shard_digest}
        )
        when pid == monitored_node do
      Logger.warn(
        "Node #{inspect(monitored_node)} holding shard " <>
          "#{Base.encode16(shard_digest)} is down, notifying ShardStore."
      )

      Registry.unregister(DecodeCoord.ShardStore.Registry, shard_digest)

      DecodeCoord.ShardStore.__shard_node_down(
        monitored_node,
        shard_digest
      )

      {:stop, :shutdown, {monitored_node, shard_digest}}
    end
  end
end
