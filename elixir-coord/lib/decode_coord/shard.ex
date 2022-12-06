defmodule DecodeCoord.ShardStore do
  require Logger
  use GenServer
  import Ecto.Query

  # GenServer client API

  def start_link() do
    GenServer.start_link(__MODULE__, nil, name: DecodeCoord.ShardStore)
  end

  def register_node(shard_digest) do
    DecodeCoord.ShardStore.WrappedNode.start(self(), shard_digest)
  end

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

  def __shard_node_down(wrapper_node_pid, node_pid, shard_digest) do
    # We need to check (excluding the currently reported node's PID)
    # whether we have sufficient nodes left for this shard. If not,
    # queue up a reconstruction request on some other node:
    remain_nodes =
      Registry.lookup(DecodeCoord.ShardStore.Registry, shard_digest)
      |> Enum.map(fn {wrapper_pid, _} -> DecodeCoord.ShardStore.WrappedNode.inner(wrapper_pid) end)
      |> Enum.filter(fn pid -> pid != node_pid end)

    if length(remain_nodes) < 1 do
      Logger.warn("No remaining nodes for shard #{Base.encode16(shard_digest)}!")

      # Select a node for placing the shard, respecting the other shards in the
      # chunk to retain fault tolerance. We need to use a subquery to retrieve
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
      # (identified by its digest) is a member of, now retrieve an ordered set
      # of shards belonging to the selected chunk:
      db_shards =
        DecodeCoord.Repo.all(
          from os in DecodeCoord.Objects.Shard,
            join: c in DecodeCoord.Objects.Chunk,
            as: :chunk,
            on: os.chunk_id == c.id,
            where: exists(subquery(missing_shard_chunk_query)),
            order_by: :shard_index
        )

      excluded_nodes =
        db_shards
        |> Enum.flat_map(fn shard ->
          DecodeCoord.ShardStore.nodes(shard.digest)
        end)
        |> Enum.into(MapSet.new())
        |> MapSet.put(node_pid)

      # TODO: what if this fails?
      {:ok, candidate_nodes} = DecodeCoord.NodeRank.get_nodes(1, excluded_nodes)

      if length(candidate_nodes) < 1 do
        Logger.error(
          "Unable to find a node to reconstruct " <>
            "#{Base.encode16(shard_digest)} on."
        )
      else
        [{reconstruct_node, _metrics} | _] = candidate_nodes

        Logger.debug(
          "Reconstructing #{Base.encode16(shard_digest)}, originally on node " <>
            "#{inspect(node_pid)}, onto node #{inspect(reconstruct_node)}."
        )

        DecodeCoord.Node.enqueue_reconstruct(reconstruct_node, shard_digest)
      end
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

      # Try to select "twin" shards of the queried chunk. If we have at least
      # one, we should retain `code_ratio_data` + `code_ratio_parity`
      # copies.
      twin_chunk_shard =
        DecodeCoord.Repo.one(
          from s in DecodeCoord.Objects.Shard,
            as: :twin_shard,
            join: c in DecodeCoord.Objects.Chunk,
            as: :chunk,
            on: s.chunk_id == c.id,
            where: s.digest == ^shard_digest,
            where: exists(subquery(missing_shard_chunk_query)),
            preload: [chunk: :object],
            limit: 1
        )

      if twin_chunk_shard != nil do
        if length(remain_nodes) <
             twin_chunk_shard.chunk.object.code_ratio_data +
               twin_chunk_shard.chunk.object.code_ratio_parity do
          Logger.debug(
            "Shard #{Base.encode16(shard_digest)} has twins in the same " <>
              "chunk and is missing copies, reconstructing."
          )

          # TODO: perhaps schedule reconstruction on more than one node if we
          # know we're missing multiple?

          # TODO: this code is duplicated.
          excluded_nodes =
            remain_nodes
            |> Enum.into(MapSet.new())
            |> MapSet.put(node_pid)

          # TODO: what if this fails?
          {:ok, candidate_nodes} = DecodeCoord.NodeRank.get_nodes(1, excluded_nodes)

          if length(candidate_nodes) < 1 do
            Logger.error(
              "Unable to find a node to reconstruct " <>
                "#{Base.encode16(shard_digest)} on."
            )
          else
            [{redistribute_node, _metrics} | _] = candidate_nodes

            Logger.debug(
              "Redistributing #{Base.encode16(shard_digest)}, originally on node " <>
                "#{inspect(node_pid)}, onto node #{inspect(redistribute_node)}."
            )

            DecodeCoord.Node.enqueue_redistribute(redistribute_node, shard_digest)
          end
        else
          Logger.debug(
            "Shard #{Base.encode16(shard_digest)} has twin shards in the " <>
              "same chunk which are on a sufficient number of nodes (" <>
              "#{length(remain_nodes)} vs. " <>
              "#{twin_chunk_shard.chunk.object.code_ratio_data} + " <>
              "#{twin_chunk_shard.chunk.object.code_ratio_parity})."
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

    {:ok, nil}
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
        self(),
        monitored_node,
        shard_digest
      )

      {:stop, :shutdown, {monitored_node, shard_digest}}
    end
  end
end
