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
        IO.puts("Getting inner PID from wrapped for nodes lookup: #{inspect(wrapper_pid)}")
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

      # Select a node for placing the shard, respecting the other
      # shards in the chunk to retain fault tolerance:
      #
      # TODO: limit one chunk
      db_shards =
        DecodeCoord.Repo.all(
          from os in DecodeCoord.Objects.Shard,
            join: c in DecodeCoord.Objects.Chunk,
            on: os.chunk_id == c.id,
            join: is in DecodeCoord.Objects.Shard,
            on: c.id == is.chunk_id,
            where: is.digest == ^shard_digest and os.digest != is.digest,
            order_by: :shard_index
        )

      IO.puts(
        "Node #{inspect(node_pid)} (wrapper #{inspect(wrapper_node_pid)}) down, building excluded nodes"
      )

      excluded_nodes =
        db_shards
        |> Enum.flat_map(fn shard ->
          DecodeCoord.ShardStore.nodes(shard.digest)
        end)
        |> Enum.into(MapSet.new())
        |> MapSet.put(node_pid)

      IO.puts("Node #{inspect(node_pid)}: ranking nodes")

      # TODO: what if this fails?
      {:ok, candidate_nodes} = DecodeCoord.NodeRank.get_nodes(1, excluded_nodes)

      if length(candidate_nodes) < 1 do
        Logger.error(
          "Unable to find a node to reconstruct " <>
            "#{Base.encode16(shard_digest)} on."
        )
      else
        [{reconstruct_node, _metrics} | _] = candidate_nodes

        IO.puts(
          "Reconstructing from node #{inspect(node_pid)} on node #{inspect(reconstruct_node)}"
        )

        DecodeCoord.Node.enqueue_reconstruct(reconstruct_node, shard_digest)
      end
    else
      Logger.debug(
        "Remaining nodes for shard #{Base.encode16(shard_digest)}: #{inspect(remain_nodes)}"
      )
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
      IO.puts("Inner impl")
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
