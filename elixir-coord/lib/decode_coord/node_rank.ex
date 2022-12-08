defmodule DecodeCoord.NodeRank do
  require Logger
  use GenServer

  # GenServer client interface

  def start_link() do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  def post_metrics(metrics) do
    GenServer.call(__MODULE__, {:post_metrics, self(), metrics})
  end

  def get_nodes(count \\ nil, excluded_nodes \\ MapSet.new()) do
    GenServer.call(__MODULE__, {:get_nodes, count, excluded_nodes})
  end

  # GenServer server interface

  @impl true
  def init(nil) do
    {:ok, []}
  end

  @impl true
  def handle_call({:post_metrics, node_pid, metrics}, _from, state) do
    {metrics_iter, node_seen} =
      state
      |> Enum.map_reduce(false, fn {node, metric}, node_seen ->
        {{node, metric}, node_seen || node == node_pid}
      end)

    if not node_seen do
      Process.monitor(node_pid)
    end

    {:reply, :ok,
     metrics_iter
     |> Enum.filter(fn {node, _metric} -> node != node_pid end)
     |> then(fn list -> [{node_pid, metrics} | list] end)
     |> Enum.sort_by(
       fn {_node, metric} ->
         # TODO: lookup current max shard size
         DecodeCoord.NodeMetrics.rank_upload(50 * 1024 * 1024, metric)
       end,
       :desc
     )
     |> Enum.to_list()}
  end

  @impl true
  def handle_call({:get_nodes, count, excluded}, _from, state) do
    ranked_nodes =
      state
      |> Enum.filter(fn {node, _metric} -> not MapSet.member?(excluded, node) end)
      |> then(fn enum ->
        if count != nil do
          Enum.take(enum, count)
        else
          enum
        end
      end)
      |> Enum.to_list()

    {:reply, ranked_nodes, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    {
      :noreply,
      state
      |> Enum.filter(fn {node, _metric} -> node != pid end)
      |> Enum.to_list()
    }
  end
end
