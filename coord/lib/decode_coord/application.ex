defmodule DecodeCoord.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # Start the Ecto repository
      DecodeCoord.Repo,
      # Start the Telemetry supervisor
      DecodeCoordWeb.Telemetry,
      # Start the PubSub system
      {Phoenix.PubSub, name: DecodeCoord.PubSub},
      # Start the Endpoint (http/https)
      DecodeCoordWeb.Endpoint,
      # Start a Registry for the DecodeCoord.Node processes managing a node's
      # lifecycle and keeping track of its shards:
      {Registry, keys: :unique, name: DecodeCoord.Node.Registry},
      # We further maintain an inverse mapping of shards to nodes, also in a
      # registry with non-unique keys, through a custom process called
      # ShardStore (backed through a Registry underneath)
      %{id: DecodeCoord.ShardStore, start: {DecodeCoord.ShardStore, :start_link, []}},
      # Start the node ranking
      %{id: DecodeCoord.NodeRank, start: {DecodeCoord.NodeRank, :start_link, []}},
      # Start a Finch HTTP client worker pool for connecting to nodes
      {Finch, name: DecodeCoord.NodeClient, pools: %{default: [size: 100, count: 100]}}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: DecodeCoord.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  @impl true
  def config_change(changed, _new, removed) do
    DecodeCoordWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
