defmodule DecodeCoordWeb.NodeView do
  use DecodeCoordWeb, :view
  alias DecodeCoordWeb.NodeView

  def render("index.json", %{node: node}) do
    %{data: render_many(node, NodeView, "node.json")}
  end

  def render("show.json", %{node: node}) do
    %{data: render_one(node, NodeView, "node.json")}
  end

  def render("node.json", %{node: node}) do
    %{
      id: node.id
    }
  end
end
