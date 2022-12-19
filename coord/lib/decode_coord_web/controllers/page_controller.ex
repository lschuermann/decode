defmodule DecodeCoordWeb.PageController do
  use DecodeCoordWeb, :controller

  def index(conn, _params) do
    render(conn, "index.html")
  end
end
