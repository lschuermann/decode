defmodule DecodeCoordWeb.APIFallbackController do
  use DecodeCoordWeb, :controller

  def call(conn, {:error, :not_found}) do
    conn
    |> put_status(:not_found)
    |> put_view(DecodeCoordWeb.APIErrorView)
    |> render(:"404")
  end
end
