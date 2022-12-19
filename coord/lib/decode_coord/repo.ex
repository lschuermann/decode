defmodule DecodeCoord.Repo do
  use Ecto.Repo,
    otp_app: :decode_coord,
    adapter: Ecto.Adapters.Postgres
end
