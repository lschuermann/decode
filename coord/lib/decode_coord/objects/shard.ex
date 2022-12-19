defmodule DecodeCoord.Objects.Shard do
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id
  schema "shards" do
    belongs_to :chunk, DecodeCoord.Objects.Chunk
    field :shard_index, :integer
    field :digest, :binary
  end

  @doc false
  def changeset(shard, attrs) do
    shard
    |> cast(attrs, [])
    |> validate_required([])
  end
end
