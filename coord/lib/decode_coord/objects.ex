defmodule DecodeCoord.Objects do
  @moduledoc """
  The Objects context.
  """

  import Ecto.Query, warn: false
  alias DecodeCoord.Repo

  alias DecodeCoord.Objects.Object

  @doc """
  Returns the list of objects.

  ## Examples

      iex> list_objects()
      [%Object{}, ...]

  """
  def list_objects do
    Repo.all(Object)
  end

  @doc """
  Gets a single object.

  Raises `Ecto.NoResultsError` if the Object does not exist.

  ## Examples

      iex> get_object!(123)
      %Object{}

      iex> get_object!(456)
      ** (Ecto.NoResultsError)

  """
  def get_object!(id), do: Repo.get!(Object, id)

  @doc """
  Creates a object.

  ## Examples

      iex> create_object(%{field: value})
      {:ok, %Object{}}

      iex> create_object(%{field: bad_value})
      {:error, %Ecto.Changeset{}}

  """
  def create_object(attrs \\ %{}) do
    %Object{}
    |> Object.changeset(attrs)
    |> Repo.insert()
  end

  @doc """
  Updates a object.

  ## Examples

      iex> update_object(object, %{field: new_value})
      {:ok, %Object{}}

      iex> update_object(object, %{field: bad_value})
      {:error, %Ecto.Changeset{}}

  """
  def update_object(%Object{} = object, attrs) do
    object
    |> Object.changeset(attrs)
    |> Repo.update()
  end

  @doc """
  Deletes a object.

  ## Examples

      iex> delete_object(object)
      {:ok, %Object{}}

      iex> delete_object(object)
      {:error, %Ecto.Changeset{}}

  """
  def delete_object(%Object{} = object) do
    Repo.delete(object)
  end

  @doc """
  Returns an `%Ecto.Changeset{}` for tracking object changes.

  ## Examples

      iex> change_object(object)
      %Ecto.Changeset{data: %Object{}}

  """
  def change_object(%Object{} = object, attrs \\ %{}) do
    Object.changeset(object, attrs)
  end

  alias DecodeCoord.Objects.Chunk

  @doc """
  Returns the list of chunks.

  ## Examples

      iex> list_chunks()
      [%Chunk{}, ...]

  """
  def list_chunks do
    Repo.all(Chunk)
  end

  @doc """
  Gets a single chunk.

  Raises `Ecto.NoResultsError` if the Chunk does not exist.

  ## Examples

      iex> get_chunk!(123)
      %Chunk{}

      iex> get_chunk!(456)
      ** (Ecto.NoResultsError)

  """
  def get_chunk!(id), do: Repo.get!(Chunk, id)

  @doc """
  Creates a chunk.

  ## Examples

      iex> create_chunk(%{field: value})
      {:ok, %Chunk{}}

      iex> create_chunk(%{field: bad_value})
      {:error, %Ecto.Changeset{}}

  """
  def create_chunk(attrs \\ %{}) do
    %Chunk{}
    |> Chunk.changeset(attrs)
    |> Repo.insert()
  end

  @doc """
  Updates a chunk.

  ## Examples

      iex> update_chunk(chunk, %{field: new_value})
      {:ok, %Chunk{}}

      iex> update_chunk(chunk, %{field: bad_value})
      {:error, %Ecto.Changeset{}}

  """
  def update_chunk(%Chunk{} = chunk, attrs) do
    chunk
    |> Chunk.changeset(attrs)
    |> Repo.update()
  end

  @doc """
  Deletes a chunk.

  ## Examples

      iex> delete_chunk(chunk)
      {:ok, %Chunk{}}

      iex> delete_chunk(chunk)
      {:error, %Ecto.Changeset{}}

  """
  def delete_chunk(%Chunk{} = chunk) do
    Repo.delete(chunk)
  end

  @doc """
  Returns an `%Ecto.Changeset{}` for tracking chunk changes.

  ## Examples

      iex> change_chunk(chunk)
      %Ecto.Changeset{data: %Chunk{}}

  """
  def change_chunk(%Chunk{} = chunk, attrs \\ %{}) do
    Chunk.changeset(chunk, attrs)
  end

  alias DecodeCoord.Objects.Shard

  @doc """
  Returns the list of shards.

  ## Examples

      iex> list_shards()
      [%Shard{}, ...]

  """
  def list_shards do
    Repo.all(Shard)
  end

  @doc """
  Gets a single shard.

  Raises `Ecto.NoResultsError` if the Shard does not exist.

  ## Examples

      iex> get_shard!(123)
      %Shard{}

      iex> get_shard!(456)
      ** (Ecto.NoResultsError)

  """
  def get_shard!(id), do: Repo.get!(Shard, id)

  @doc """
  Creates a shard.

  ## Examples

      iex> create_shard(%{field: value})
      {:ok, %Shard{}}

      iex> create_shard(%{field: bad_value})
      {:error, %Ecto.Changeset{}}

  """
  def create_shard(attrs \\ %{}) do
    %Shard{}
    |> Shard.changeset(attrs)
    |> Repo.insert()
  end

  @doc """
  Updates a shard.

  ## Examples

      iex> update_shard(shard, %{field: new_value})
      {:ok, %Shard{}}

      iex> update_shard(shard, %{field: bad_value})
      {:error, %Ecto.Changeset{}}

  """
  def update_shard(%Shard{} = shard, attrs) do
    shard
    |> Shard.changeset(attrs)
    |> Repo.update()
  end

  @doc """
  Deletes a shard.

  ## Examples

      iex> delete_shard(shard)
      {:ok, %Shard{}}

      iex> delete_shard(shard)
      {:error, %Ecto.Changeset{}}

  """
  def delete_shard(%Shard{} = shard) do
    Repo.delete(shard)
  end

  @doc """
  Returns an `%Ecto.Changeset{}` for tracking shard changes.

  ## Examples

      iex> change_shard(shard)
      %Ecto.Changeset{data: %Shard{}}

  """
  def change_shard(%Shard{} = shard, attrs \\ %{}) do
    Shard.changeset(shard, attrs)
  end
end
