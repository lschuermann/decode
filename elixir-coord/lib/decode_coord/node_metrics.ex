defmodule DecodeCoord.NodeMetrics do
  require Logger

  defstruct [
    :load_avg, # between 0 and 1
    :disk_capacity, # in bytes
    :disk_free, # in bytes
  ]

  def rank_upload(_, nil), do: 0

  def rank_upload(max_shard_size, %DecodeCoord.NodeMetrics{
	load_avg: load_avg,
	disk_capacity: disk_capacity,
	disk_free: disk_free,
  } = _statistic) do
    cond do
      disk_capacity <= 0 ->
	Logger.warn "Invariant violated: disk capacity must be at least 1 byte"

      disk_free < 0 ->
	Logger.warn "Invariant violated: can't use more than available"
	0

      disk_free > disk_capacity ->
	Logger.warn "Invariant violated: more space free than available"
	0

      max_shard_size > disk_free ->
	# Can't place the object on this node
	0

      true ->
        ((disk_free / disk_capacity) * 10)
	+ (1 - load_avg) * 3
    end
  end

  def rank_download(nil), do: 0

  def rank_download(%DecodeCoord.NodeMetrics{
	load_avg: load_avg,
  } = _statistic) do
    (1 - load_avg)
  end

end
