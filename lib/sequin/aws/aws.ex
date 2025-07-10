defmodule Sequin.Aws do
  @moduledoc false
  def message_group_id(group_id), do: hash(group_id)
  def message_deduplication_id(idempotency_key), do: hash(idempotency_key)

  defp hash(value) do
    :md5
    |> :crypto.hash(value)
    |> Base.encode16(case: :lower, padding: false)
  end
end
