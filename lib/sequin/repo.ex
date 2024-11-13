defmodule Sequin.Repo do
  use Ecto.Repo,
    otp_app: :sequin,
    adapter: Ecto.Adapters.Postgres

  @doc """
  A small wrapper around `Repo.transaction/2'.

  Commits the transaction if the lambda returns `:ok` or `{:ok, result}`,
  rolling it back if the lambda returns `:error` or `{:error, reason}`. In both
  cases, the function returns the result of the lambda.
  """
  @spec transact((-> any()), keyword()) :: {:ok, any()} | {:error, any()}
  def transact(fun, opts \\ []) do
    transaction(
      fn ->
        case fun.() do
          {:ok, value} -> value
          :ok -> :transaction_committed
          {:error, reason} -> rollback(reason)
          :error -> rollback(:transaction_rollback_error)
        end
      end,
      opts
    )
  end

  def lazy_preload(%_{} = entity, field_names) do
    # For some reason, in test, when we manually set fields on a struct,
    # sometimes Ecto will still try to preload them.
    Enum.reduce(field_names, entity, fn
      {field_name, child_field_names}, entity ->
        entity
        |> maybe_preload(field_name)
        |> Map.update!(field_name, fn
          nil ->
            nil

          sub_entity ->
            lazy_preload(sub_entity, child_field_names)
        end)

      field_name, entity ->
        maybe_preload(entity, field_name)
    end)
  end

  defp maybe_preload(entity, field_name) do
    value = Map.fetch!(entity, field_name)

    if is_struct(value, Ecto.Association.NotLoaded) do
      preload(entity, [field_name])
    else
      entity
    end
  end
end
