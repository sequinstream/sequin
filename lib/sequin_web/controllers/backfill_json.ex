defmodule SequinWeb.BackfillJSON do
  @doc """
  Renders a list of backfills.
  """
  def index(%{backfills: backfills}) do
    %{data: for(backfill <- backfills, do: data(backfill))}
  end

  @doc """
  Renders a single backfill.
  """
  def show(%{backfill: backfill}) do
    %{data: data(backfill)}
  end

  defp data(backfill) do
    Sequin.Transforms.to_external(backfill)
  end
end
