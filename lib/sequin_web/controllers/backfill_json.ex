defmodule SequinWeb.BackfillJSON do
  @moduledoc false
  alias Sequin.Consumers.Backfill

  def render(:index, %{backfills: backfills}) do
    %{data: backfills}
  end

  def render(:show, %{backfill: backfill}) do
    backfill
  end

  def render(:delete, %{backfill: backfill}) do
    %{id: backfill.id, cancelled: true}
  end
end
