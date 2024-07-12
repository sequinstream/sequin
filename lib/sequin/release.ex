defmodule Sequin.Release do
  @moduledoc false
  @app :sequin

  def migrate(step) do
    load_app()
    ensure_ssl_started()
    Application.ensure_all_started(:sequin)

    for repo <- repos() do
      {:ok, _, _} = Ecto.Migrator.with_repo(repo, &Ecto.Migrator.run(&1, :up, step: step))
    end

    Sequin.Streams.maybe_seed()

    :ok
  end

  def migrate do
    load_app()
    ensure_ssl_started()
    Application.ensure_all_started(:sequin)

    for repo <- repos() do
      {:ok, _, _} = Ecto.Migrator.with_repo(repo, &Ecto.Migrator.run(&1, :up, all: true))
    end

    Sequin.Streams.maybe_seed()

    :ok
  end

  def rollback(repo, version) do
    load_app()
    {:ok, _, _} = Ecto.Migrator.with_repo(repo, &Ecto.Migrator.run(&1, :down, to: version))
  end

  defp repos do
    [Sequin.Repo]
  end

  defp load_app do
    Application.load(@app)
  end

  defp ensure_ssl_started do
    Application.ensure_all_started(:ssl)
  end
end
