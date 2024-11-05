defmodule Sequin.ObanQuery do
  @moduledoc false
  import Ecto.Query, only: [from: 2]

  def where_worker(query, job_mod) when is_atom(job_mod) do
    worker_name = Oban.Worker.to_string(job_mod)

    from j in query,
      where: j.worker == ^worker_name
  end

  def where_args(query, args) when is_map(args) do
    from j in query,
      where: fragment("? @> ?", j.args, ^args)
  end
end
