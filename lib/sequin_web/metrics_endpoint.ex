defmodule SequinWeb.MetricsEndpoint do
  def child_spec(_opts) do
    opts = Application.get_env(:sequin, __MODULE__, [])

    if Keyword.get(opts, :server, false) do
      opts
      |> Keyword.get(:http, [])
      |> Keyword.put(:plug, {Peep.Plug, peep_worker: :sequin})
      |> Bandit.child_spec()
    else
      :ignore
    end
  end
end
