defmodule Sequin.Databases.ConnectionOpts do
  @moduledoc """
  This struct allows us to augment the fields that Postgrex.start_link/1 expects. We can add
  things like SSH tunnels etc that wrap the base Postgres connection details. We can also create
  generic connection structs ahead of having anything in the databaes (for testing connections
  during create).
  """

  use TypedStruct

  @derive {Inspect, except: [:password]}
  typedstruct do
    field :database, String.t(), enforce: true
    field :hostname, String.t(), enforce: true
    field :password, String.t(), enforce: true
    field :pool_size, number(), default: 1
    field :port, :inet.port_number(), enforce: true
    field :queue_interval, number(), default: 1_000
    field :queue_target, number(), default: 10_000
    field :ssl_opts, list(any()), default: []
    field :ssl, boolean(), default: false
    field :username, String.t(), enforce: true
  end

  def to_postgrex_opts(%__MODULE__{} = opts) do
    opts
    |> Map.from_struct()
    |> Map.put(:types, PostgrexTypes)
    |> Enum.to_list()
  end
end
