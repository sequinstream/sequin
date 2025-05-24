defmodule Sequin.ApplicationBehaviour do
  @moduledoc false
  @callback get_env(module(), atom()) :: term()
end
