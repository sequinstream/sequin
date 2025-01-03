defmodule Sequin.TestSupport.DateTime do
  @moduledoc false
  @callback utc_now() :: DateTime.t()
end
