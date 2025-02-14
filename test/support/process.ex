defmodule Sequin.TestSupport.Process do
  @moduledoc false
  @callback alive?(pid()) :: boolean()
end
