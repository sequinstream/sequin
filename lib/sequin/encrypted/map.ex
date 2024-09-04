defmodule Sequin.Encrypted.Map do
  @moduledoc false
  use Cloak.Ecto.Map, vault: Sequin.Vault

  @type t :: map()
end
