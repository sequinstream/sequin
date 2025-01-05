defmodule Sequin.TestSupport.AccountsSupport do
  @moduledoc false
  def extract_user_token(fun) do
    {:ok, captured_email} = fun.(&"[TOKEN]#{&1}[TOKEN]")

    url =
      captured_email.provider_options.data_variables
      |> Map.values()
      |> Sequin.Enum.find!(fn url -> String.contains?(url, "[TOKEN]") end)

    [_, token | _] = String.split(url, "[TOKEN]")
    token
  end
end
