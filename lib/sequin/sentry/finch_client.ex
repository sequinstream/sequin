defmodule Sequin.Sentry.FinchClient do
  @moduledoc """
  This client implements the `Sentry.HTTPClient` behaviour.
  It's based on the [Finch](https://github.com/sneako/finch) Elixir HTTP client,
  which is an *optional dependency* of this library. If you wish to use another
  HTTP client, you'll have to implement your own `Sentry.HTTPClient`. See the
  documentation for `Sentry.HTTPClient` for more information.
  Finch is built on top of [NimblePool](https://github.com/dashbitco/nimble_pool). If you need to set other pool configuration options,
  see "Pool Configuration Options" in the Finch documentation for details on the possible map values.
  [finch configuration options](https://hexdocs.pm/finch/Finch.html#start_link/1-pool-configuration-options)
  """
  @behaviour Sentry.HTTPClient

  @impl true
  def child_spec do
    if Code.ensure_loaded?(Finch) do
      case Application.ensure_all_started(:finch) do
        {:ok, _apps} -> :ok
        {:error, reason} -> raise "failed to start the :finch application: #{inspect(reason)}"
      end

      Finch.child_spec(
        name: __MODULE__,
        pools: %{:default => [size: 10]}
      )
    else
      raise """
      cannot start the :sentry application because the HTTP client is set to \
      Sentry.FinchClient (which is the default), but the :finch library is not loaded. \
      Add :finch to your dependencies to fix this.
      """
    end
  end

  @impl true
  def post(url, headers, body) do
    request = Finch.build(:post, url, headers, body)

    case Finch.request(request, __MODULE__) do
      {:ok, %Finch.Response{status: status, headers: headers, body: body}} ->
        {:ok, status, headers, body}

      {:error, error} ->
        {:error, error}
    end
  end
end
