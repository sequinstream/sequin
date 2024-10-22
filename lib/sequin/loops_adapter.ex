defmodule Sequin.Swoosh.Adapters.Loops do
  @moduledoc ~s"""
  An adapter that sends email using the Loops.so API.

  For reference: [Loops API docs](https://loops.so/docs/api-reference/overview)

  **This adapter requires an API Client.** Swoosh comes with Hackney, Finch and Req out of the box.
  See the [installation section](https://hexdocs.pm/swoosh/Swoosh.html#module-installation)
  for details.

  ## Example

      # config/config.exs
      config :sample, Sample.Mailer,
        adapter: Swoosh.Adapters.Loops,
        api_key: "my-api-key"

      # lib/sample/mailer.ex
      defmodule Sample.Mailer do
        use Swoosh.Mailer, otp_app: :sample
      end

  ## Using with provider options

      import Swoosh.Email

      new()
      |> from({"Sender Name", "sender@example.com"})
      |> to({"Recipient Name", "recipient@example.com"})
      |> subject("Hello from Loops!")
      |> html_body("<h1>Hello</h1>")
      |> text_body("Hello")
      |> put_provider_option(:transactional_id, "your-transactional-id")
      |> put_provider_option(:add_to_audience, true)
      |> put_provider_option(:data_variables, %{
        "variable1" => "value1",
        "variable2" => "value2"
      })

  ## Provider Options

  Supported provider options are the following:

    * `:transactional_id` (string) - The ID of the transactional email template (required)
    * `:add_to_audience` (boolean) - Whether to add the recipient to your audience
    * `:data_variables` (map) - Key/value pairs of variables to be used in the email template

  """

  use Swoosh.Adapter, required_config: [:api_key]

  alias Sequin.Loops
  alias Sequin.Loops.TransactionalRequest
  alias Swoosh.Email

  @impl Swoosh.Adapter
  def deliver(%Email{} = email, config \\ []) do
    request = prepare_request(email)

    case Loops.send_transactional(request, api_key: config[:api_key]) do
      {:ok, response} -> {:ok, %{id: response["id"]}}
      {:error, error} -> {:error, error}
    end
  end

  defp prepare_request(email) do
    %TransactionalRequest{
      email: email.to |> List.first() |> elem(1),
      transactional_id: email.provider_options[:transactional_id],
      add_to_audience: email.provider_options[:add_to_audience],
      data_variables: email.provider_options[:data_variables] || %{}
    }
  end
end
