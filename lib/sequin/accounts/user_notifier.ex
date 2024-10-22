defmodule Sequin.Accounts.UserNotifier do
  @moduledoc false
  import Swoosh.Email

  alias Sequin.Mailer

  # Delivers the email using the application mailer.
  defp deliver(recipient, subject, template_id, data_variables) do
    email =
      new()
      |> to(recipient)
      |> from({"Sequin", "support@sequinstream.com"})
      |> subject(subject)
      |> put_provider_option(:transactional_id, template_id)
      |> put_provider_option(:data_variables, data_variables)

    with {:ok, _metadata} <- Mailer.deliver(email) do
      {:ok, email}
    end
  end

  @doc """
  Deliver instructions to confirm account.
  """
  def deliver_confirmation_instructions(user, url) do
    deliver(user.email, "[Sequin] Please confirm your email", "cm0q0dm7f02pizz7zypxmjidb", %{
      "user_email" => user.email,
      "confirmation_url" => url
    })
  end

  @doc """
  Deliver instructions to reset a user password.
  """
  def deliver_reset_password_instructions(user, url) do
    deliver(user.email, "[Sequin] Reset your password", "cm0q0hf5z02tezz7zkpj2du1d", %{
      "user_email" => user.email,
      "reset_password_url" => url
    })
  end

  @doc """
  Deliver instructions to update a user email.
  """
  def deliver_update_email_instructions(user, url) do
    deliver(user.email, "[Sequin] Complete your update email request", "cm0q0ifgj02uozz7zdhdwbyzd", %{
      "user_email" => user.email,
      "update_email_url" => url
    })
  end
end
