defmodule Sequin.Accounts.UserNotifier do
  @moduledoc false
  import Swoosh.Email

  alias Sequin.Mailer

  # Delivers the email using the application mailer.
  defp deliver(recipient, subject, body) do
    email =
      new()
      |> to(recipient)
      |> from({"Sequin", "support@sequinstream.com"})
      |> subject(subject)
      |> text_body(body)

    if Application.get_env(:sequin, :env) == :prod do
      {:ok, :not_sent}
    else
      with {:ok, _metadata} <- Mailer.deliver(email) do
        {:ok, email}
      end
    end
  end

  @doc """
  Deliver instructions to confirm account.
  """
  def deliver_confirmation_instructions(user, url) do
    deliver(user.email, "Confirmation instructions", """

    ==============================

    Hi #{user.email},

    You can confirm your account by visiting the URL below:

    #{url}

    If you didn't create an account with us, please ignore this.

    ==============================
    """)
  end

  @doc """
  Deliver instructions to reset a user password.
  """
  def deliver_reset_password_instructions(user, url) do
    deliver(user.email, "Reset password instructions", """

    ==============================

    Hi #{user.email},

    You can reset your password by visiting the URL below:

    #{url}

    If you didn't request this change, please ignore this.

    ==============================
    """)
  end

  @doc """
  Deliver instructions to update a user email.
  """
  def deliver_update_email_instructions(user, url) do
    deliver(user.email, "Update email instructions", """

    ==============================

    Hi #{user.email},

    You can change your email by visiting the URL below:

    #{url}

    If you didn't request this change, please ignore this.

    ==============================
    """)
  end

  def deliver_invite_to_account_instructions(user_email, account_name, url) do
    deliver(user_email, "Invite to account instructions", """

    ==============================

    Hi #{user_email},

    You have been invited to join an account on #{account_name}. To accept the invite, please visit the URL below:

    #{url}

    If you don't want to join this account, please ignore this email.

    ==============================
    """)
  end
end
