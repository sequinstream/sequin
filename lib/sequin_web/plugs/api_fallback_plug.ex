defmodule SequinWeb.ApiFallbackPlug do
  @moduledoc """
  General error-handling fallback for API routes.
  """
  use SequinWeb, :controller

  alias Ecto.Changeset
  alias Sequin.Error.BadRequestError
  alias Sequin.Error.InvariantError
  alias Sequin.Error.NotFoundError
  alias Sequin.Error.ServiceError
  alias Sequin.Error.TimeoutError
  alias Sequin.Error.UnauthorizedError
  alias Sequin.Error.ValidationError

  require Logger

  def call(conn, {:error, %NotFoundError{} = error}) do
    # Remove params from error to avoid leaking sensitive information
    error = %{error | params: nil}
    render_error(conn, :not_found, error)
  end

  def call(conn, {:error, %ServiceError{} = error}) do
    render_error(conn, :bad_gateway, error)
  end

  def call(conn, {:error, %TimeoutError{} = error}) do
    render_error(conn, :gateway_timeout, error)
  end

  def call(conn, {:error, %UnauthorizedError{} = error}) do
    render_error(conn, :unauthorized, error)
  end

  def call(conn, {:error, %BadRequestError{} = error}) do
    render_error(conn, :bad_request, error)
  end

  def call(conn, {:error, %InvariantError{} = error}) do
    render_error(conn, :internal_server_error, error)
  end

  def call(conn, {:error, %ValidationError{} = error}) do
    response = %{
      summary: error.summary,
      validation_errors: error.errors,
      code: error.code
    }

    Logger.metadata(response: response)

    conn
    |> put_status(:unprocessable_entity)
    |> json(response)
    |> halt()
  end

  def call(conn, {:error, %Changeset{} = changeset}) do
    errors = Sequin.Error.errors_on(changeset)

    response = %{
      summary: "Validation failed",
      validation_errors: errors
    }

    Logger.metadata(response: response)

    conn
    |> put_status(:unprocessable_entity)
    |> json(response)
    |> halt()
  end

  defp render_error(conn, status, error) do
    response = %{summary: Exception.message(error)}

    Logger.metadata(response: response)

    conn
    |> put_status(status)
    |> json(response)
    |> halt()
  end
end
