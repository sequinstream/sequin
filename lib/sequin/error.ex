defmodule Sequin.Error do
  @moduledoc false
  alias Ecto.Changeset
  alias Sequin.JSON

  require Logger

  defmodule BadRequestError do
    @moduledoc false
    @derive Jason.Encoder
    @enforce_keys [:message]
    defexception [:message]

    @type t :: %__MODULE__{
            message: String.t()
          }

    def from_json(json), do: JSON.struct(json, __MODULE__)
  end

  defmodule NotFoundError do
    @moduledoc false
    @derive Jason.Encoder
    @enforce_keys [:entity]
    defexception [:entity, :params]

    @type t :: %__MODULE__{
            entity: atom(),
            params: map() | nil
          }

    @impl Exception
    def message(%__MODULE__{} = error) do
      case error.params do
        nil ->
          "Not found: No `#{render_entity(error.entity)}` found matching the provided ID or name"

        params ->
          "Not found: No `#{render_entity(error.entity)}` found matching params: `#{inspect(params, pretty: true)}`"
      end
    end

    defp render_entity(entity) do
      case entity do
        :resource -> "sync"
        _ -> entity |> Atom.to_string() |> String.replace("_", " ")
      end
    end

    def from_json(json) do
      json
      |> JSON.decode_atom("entity")
      |> Map.update!("params", fn
        nil -> nil
        params when is_map(params) -> Sequin.Map.atomize_keys(params)
      end)
      |> JSON.struct(__MODULE__)
    end
  end

  defmodule ServiceError do
    @moduledoc false
    @derive Jason.Encoder
    @enforce_keys [:code, :message, :service]
    defexception [:code, :message, :service, :details]

    @type t :: %__MODULE__{
            code: String.t(),
            message: String.t(),
            service: atom(),
            details: term()
          }

    @impl Exception
    def exception(opts) do
      service = Keyword.fetch!(opts, :service)
      base_message = Keyword.get(opts, :message, "Failed request")
      message = "[#{service}]: #{base_message}"

      %__MODULE__{
        code: opts[:code],
        message: message,
        service: service,
        details: opts[:details]
      }
    end

    def from_json(json) do
      json
      |> JSON.decode_atom("service")
      |> JSON.struct(__MODULE__)
    end
  end

  defmodule TimeoutError do
    @moduledoc false
    @derive Jason.Encoder
    @enforce_keys [:source, :timeout_ms]
    defexception [:source, :timeout_ms]

    @type t :: %__MODULE__{
            source: atom(),
            timeout_ms: non_neg_integer()
          }

    @impl Exception
    def message(%__MODULE__{} = error) do
      source = error.source |> to_string() |> String.capitalize()
      "#{source} timeout: #{error.timeout_ms}ms"
    end

    def from_json(json) do
      json
      |> JSON.decode_atom("source")
      |> JSON.struct(__MODULE__)
    end
  end

  defmodule UnauthorizedError do
    @moduledoc false
    @derive Jason.Encoder
    @enforce_keys [:message]
    defexception message: "Unauthorized"

    @type t :: %__MODULE__{
            message: String.t()
          }

    def from_json(json), do: JSON.struct(json, __MODULE__)
  end

  defmodule ValidationError do
    @moduledoc false
    @derive Jason.Encoder
    defexception errors: %{}, summary: nil, code: nil

    @type errors :: %{
            field_name() => [String.t()]
          }
    @type field_name :: atom() | String.t()
    @type t :: %__MODULE__{
            errors: errors(),
            summary: String.t(),
            code: atom()
          }

    @impl Exception
    def exception(opts) do
      errors =
        if changeset = opts[:changeset] do
          Sequin.Error.errors_on(changeset)
        else
          opts[:errors] || %{}
        end

      %__MODULE__{
        errors: errors,
        summary: opts[:summary],
        code: opts[:code]
      }
    end

    @impl Exception
    def message(%__MODULE__{} = error) do
      formatted_errors = Enum.map(error.errors, fn {field, errors} -> "- #{field}: #{Enum.join(errors, "; ")}" end)

      Enum.join(List.wrap(error.summary) ++ formatted_errors, "\n")
    end

    def from_json(json), do: JSON.struct(json, __MODULE__)

    def from_postgrex(summary_prefix \\ "Postgres error: ", %Postgrex.Error{} = error) do
      pg_code = error.postgres && error.postgres.code
      message = error.message || (error.postgres && error.postgres.message)
      code = if pg_code, do: {:postgrex_error, pg_code}, else: :postgrex_error

      %__MODULE__{
        errors: %{},
        summary: summary_prefix <> message,
        code: code
      }
    end
  end

  # defmodule Guards do
  #   @moduledoc false
  #   # STEP 2: Add a clause for the new error module to this guard:
  #   defguard is_common_error(error)
  #            when is_exception(error, BadRequestError) or
  #                   is_exception(error, BadStatusError) or
  #                   is_exception(error, ConflictError) or
  #                   is_exception(error, ForbiddenError) or
  #                   is_exception(error, InvariantError) or
  #                   is_exception(error, NotFoundError) or
  #                   is_exception(error, NotImplementedError) or
  #                   is_exception(error, QueryError) or
  #                   is_exception(error, RateLimitError) or
  #                   is_exception(error, ServiceError) or
  #                   is_exception(error, TimeoutError) or
  #                   is_exception(error, UnauthorizedError) or
  #                   is_exception(error, UnknownError) or
  #                   is_exception(error, ValidationError)
  # end

  # STEP 3: Add the new error module to this type definition:
  @type t ::
          BadRequestError.t()
          | NotFoundError.t()
          | ServiceError.t()
          | TimeoutError.t()
          | UnauthorizedError.t()
          | ValidationError.t()

  # STEP 4: Add a constructor function for the new module in alphabetical order.

  # STEP 5: Add a factory function in Sequin.Factory.ErrorFactory for creating
  # your new error type in tests.

  # STEP 6: Add a `call` clause (and tests) for the new error type in
  # IxWeb.ApiFallbackPlug.

  @spec bad_request([opt]) :: BadRequestError.t()
        when opt: {:message, String.t()}
  def bad_request(opts), do: BadRequestError.exception(opts)

  @spec not_found([opt]) :: NotFoundError.t()
        when opt:
               {:entity, atom()}
               | {:params, map()}
  def not_found(opts), do: NotFoundError.exception(opts)

  @spec service([opt]) :: ServiceError.t()
        when opt: {:code, String.t()} | {:message, String.t()} | {:service, atom()} | {:details, term()}
  def service(opts), do: ServiceError.exception(opts)

  @spec timeout([opt]) :: TimeoutError.t()
        when opt: {:source, atom()} | {:timeout_ms, non_neg_integer()}
  def timeout(opts), do: TimeoutError.exception(opts)

  @spec unauthorized([opt]) :: UnauthorizedError.t()
        when opt: {:message, String.t()}
  def unauthorized(opts), do: UnauthorizedError.exception(opts)

  @spec validation([opt]) :: ValidationError.t()
        when opt:
               {:changeset, Changeset.t()}
               | {:errors, ValidationError.errors()}
               | {:summary, String.t()}
               | {:code, atom()}
  def validation(opts), do: ValidationError.exception(opts)

  @doc """
  Traverse a changeset to extract errors into a map.

  This is useful for when you want to return a changeset error to the client.

  For example, if you have a changeset with an error message like this:

      "must be at least %{min} characters"

  And you want to return the error to the client as JSON, you can call this
  function to replace the %{min} placeholder with the actual value of the
  `min` key in the changeset's `errors` map.

  Additionally, this function will traverse the changeset's embeded changesets
  and merge their errors into the returned map.

  `key_mapper` allows you to transform the keys in the returned map. The mapper
  function will receive the struct and the key as arguments, and should return
  the transformed key.

  This allows us to transform error keys from the internal database column
  names to the external API field names.
  """
  def errors_on(%Ecto.Changeset{} = changeset) do
    errors = traverse_errors(changeset)

    embedded_errors =
      changeset.changes
      |> Enum.map(fn
        {key, %Ecto.Changeset{valid?: false} = embedded_changeset} ->
          {key, traverse_errors(embedded_changeset)}

        _ ->
          nil
      end)
      |> Enum.reject(&is_nil/1)
      |> Map.new()

    Map.merge(errors, embedded_errors)
  end

  defp traverse_errors(changeset) do
    changeset
    |> Ecto.Changeset.traverse_errors(fn {msg, opts} ->
      Regex.replace(~r"%{(\w+)}", msg, fn _match, key ->
        opts |> Keyword.get(String.to_existing_atom(key), key) |> to_string()
      end)
    end)
    |> Map.new()
  end
end
