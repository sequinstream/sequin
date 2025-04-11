defmodule Sequin.Consumers.FunctionTransform do
  @moduledoc false
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerEventData.Metadata
  alias Sequin.Consumers.FunctionTransform
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.Transform
  alias Sequin.Transforms.Message
  alias Sequin.Transforms.MiniElixir.Validator

  @maxlen 2000

  @derive {Jason.Encoder, only: [:type, :code]}

  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:function], default: :function
    field :code, :string
  end

  def changeset(struct, params) do
    changeset = cast(struct, params, [:code])

    if Sequin.feature_enabled?(:function_transforms) do
      changeset
      |> validate_required([:code])
      |> validate_change(:code, fn :code, code ->
        if @maxlen < byte_size(code) do
          [code: "too long"]
        else
          with {:ok, ast} <- Code.string_to_quoted(code),
               {:ok, body} <- Validator.unwrap(ast),
               :ok <- Validator.check(body),
               :ok <- safe_evaluate_code(code) do
            []
          else
            {:error, {location, {_, _} = msg, token}} ->
              msg = "parse error at #{inspect(location)}: #{inspect(msg)} #{token}"
              [code: msg]

            {:error, {location, msg, token}} ->
              msg = "parse error at #{inspect(location)}: #{msg} #{token}"
              [code: msg]

            {:error, :validator, msg} ->
              [code: "validation failed: #{msg}"]

            {:error, :evaluation_error, %CompileError{} = error} ->
              [code: "code failed to evaluate: #{Exception.message(error)}"]

            # We ignore other runtime errors because the synthetic message
            # might cause ie. bad arithmetic errors whereas the users' real
            # data might be ok.
            {:error, :evaluation_error, _} ->
              []
          end
        end
      end)
    else
      add_error(changeset, :type, "Function transforms are not enabled. Talk to the Sequin team to enable them.")
    end
  end

  def safe_evaluate_code(code) do
    Message.to_external(
      %SinkConsumer{id: nil, transform: %Transform{transform: %FunctionTransform{code: code}}},
      synthetic_message()
    )

    :ok
  rescue
    error ->
      {:error, :evaluation_error, error}
  end

  def synthetic_message do
    %ConsumerEvent{
      data: %ConsumerEventData{
        record: %{
          "id" => 1,
          "name" => "Paul Atreides",
          "house" => "Fremen",
          "inserted_at" => DateTime.utc_now()
        },
        changes: %{"house" => "House Atreides"},
        action: :update,
        metadata: %Metadata{
          table_schema: "public",
          table_name: "characters",
          commit_timestamp: DateTime.utc_now(),
          commit_lsn: 309_018_972_104,
          database_name: "dune",
          transaction_annotations: nil,
          consumer: %Metadata.Sink{
            id: Sequin.uuid4(),
            name: "my-consumer"
          }
        }
      }
    }
  end
end
