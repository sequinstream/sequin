defmodule Sequin.Factory.FunctionsFactory do
  @moduledoc false
  import Sequin.Factory.Support

  alias Sequin.Consumers.FilterFunction
  alias Sequin.Consumers.Function
  alias Sequin.Consumers.PathFunction
  alias Sequin.Consumers.RoutingFunction
  alias Sequin.Consumers.TransformFunction
  alias Sequin.Factory
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Repo

  # Base function factory
  def function(attrs \\ []) do
    attrs = Map.new(attrs)

    {account_id, attrs} =
      Map.pop_lazy(attrs, :account_id, fn -> AccountsFactory.insert_account!().id end)

    {function_type, attrs} =
      Map.pop_lazy(attrs, :function_type, fn -> Enum.random([:path, :transform, :routing, :filter]) end)

    {function_attrs, attrs} = Map.pop(attrs, :function_attrs, [])

    function_attrs =
      case function_type do
        :path -> path_function(function_attrs)
        :transform -> transform_function(function_attrs)
        :routing -> routing_function(function_attrs)
        :filter -> filter_function(function_attrs)
      end

    merge_attributes(
      %Function{
        id: Factory.uuid(),
        account_id: account_id,
        name: Factory.unique_word(),
        description: Factory.word(),
        type: to_string(function_type),
        function: function_attrs
      },
      attrs
    )
  end

  def function_attrs(attrs \\ []) do
    attrs
    |> function()
    |> Sequin.Map.from_ecto()
    |> Map.update!(:function, &Sequin.Map.from_ecto/1)
  end

  def insert_function!(attrs \\ []) do
    attrs = Map.new(attrs)

    {account_id, attrs} =
      Map.pop_lazy(attrs, :account_id, fn -> AccountsFactory.insert_account!().id end)

    attrs
    |> function_attrs()
    |> then(&Function.create_changeset(%Function{account_id: account_id}, &1))
    |> Repo.insert!()
  end

  # Path Function
  def path_function(attrs \\ []) do
    valid_paths = [
      "record",
      "changes",
      "action",
      "metadata",
      "record.id",
      "changes.name",
      "metadata.table_schema",
      "metadata.table_name",
      "metadata.commit_timestamp",
      "metadata.commit_lsn",
      "metadata.transaction_annotations",
      "metadata.sink",
      "metadata.transaction_annotations.user_id",
      "metadata.sink.id",
      "metadata.sink.name"
    ]

    merge_attributes(
      %PathFunction{
        type: :path,
        path: Enum.random(valid_paths)
      },
      attrs
    )
  end

  def path_function_attrs(attrs \\ []) do
    attrs
    |> path_function()
    |> Sequin.Map.from_ecto()
  end

  def insert_path_function!(attrs \\ []) do
    attrs
    |> Map.new()
    |> Map.put(:function_type, :path)
    |> insert_function!()
  end

  # Transform Function
  def transform_function(attrs \\ []) do
    attrs = Map.new(attrs)

    {body, attrs} =
      Map.pop(attrs, :body, """
      %{
        action: action,
        record: record,
        changes: changes,
        metadata: metadata
      }
      """)

    merge_attributes(
      %TransformFunction{
        type: :transform,
        code: """
        def transform(action, record, changes, metadata) do
          #{body}
        end
        """
      },
      attrs
    )
  end

  def transform_function_attrs(attrs \\ []) do
    attrs
    |> transform_function()
    |> Sequin.Map.from_ecto()
  end

  def insert_transform_function!(attrs \\ []) do
    attrs
    |> Map.new()
    |> Map.put(:function_type, :transform)
    |> insert_function!()
  end

  # Routing Function
  def routing_function(attrs \\ []) do
    {sink_type, attrs} =
      Map.pop_lazy(Map.new(attrs), :sink_type, fn ->
        Enum.random([
          :http_push,
          :redis_string,
          :redis_stream,
          :kafka,
          :gcp_pubsub,
          :typesense,
          :meilisearch,
          :elasticsearch,
          :sqs,
          :sns
        ])
      end)

    {body, attrs} =
      Map.pop_lazy(attrs, :body, fn ->
        case sink_type do
          :redis_string ->
            """
            %{
              key: record["id"] || "default",
              value: record
            }
            """

          :redis_stream ->
            """
            %{
              stream_key: metadata.table_name
            }
            """

          :http_push ->
            """
            %{
              url: "https://example.com/push",
              body: record
            }
            """

          :kafka ->
            """
            %{
              topic: metadata.table_name
            }
            """

          :gcp_pubsub ->
            """
            %{
              topic_id: metadata.table_name
            }
            """

          :sns ->
            """
            %{
              topic_arn: "arn:aws:sns:us-west-2:123456789012:" <> metadata.table_name
            }
            """

          :typesense ->
            """
            %{
              collection_name: metadata.table_name
              }
            """

          :sqs ->
            """
            %{
              queue_url: metadata.table_name
            }
            """

          :meilisearch ->
            """
            %{
              index_name: metadata.table_name
            }
            """

          :elasticsearch ->
            """
            %{
              index_name: metadata.table_name
            }
            """
        end
      end)

    merge_attributes(
      %RoutingFunction{
        type: :routing,
        sink_type: sink_type,
        code: """
        def route(action, record, changes, metadata) do
          #{body}
        end
        """
      },
      attrs
    )
  end

  def routing_function_attrs(attrs \\ []) do
    attrs
    |> routing_function()
    |> Sequin.Map.from_ecto()
  end

  def insert_routing_function!(attrs \\ []) do
    attrs
    |> Map.new()
    |> Map.put(:function_type, :routing)
    |> insert_function!()
  end

  # Filter Function
  def filter_function(attrs \\ []) do
    attrs = Map.new(attrs)

    {body, attrs} =
      Map.pop(attrs, :body, """
      true
      """)

    merge_attributes(
      %FilterFunction{
        type: :filter,
        code: """
        def filter(action, record, changes, metadata) do
          #{body}
        end
        """
      },
      attrs
    )
  end

  def filter_function_attrs(attrs \\ []) do
    attrs
    |> filter_function()
    |> Sequin.Map.from_ecto()
  end

  def insert_filter_function!(attrs \\ []) do
    attrs
    |> Map.new()
    |> Map.put(:function_type, :filter)
    |> insert_function!()
  end
end
