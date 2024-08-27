defmodule Sequin.Factory.ErrorFactory do
  @moduledoc """
  Factories for creating `Common.Error`s.
  """
  import Sequin.Factory.Support

  alias Sequin.Error
  alias Sequin.Factory

  @postgres_codes [
    :deadlock_detected,
    :dependent_objects_still_exist,
    :disk_full,
    :encode_error,
    :feature_not_supported,
    :foreign_key_violation,
    :insufficient_privilege,
    :invalid_schema_name,
    :not_null_violation,
    :undefined_column,
    :undefined_function,
    :undefined_table,
    :unique_violation
  ]

  def error_message, do: Faker.Lorem.sentence()
  def http_status_code, do: Faker.random_between(100, 599)
  def postgres_code, do: Factory.one_of(@postgres_codes)

  def random_error do
    Factory.one_of([
      not_found_error(),
      service_error(),
      timeout_error(),
      unauthorized_error()
    ])
  end

  def query_error_message(:not_null_violation) do
    column = Factory.postgres_object() <> "_col"
    table = Factory.postgres_object() <> "_tbl"

    "ERROR 23502 (not_null_violation) null value in column \"#{column}\" of relation \"#{table}\" violates not-null constraint\n\n    table: #{table}\n    column: #{column}\n\nFailing row contains (null, hi)."
  end

  def query_error_message(:unique_violation) do
    constraint = Factory.postgres_object() <> "_uniq"
    table = Factory.postgres_object() <> "_tbl"

    "ERROR 23505 (unique_violation) duplicate key value violates unique constraint \"#{constraint}\"\n\n    table: #{table}\n    constraint: #{constraint}\n\nKey (name)=(a) already exists."
  end

  def query_error_message(:foreign_key_violation) do
    constraint = Factory.postgres_object() <> "_fkey"
    table = Factory.postgres_object() <> "_tbl"

    "ERROR 23503 (foreign_key_violation) insert or update on table \"#{table}\" violates foreign key constraint \"#{constraint}\"\n\n    table: #{table}\n    constraint: #{constraint}\n\nKey (a_id)=(hi) is not present in table \"#{Factory.postgres_object()}\"."
  end

  def query_error_message(_), do: Faker.Lorem.sentence()

  def timeout_source, do: Factory.one_of([:database, :pacer])

  def bad_request_error(attrs \\ %{}) do
    [message: error_message()]
    |> Error.bad_request()
    |> merge_attributes(attrs)
  end

  def not_found_error(attrs \\ %{}) do
    [entity: Factory.atom(), params: %{id: Factory.uuid(), parent_id: Factory.uuid()}]
    |> Error.not_found()
    |> merge_attributes(attrs)
  end

  def service_error(attrs \\ %{}) do
    [code: Faker.Internet.domain_word(), message: error_message(), service: Factory.atom()]
    |> Error.service()
    |> merge_attributes(attrs)
  end

  def timeout_error(attrs \\ %{}) do
    [source: timeout_source(), timeout_ms: Factory.milliseconds()]
    |> Error.timeout()
    |> merge_attributes(attrs)
  end

  def unauthorized_error(attrs \\ %{}) do
    [message: error_message()]
    |> Error.unauthorized()
    |> merge_attributes(attrs)
  end

  def validation_error(attrs \\ %{}) do
    {errors, attrs} =
      attrs
      |> Map.new()
      |> Map.pop_lazy(:errors, &field_errors/0)

    [errors: errors, summary: error_message()]
    |> Error.validation()
    |> merge_attributes(attrs)
  end

  defp field_errors do
    error_count = Faker.random_between(0, 3)

    for_result =
      for _i <- 1..error_count//1 do
        message_count = Faker.random_between(1, 2)
        messages = for _j <- 1..message_count//1, do: error_message()

        {Factory.atom(), messages}
      end

    Map.new(for_result)
  end
end
