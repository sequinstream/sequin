defmodule Sequin.Factory do
  @moduledoc """
  General cross-context factory functions.
  """

  def atom, do: String.to_atom(Faker.Internet.domain_word())
  def boolean, do: one_of([true, false])

  def count(opts \\ []) do
    min = Keyword.get(opts, :min, 0)
    max = Keyword.get(opts, :max, 10_000)
    Faker.random_between(min, max)
  end

  def date, do: Faker.Date.backward(365)
  def duration, do: Faker.random_between(1, 100)
  def email, do: Faker.Internet.email()
  def first_name, do: Faker.Person.first_name()

  def float(opts \\ []) do
    min = Keyword.get(opts, :min, 0)
    max = Keyword.get(opts, :max, 1000)
    round = Keyword.get(opts, :round, 2)
    int = Faker.random_between(min, max)
    Float.round(int * 1.0, round)
  end

  def genserver_name(test_mod, base_name), do: Module.concat([test_mod, base_name, uuid()])
  def hostname, do: Faker.Internet.domain_name()
  def http_method, do: one_of([:get, :post, :put, :patch, :delete])
  def index, do: Faker.random_between(1, 250_000)
  def integer, do: Faker.random_between(1, 100_000)
  def iso_timestamp, do: DateTime.to_iso8601(timestamp())
  def last_name, do: Faker.Person.last_name()
  def milliseconds, do: Faker.random_between(1, 300_000)
  def naive_timestamp, do: Faker.NaiveDateTime.backward(365)
  def one_of(module) when is_atom(module), do: one_of(module.__valid_values__())
  def one_of(opts), do: Enum.random(opts)
  def password, do: Faker.String.base64(12)

  def pid do
    :c.pid(
      0,
      Faker.random_between(0, 1000),
      Faker.random_between(0, 1000)
    )
  end

  def port, do: Faker.random_between(5000, 9999)
  def postgres_object, do: Faker.Internet.domain_word()
  def slug, do: to_string(Faker.Lorem.characters(10))
  def timestamp, do: Faker.DateTime.backward(365)
  def timestamp_between(min, max), do: Faker.DateTime.between(min, max)
  def timestamp_future, do: Faker.DateTime.forward(365)
  def timestamp_past, do: Faker.DateTime.backward(365)
  def token, do: Faker.String.base64(32)
  def unique_integer, do: System.unique_integer([:positive])
  def unix_timestamp, do: DateTime.to_unix(timestamp())
  def url, do: Faker.Internet.url()
  def username, do: Faker.Internet.user_name()
  def utc_datetime, do: DateTime.truncate(Faker.DateTime.backward(365), :second)
  def utc_datetime_usec, do: Faker.DateTime.backward(365)
  def uuid, do: UUID.uuid4()
  def word, do: Faker.Lorem.word()
end
