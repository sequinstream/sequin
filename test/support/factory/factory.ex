defmodule Sequin.Factory do
  @moduledoc """
  General cross-context factory functions.
  """

  def atom, do: String.to_atom(Faker.Internet.domain_word())
  def append_unique(str), do: str <> "_" <> to_string(:erlang.unique_integer([:positive]))
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
  def naive_timestamp, do: NaiveDateTime.truncate(Faker.NaiveDateTime.backward(365), :second)
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
  def name, do: to_string(Faker.Lorem.characters(10))
  def sequence, do: :erlang.unique_integer([:positive])
  def time, do: Time.utc_now()
  def timestamp, do: Faker.DateTime.backward(365)

  def timestamp_between(min, max), do: Faker.DateTime.between(min, max)
  def timestamp_future, do: Faker.DateTime.forward(365)
  def timestamp_past, do: Faker.DateTime.backward(365)
  def token, do: Faker.String.base64(32)
  def unique_integer, do: System.unique_integer([:positive])
  def unique_word, do: Faker.Lorem.word() <> "_" <> to_string(:erlang.unique_integer([:positive]))
  def unique_postgres_object, do: postgres_object() <> "_" <> to_string(:erlang.unique_integer([:positive]))
  def unix_timestamp, do: DateTime.to_unix(timestamp())
  def url, do: Faker.Internet.url()
  def username, do: Faker.Internet.user_name()
  def utc_datetime, do: DateTime.truncate(Faker.DateTime.backward(365), :second)
  def utc_datetime_usec, do: Faker.DateTime.backward(365)
  def uuid, do: UUID.uuid4()
  def word, do: Faker.Lorem.word()

  def rsa_key do
    """
    -----BEGIN PRIVATE KEY-----
    MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCtFfJBTI+O80vm
    EzPKWlLrLFgy8S33dBs1nSTn4KUvJqiY2pFtSu4YRtllBo/fcw7GqhUWkpfbL5hv
    ownHDxRtjprrr1dXogmxxVIOMC+VrKG3kGdzZCenCWGrSWfAdgCUe9zZaoduUHI1
    92xRcb+lQshPdgcgU1SKakR8l2grFRN0EX9xUFBsDuhQfYf0vWNl3EQ4KK0q3Alr
    TT5BppEggT+0/aFR6pUBRMa7Z3eV/5by1L9MR5LAnj9AFOlQSzHiMk8BCAU06Kgc
    zZAdIHL8eGP4vn+qucJDodmboL+fINF6Bdzv9nlpirTFUeWtPTyctpBZTO77pwx8
    IAivSQh3AgMBAAECggEAVBEfu9WKuHy09YeIFRVvxqegIwX/NHwhJxYvMaxrro4R
    P0SRc8r7/7CRsD7SrE3+9EDxwyfqN9xTJo3ClvtdCaVE24orWvXpzX0wUJHY2tKh
    LT4m8OXJWJj25jHeAZ2OxI+wgaz7IHpULqAA7UHZOlRIZEfISEwQ+LWKlBUZ1Co4
    vyrK3zWE5vcaRaiUMsAVfw61CpFN1Y8Btw8DkVn1nIGvu10B+Fuzdf+I1j+SEldo
    b7VRVEFNVxGhI1/1GKV50r4z5EC7lquiqF7KMg8b300pfnc+bWUD2rj5HpPd7xaP
    TJLoYIPceK9iBndbAU76PUsBELiVu/KhO9fB3F6RIQKBgQDb3ACGChChycJeyKy4
    6rQ4cdstH5A9KJ520xBrK67fCYElbanRXDBOsSdJzyHIkZZ3QdK9P3TRtUTN/KBc
    13xMcOo9c+IA0O7aWp6qY1T+8vUBReF8mzEbeEZG2Q9468aXLOGfOK3Fq9/NKATT
    iPmPChjPQEqfSRmG61OStY2PRwKBgQDJiaRN0z1t65solqWtmI/xC9MN5rzFK+8Z
    mmlHP5VHu3+vwXJIWEAmsT6C/ysyfP7iblRlEo1uY4sccIIpLLa3gIvByE3N9r/I
    Xrnye3qs2pv3CX2tZSiQ68MhI7qN2D1DgDe4aJYimhzs2/9DoTRP/xEpMZLUtzj6
    UH26NwM1UQKBgFTy2ljwBqEcfbd1vhbsyJmOlGsI6QhYa3Hp90wRYs3WtEmr55N3
    FUsyc8W2/IyshACsNCrfG9nzOhSE6ck1kVdPwZHg9o/uKnu/y9J18t9XLIdDYu5s
    YDsG69BwCeRk5SSAOOT2V14rHJv+PG8nW5WDBzb81lhZPD2/K1liQMH9AoGAEsFn
    dAKrndYmS2Gxq3UeOC2Eh6+oc6UCDFztXT8SkmllmaKkEw17ct5d1e0PRRSS19my
    qvFODi7fXFcwFcreejdRSkhszTUgZfJC1ckeAoYZq6TLeF6Ipuv57dSYYOj10plV
    FilNh4zWEkjq+Y1ABA3VuAKwCnG+sLTa7oB+IPECgYBtJkkf2ljUccYHONAn6a9q
    hexxXaQsKmTS3wefUs23kYOUAWAk6Ev8nEF3DKP+8ep+sIQfukdlvEkCEwOotcy7
    jWtPhsc2L/dT6y91gENeAYG7XgnGnJZUJNpRyrwhm5WkqsLOFIX/Ivj09UvZ/n2X
    QVH5unLV4h1YJkM3Qoc4Zw==
    -----END PRIVATE KEY-----
    """
  end
end
