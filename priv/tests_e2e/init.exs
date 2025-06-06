Mix.install([
  {:postgrex, "~> 0.17.5"},
  {:jason, "~> 1.4"},
  {:kafka_ex, "~> 0.13.0"}
])

Application.put_env(:kafka_ex, :brokers, [{"localhost", 9092}])
Application.put_env(:kafka_ex, :consumer_group, "e2e_test_group")

[:postgrex, :kafka_ex]
|> Enum.each(fn app ->
  {:ok, _} = Application.ensure_all_started(app)
end)

:inets.start()
ExUnit.start(timeout: 120_000)
