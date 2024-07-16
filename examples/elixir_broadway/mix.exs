defmodule ElixirBroadway.MixProject do
  use Mix.Project

  def project do
    [
      app: :elixir_broadway,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {ElixirBroadway.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:off_broadway_sequin, "~> 0.1.0"},
      {:broadway, "~> 1.0"}
    ]
  end
end
