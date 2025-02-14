defmodule Sequin do
  @moduledoc """
  Sequin keeps the contexts that define your domain
  and business logic.

  Contexts are also responsible for managing your data, regardless
  if it comes from the database, an external API or others.
  """
  @datetime_mod Application.compile_env(:sequin, [Sequin, :datetime_mod], DateTime)
  @uuid_mod Application.compile_env(:sequin, [Sequin, :uuid_mod], UUID)
  @enum_mod Application.compile_env(:sequin, [Sequin, :enum_mod], Enum)
  @process_mod Application.compile_env(:sequin, [Sequin, :process_mod], Process)

  defdelegate utc_now, to: @datetime_mod
  defdelegate uuid4, to: @uuid_mod
  defdelegate random(enum), to: @enum_mod

  def process_alive?(pid) do
    @process_mod.alive?(pid)
  end

  @doc """
  Returns true if the feature is enabled.

  By default, all features are disabled, to ensure the opt-in value is explicit
  from the configuration.

      iex> Sequin.feature_enabled?(:account_self_signup)
      false

  To enable a feature, add the feature name in the `:features` key in the
  `:sequin` application configuration, with the value set to `:enabled`.

      iex> Application.put_env(:sequin, :features, [account_self_signup: :enabled])
      iex> Sequin.feature_enabled?(:account_self_signup)
      true
  """
  def feature_enabled?(feature) do
    value =
      :sequin
      |> Application.get_env(:features, [])
      |> Keyword.get(feature, :disabled)

    value == :enabled
  end
end
