defmodule Sequin.ErrorTest do
  use Sequin.Case, async: true

  alias Sequin.Accounts.Account
  alias Sequin.Consumers.Source
  alias Sequin.Error
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory

  describe "errors_on/1" do
    test "returns the errors on a changeset" do
      account_attrs = AccountsFactory.account_attrs()
      changeset = Account.changeset(%Account{}, account_attrs)
      assert Error.errors_on(changeset) == %{}

      account_attrs = %{account_attrs | name: true}
      changeset = Account.changeset(%Account{}, account_attrs)
      assert Error.errors_on(changeset) == %{name: ["is invalid"]}
    end

    test "returns errors when a list fails validation" do
      source_attrs = ConsumersFactory.source_attrs(include_schemas: ["public"])
      changeset = Source.changeset(%Source{}, source_attrs)
      assert Error.errors_on(changeset) == %{}

      source_attrs = %{source_attrs | include_schemas: []}
      changeset = Source.changeset(%Source{}, source_attrs)
      assert Error.errors_on(changeset) == %{include_schemas: ["should have at least 1 item(s)"]}
    end
  end
end
