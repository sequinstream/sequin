defmodule Sequin.ErrorTest do
  use Sequin.Case, async: true

  alias Sequin.Accounts.Account
  alias Sequin.Consumers.SequenceFilter
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
      sequence_filter_attrs = ConsumersFactory.sequence_filter_attrs()
      changeset = SequenceFilter.create_changeset(%SequenceFilter{}, sequence_filter_attrs)
      assert Error.errors_on(changeset) == %{}

      sequence_filter_attrs = %{sequence_filter_attrs | group_column_attnums: []}
      changeset = SequenceFilter.create_changeset(%SequenceFilter{}, sequence_filter_attrs)
      assert Error.errors_on(changeset) == %{group_column_attnums: ["should have at least 1 item(s)"]}
    end
  end
end
