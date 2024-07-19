defmodule Sequin.WebhookTest do
  use Sequin.DataCase, async: true

  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.SourcesFactory
  alias Sequin.Factory.StreamsFactory

  test "create valid webhook" do
    account = AccountsFactory.insert_account!()
    stream = StreamsFactory.insert_stream!(account_id: account.id)
    assert SourcesFactory.insert_webhook!(account_id: account.id, stream_id: stream.id)
  end
end
