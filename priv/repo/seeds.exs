# Script for populating the database. You can run it as:
#
#     mix run priv/repo/seeds.exs
#
# Inside the script, you can read and write to any of your
# repositories directly:
#
#     Sequin.Repo.insert!(%Sequin.SomeSchema{})
#
# We recommend using the bang functions (`insert!`, `update!`
# and so on) as they will fail if something goes wrong.

account = Sequin.Repo.insert!(%Sequin.Accounts.Account{})
{:ok, _stream} = Sequin.Streams.create_stream_for_account_with_lifecycle(account.id, %{slug: "default"})
