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
Sequin.Repo.insert!(%Sequin.Streams.Stream{slug: "default", account_id: account.id})
