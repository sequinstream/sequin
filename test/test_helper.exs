alias Sequin.Databases.PostgresDatabase
alias Sequin.Test.UnboxedRepo

UnboxedRepo.start_link()
Sequin.Test.Support.ReplicationSlots.setup_all()
ExUnit.start()
Ecto.Adapters.SQL.Sandbox.mode(Sequin.Repo, :manual)

# These can be left dirty by unboxed repo tests, namely ReplicationSlot tests
UnboxedRepo.delete_all(Sequin.Test.Support.Models.Character)
UnboxedRepo.delete_all(Sequin.Test.Support.Models.CharacterDetailed)
UnboxedRepo.delete_all(Sequin.Test.Support.Models.CharacterMultiPK)
UnboxedRepo.delete_all(Sequin.Test.Support.Models.TestEventLogPartitioned)

# Clean out health redis keys
:ok = Sequin.Health.clean_test_keys()
:ok = Sequin.DatabasesRuntime.TableProducer.clean_test_keys()

# Cache the metadata for the character tables, for use in factory
{:ok, tables} = Sequin.Databases.list_tables(UnboxedRepo)

tables =
  Enum.map(tables, fn table ->
    table = Map.update!(table, :columns, fn columns -> Enum.map(columns, &struct!(PostgresDatabase.Table.Column, &1)) end)

    struct!(PostgresDatabase.Table, table)
  end)

:character_tables
|> :ets.new([:set, :public, :named_table])
|> :ets.insert({:tables, tables})
