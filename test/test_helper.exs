alias Sequin.Databases.PostgresDatabaseTable
alias Sequin.Test.UnboxedRepo

UnboxedRepo.start_link()
Sequin.TestSupport.ReplicationSlots.setup_all()
ExUnit.start()
Ecto.Adapters.SQL.Sandbox.mode(Sequin.Repo, :manual)

migrations_dir = Path.join(UnboxedRepo.config()[:priv], "migrations")
[{create_mod, _}] = Code.require_file(Path.join(migrations_dir, "20240816005458_create_test_tables.exs"))
create_mod.check_version!(UnboxedRepo)

# These can be left dirty by unboxed repo tests, namely ReplicationSlot tests
UnboxedRepo.delete_all(Sequin.TestSupport.Models.Character)
UnboxedRepo.delete_all(Sequin.TestSupport.Models.CharacterDetailed)
UnboxedRepo.delete_all(Sequin.TestSupport.Models.CharacterMultiPK)
UnboxedRepo.delete_all(Sequin.TestSupport.Models.TestEventLogPartitioned)

# Clean out health redis keys
:ok = Sequin.Health.clean_test_keys()
:ok = Sequin.Runtime.TableReader.clean_test_keys()

# Cache the metadata for the character tables, for use in factory
{:ok, tables} = Sequin.Databases.list_tables(UnboxedRepo)

tables =
  Enum.map(tables, fn table ->
    table = Map.update!(table, :columns, fn columns -> Enum.map(columns, &struct!(PostgresDatabaseTable.Column, &1)) end)

    struct!(PostgresDatabaseTable, table)
  end)

:character_tables
|> :ets.new([:set, :public, :named_table])
|> :ets.insert({:tables, tables})

# Mocks
Mox.defmock(Sequin.Sinks.RedisMock, for: Sequin.Sinks.Redis)
