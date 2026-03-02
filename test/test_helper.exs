alias Sequin.Databases.PostgresDatabaseTable
alias Sequin.Test.UnboxedRepo

# Suppress noisy log messages in tests that come from OTP/library processes
# (not test-owned) and would otherwise leak into test output.
# ExUnit's capture_log only captures from direct children via $callers metadata,
# so logs from deeply nested supervised processes leak through.
:logger.add_handler_filter(:default, :suppress_test_noise, {
  fn
    # Postgrex disconnect errors when test processes exit with checked-out connections
    %{meta: %{mfa: {DBConnection.Connection, :handle_event, _}}}, _extra ->
      :stop

    # GenServer terminating errors during test teardown
    %{meta: %{mfa: {:gen_server, :error_info, _}}}, _extra ->
      :stop

    # DebouncedLogger flush messages from timer processes
    %{meta: %{mfa: {Sequin.DebouncedLogger, :flush_bucket, _}}}, _extra ->
      :stop

    # DebouncedLogger initial log calls from nested supervised processes
    %{meta: %{mfa: {Sequin.DebouncedLogger, _, _}}}, _extra ->
      :stop

    # SlotMessageStoreState warnings (backfill group conflict, etc.) from supervised processes
    %{meta: %{mfa: {Sequin.Runtime.SlotMessageStoreState, _, _}}}, _extra ->
      :stop

    # ReorderBuffer exit warnings during test teardown
    %{meta: %{mfa: {Sequin.Runtime.SlotProducer.ReorderBuffer, :handle_info, _}}}, _extra ->
      :stop

    # Req retry warnings from HTTP requests in nested processes
    %{meta: %{mfa: {Req.Steps, :log_retry, _}}}, _extra ->
      :stop

    _log, _extra ->
      :ignore
  end,
  %{}
})

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
