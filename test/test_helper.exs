Sequin.Test.UnboxedRepo.start_link()
Sequin.Test.Support.ReplicationSlots.setup_all()
ExUnit.start()
Ecto.Adapters.SQL.Sandbox.mode(Sequin.Repo, :manual)

# These can be left dirty by unboxed repo tests, namely ReplicationSlot tests
Sequin.Test.UnboxedRepo.delete_all(Sequin.Test.Support.Models.Character)
Sequin.Test.UnboxedRepo.delete_all(Sequin.Test.Support.Models.CharacterDetailed)
Sequin.Test.UnboxedRepo.delete_all(Sequin.Test.Support.Models.CharacterMultiPK)
Sequin.Test.UnboxedRepo.delete_all(Sequin.Test.Support.Models.TestEventLogPartitioned)

# Clean out health redis keys
:ok = Sequin.Health.clean_test_keys()
:ok = Sequin.DatabasesRuntime.TableProducer.clean_test_keys()
