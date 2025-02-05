Mox.defmock(Sequin.DatabasesRuntime.MessageHandlerMock,
  for: Sequin.DatabasesRuntime.SlotProcessor.MessageHandlerBehaviour
)

Mox.defmock(Sequin.Sinks.NatsMock,
  for: Sequin.Sinks.Nats
)

Mox.defmock(Sequin.Sinks.RabbitMqMock,
  for: Sequin.Sinks.RabbitMq
)

Mox.defmock(Sequin.DatabasesRuntime.TableReaderServerMock,
  for: Sequin.DatabasesRuntime.TableReaderServer
)

Mox.defmock(Sequin.TestSupport.DateTimeMock,
  for: Sequin.TestSupport.DateTime
)

Mox.defmock(Sequin.TestSupport.UUIDMock,
  for: Sequin.TestSupport.UUID
)

Mox.defmock(Sequin.DatabasesRuntime.SlotMessageStoreMock,
  for: Sequin.DatabasesRuntime.SlotMessageStoreBehaviour
)
