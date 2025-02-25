defmodule S2.V1alpha.StreamService.Service do
  @moduledoc false

  use GRPC.Service, name: "s2.v1alpha.StreamService", protoc_gen_elixir_version: "0.14.0"

  rpc(:CheckTail, S2.V1alpha.CheckTailRequest, S2.V1alpha.CheckTailResponse)

  rpc(:Append, S2.V1alpha.AppendRequest, S2.V1alpha.AppendResponse)

  rpc(
    :AppendSession,
    stream(S2.V1alpha.AppendSessionRequest),
    stream(S2.V1alpha.AppendSessionResponse)
  )

  rpc(:Read, S2.V1alpha.ReadRequest, S2.V1alpha.ReadResponse)

  rpc(:ReadSession, S2.V1alpha.ReadSessionRequest, stream(S2.V1alpha.ReadSessionResponse))
end

defmodule S2.V1alpha.StreamService.Stub do
  @moduledoc false

  use GRPC.Stub, service: S2.V1alpha.StreamService.Service
end
