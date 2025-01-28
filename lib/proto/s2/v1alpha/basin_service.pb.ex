defmodule S2.V1alpha.BasinService.Service do
  @moduledoc false

  use GRPC.Service, name: "s2.v1alpha.BasinService", protoc_gen_elixir_version: "0.14.0"

  rpc(:ListStreams, S2.V1alpha.ListStreamsRequest, S2.V1alpha.ListStreamsResponse)

  rpc(:CreateStream, S2.V1alpha.CreateStreamRequest, S2.V1alpha.CreateStreamResponse)

  rpc(:DeleteStream, S2.V1alpha.DeleteStreamRequest, S2.V1alpha.DeleteStreamResponse)

  rpc(:GetStreamConfig, S2.V1alpha.GetStreamConfigRequest, S2.V1alpha.GetStreamConfigResponse)

  rpc(
    :ReconfigureStream,
    S2.V1alpha.ReconfigureStreamRequest,
    S2.V1alpha.ReconfigureStreamResponse
  )
end

defmodule S2.V1alpha.BasinService.Stub do
  @moduledoc false

  use GRPC.Stub, service: S2.V1alpha.BasinService.Service
end
