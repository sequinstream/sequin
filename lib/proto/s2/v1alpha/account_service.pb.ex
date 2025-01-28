defmodule S2.V1alpha.AccountService.Service do
  @moduledoc false

  use GRPC.Service, name: "s2.v1alpha.AccountService", protoc_gen_elixir_version: "0.14.0"

  rpc(:ListBasins, S2.V1alpha.ListBasinsRequest, S2.V1alpha.ListBasinsResponse)

  rpc(:CreateBasin, S2.V1alpha.CreateBasinRequest, S2.V1alpha.CreateBasinResponse)

  rpc(:DeleteBasin, S2.V1alpha.DeleteBasinRequest, S2.V1alpha.DeleteBasinResponse)

  rpc(:ReconfigureBasin, S2.V1alpha.ReconfigureBasinRequest, S2.V1alpha.ReconfigureBasinResponse)

  rpc(:GetBasinConfig, S2.V1alpha.GetBasinConfigRequest, S2.V1alpha.GetBasinConfigResponse)
end

defmodule S2.V1alpha.AccountService.Stub do
  @moduledoc false

  use GRPC.Stub, service: S2.V1alpha.AccountService.Service
end
