defmodule Sequin.Bench.Test do
  def run do
    map_fun = fn i -> [i, i * i] end

    Sequin.Bench.run(
      [
        {"flat_map", fn input -> Enum.flat_map(input, map_fun) end},
        {"map.flatten", fn input -> input |> Enum.map(map_fun) |> List.flatten() end}
      ],
      max_time_s: 3,
      warmup_time_s: 1,
      parallel: [1, 4],
      inputs: [
        {"Small", Enum.to_list(1..1_000)},
        {"Medium", Enum.to_list(1..10_000)},
        {"Large", Enum.to_list(1..100_000)}
      ]
    )
  end
end
