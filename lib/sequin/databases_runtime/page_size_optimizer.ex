defmodule Sequin.DatabasesRuntime.PageSizeOptimizer do
  @moduledoc """
  A module to dynamically calculate the page size for queries such that we maximize
  rows per ms while keeping query time under max_timeout_ms.

  On initialization, you specify:
    - initial_page_size
    - max_timeout_ms

  Functions:
    - put_timing/3: adds a timing measurement {page_size, time_ms} to the history.
    - put_timeout/2: records that a particular page_size has timed out.
    - size/1: returns the page_size to try for the next query.
  """
  use TypedStruct

  @max_history_entries 20

  defmodule Timing do
    @moduledoc false
    use TypedStruct

    @derive Jason.Encoder
    typedstruct do
      field :page_size, pos_integer(), enforce: true
      field :time_ms, pos_integer(), enforce: true
      field :timed_out, boolean(), default: false
    end
  end

  typedstruct do
    field :initial_page_size, pos_integer()
    field :max_timeout_ms, pos_integer()
    field :max_page_size, pos_integer()
    field :history, [Timing.t()], default: []
  end

  @callback new(opts :: keyword()) :: any()
  @callback put_timing(state :: any(), page_size :: pos_integer(), time_ms :: pos_integer()) :: any()
  @callback put_timeout(state :: any(), page_size :: pos_integer()) :: any()
  @callback size(state :: any()) :: pos_integer()
  @callback history(state :: any()) :: [Timing.t()]

  @doc """
  Initializes the state with optional configuration.

  ## Options
    * `:initial_page_size` - Starting page size
    * `:max_timeout_ms` - Maximum timeout in milliseconds
  """
  def new(opts \\ []) do
    %__MODULE__{
      initial_page_size: Keyword.get(opts, :initial_page_size, 100),
      max_timeout_ms: Keyword.get(opts, :max_timeout_ms, 5000),
      max_page_size: Keyword.get(opts, :max_page_size, 10_000)
    }
  end

  @doc """
  Records a successful query timing.

  Adds `%{page_size: page_size, time_ms: time_ms, timed_out: false}` to the history.
  """
  def put_timing(%__MODULE__{} = state, page_size, time_ms) when time_ms > 0 do
    entry = %{page_size: page_size, time_ms: time_ms, timed_out: false}
    update_history(state, entry)
  end

  @doc """
  Records a timeout event.

  Adds `%{page_size: page_size, time_ms: state.max_timeout_ms, timed_out: true}` to the history.
  """
  def put_timeout(%__MODULE__{max_timeout_ms: max_timeout_ms} = state, page_size) do
    entry = %{page_size: page_size, time_ms: max_timeout_ms, timed_out: true}
    update_history(state, entry)
  end

  @doc """
  Returns the current history of timings.
  """
  def history(state) do
    state.history
  end

  defp update_history(state, entry) do
    new_history = state.history ++ [entry]
    # keep only the most recent @max_history_entries measurements
    new_history = Enum.take(new_history, -@max_history_entries)
    %{state | history: new_history}
  end

  @doc """
  Computes the next page_size to use based on historical timings.

  The strategy is:
  1. If there are both successes and timeouts, we use a binary search: we pick
     the midpoint between the largest successful page_size and the smallest timed out page_size.
  2. If there are only successful measurements, we use the last successful measurement's
     ratio (max_timeout_ms / time_ms) to decide whether we can increase the page_size,
     clamping the increase to avoid drastic jumps.
  3. If there are only timeouts, we reduce the last timed out page_size.
  4. Rounding is applied to get "nice" page sizes.
  """
  def size(%__MODULE__{} = state) do
    successes = Enum.filter(state.history, fn m -> not m.timed_out end)
    timeouts = Enum.filter(state.history, fn m -> m.timed_out end)

    candidate =
      cond do
        successes != [] and timeouts != [] ->
          lb = Enum.max_by(successes, & &1.page_size).page_size
          ub = Enum.min_by(timeouts, & &1.page_size).page_size

          if lb < ub do
            (lb + ub) / 2
          else
            lb * 0.9
          end

        successes != [] ->
          # use the ratio of max_timeout_ms to the last successful time_ms
          latest = List.last(successes)
          ratio = state.max_timeout_ms / latest.time_ms
          # if the ratio is significantly more than 1.0, we can grow—but limit the stretch
          if ratio > 1.2 do
            latest.page_size * min(ratio, 2.0)
          else
            latest.page_size
          end

        timeouts != [] ->
          latest = List.last(timeouts)
          latest.page_size * 0.8

        true ->
          state.initial_page_size
      end

    candidate
    |> nudge_page_size(List.last(state.history), state.max_page_size)
    |> round_page_size()
  end

  # This helper can "nudge" the candidate so that it never jumps too much from the last value.
  defp nudge_page_size(candidate, nil, _), do: candidate

  defp nudge_page_size(candidate, last_entry, max_page_size) do
    last = last_entry.page_size
    # Ensure we don't jump more than 2x up or 50% down in one shot, and respect max_page_size
    candidate
    |> min(last * 2)
    |> min(max_page_size)
    |> max(last * 0.5)
  end

  # Rounds the candidate page_size to a "nice" number.
  defp round_page_size(candidate) do
    candidate = round(candidate)

    cond do
      candidate < 10 ->
        candidate

      candidate < 100 ->
        round(candidate / 10) * 10

      candidate < 1000 ->
        round(candidate / 50) * 50

      true ->
        round(candidate / 100) * 100
    end
  end
end
