defmodule Sequin.Case do
  @moduledoc """
  This module defines shared infrastructure for all of our tests.

  It should only import things that we want available for every test case.
  """

  use ExUnit.CaseTemplate

  using do
    quote do
      import Sequin.Test.Assertions
      import Sequin.TestSupport
      import unquote(__MODULE__)

      setup :start_supervisors
      setup :verify_stubs
      setup :default_stubs
    end
  end

  def start_supervisors(context) do
    sup_tree =
      context
      |> Map.get(:start_supervisors, [])
      |> Map.new(fn supervisor ->
        name = Module.concat([context.module, supervisor])
        start_supervised!({supervisor, name: name, id: name})
        {supervisor, name}
      end)

    {:ok, supervisors: sup_tree}
  end

  def default_stubs(_context) do
    Sequin.TestSupport.stub_utc_now(fn -> DateTime.utc_now() end)
    Sequin.TestSupport.stub_uuid4(fn -> UUID.uuid4() end)
    Sequin.TestSupport.stub_random(fn n -> Enum.random(n) end)
    Sequin.TestSupport.stub_process_alive?(fn pid -> Process.alive?(pid) end)
    Sequin.TestSupport.stub_application_get_env(fn atom, mod -> Application.get_env(atom, mod) end)

    :ok
  end

  def verify_stubs(_context) do
    # NOTE: Reorder these verify_on_exit! and run the tests to see the problem
    Hammox.verify_on_exit!() # This calls Mox.verify_on_exit!() internally
    Req.Test.verify_on_exit!()
    :ok
  end

  def errors_on(changeset) do
    Sequin.Error.errors_on(changeset)
  end

  def flash_text(conn, kind) do
    if toast = Phoenix.Flash.get(conn.assigns.flash, :toast) do
      if toast.kind == kind do
        Enum.join([Map.get(toast, :title), Map.get(toast, :description)], "\n")
      end
    else
      Phoenix.Flash.get(conn.assigns.flash, kind)
    end
  end
end

defmodule Sequin.Test.Sample do
  @moduledoc false

  @callback call_mox() :: atom()
  @callback call_hammox() :: atom()
  @callback call_req() :: atom()

  def call_mox, do: impl_mox().call_mox()
  def call_hammox, do: impl_hammox().call_hammox()
  def call_req do
    [
      base_url: "https://my.wonderful.weather.service.com.local",
    ]
    |> Keyword.merge(impl_req_opts())
    |> Req.request()
  end

  defp impl_mox, do: Application.get_env(:sequin, :test_mock_mox, Sequin.Test.RealSample)
  defp impl_hammox, do: Application.get_env(:sequin, :test_mock_hammox, Sequin.Test.RealSample)
  defp impl_req_opts, do: Application.get_env(:sequin, :test_mock_req, [])
end

defmodule Sequin.Test.RealSample do
  @behaviour Sequin.Test.Sample

  def call_mox, do: :error
  def call_hammox, do: :error
  def call_req, do: :error
end

defmodule Req.Test do
  @moduledoc """
  Req testing conveniences.

  Req is composed of:

    * `Req` - the high-level API

    * `Req.Request` - the low-level API and the request struct

    * `Req.Steps` - the collection of built-in steps

    * `Req.Test` - the testing conveniences (you're here!)

  Req already has built-in support for different variants of stubs via `:plug`, `:adapter`,
  and (indirectly) `:base_url` options. With this module you can:

    * Create request stubs using [`Req.Test.stub(name, plug)`](`stub/2`) and mocks
      using [`Req.Test.expect(name, count, plug)`](`expect/3`). Both can be used in concurrent
      tests.

    * Configure Req to run requests through mocks/stubs by setting `plug: {Req.Test, name}`.
      This works because `Req.Test` itself is a plug whose job is to fetch the mocks/stubs under
      `name`.

    * Easily create JSON responses with [`Req.Test.json(conn, body)`](`json/2`),
      HTML responses with [`Req.Test.html(conn, body)`](`html/2`), and
      text responses with [`Req.Test.text(conn, body)`](`text/2`).

    * Simulate network errors with [`Req.Test.transport_error(conn, reason)`](`transport_error/2`).

  Mocks and stubs are using the same ownership model of
  [nimble_ownership](https://hex.pm/packages/nimble_ownership), also used by
  [Mox](https://hex.pm/packages/mox). This allows `Req.Test` to be used in concurrent tests.

  ## Example

  Imagine we're building an app that displays weather for a given location using an HTTP weather
  service:

      defmodule MyApp.Weather do
        def get_rating(location) do
          case get_temperature(location) do
            {:ok, %{status: 200, body: %{"celsius" => celsius}}} ->
              cond do
                celsius < 18.0 -> {:ok, :too_cold}
                celsius < 30.0 -> {:ok, :nice}
                true -> {:ok, :too_hot}
              end

            _ ->
              :error
          end
        end

        def get_temperature(location) do
          [
            base_url: "https://weather-service",
            params: [location: location]
          ]
          |> Keyword.merge(Application.get_env(:myapp, :weather_req_options, []))
          |> Req.request()
        end
      end

  We configure it for production:

      # config/runtime.exs
      config :myapp, weather_req_options: [
        auth: {:bearer, System.fetch_env!("MYAPP_WEATHER_API_KEY")}
      ]

  In tests, instead of hitting the network, we make the request against
  a [plug](`Req.Steps.run_plug/1`) _stub_ named `MyApp.Weather`:

      # config/test.exs
      config :myapp, weather_req_options: [
        plug: {Req.Test, MyApp.Weather}
      ]

  Now we can control our stubs **in concurrent tests**:

      use ExUnit.Case, async: true

      test "nice weather" do
        Req.Test.stub(MyApp.Weather, fn conn ->
          Req.Test.json(conn, %{"celsius" => 25.0})
        end)

        assert MyApp.Weather.get_rating("Krakow, Poland") == {:ok, :nice}
      end

  ## Concurrency and Allowances

  The example above works in concurrent tests because `MyApp.Weather.get_rating/1` calls
  directly to `Req.request/1` *in the same process*. It also works in many cases where the
  request happens in a spawned process, such as a `Task`, `GenServer`, and more.

  However, if you are encountering issues with stubs not being available in spawned processes,
  it's likely that you'll need **explicit allowances**. For example, if
  `MyApp.Weather.get_rating/1` was calling `Req.request/1` in a process spawned with `spawn/1`,
  the stub would not be available in the spawned process:

      # With code like this, the stub would not be available in the spawned task:
      def get_rating_async(location) do
        spawn(fn -> get_rating(location) end)
      end

  To make stubs defined in the test process available in other processes, you can use
  `allow/3`. For example, imagine that the call to `MyApp.Weather.get_rating/1`
  was happening in a spawned GenServer:

      test "nice weather" do
        {:ok, pid} = start_gen_server(...)

        Req.Test.stub(MyApp.Weather, fn conn ->
          Req.Test.json(conn, %{"celsius" => 25.0})
        end)

        Req.Test.allow(MyApp.Weather, self(), pid)

        assert get_weather(pid, "Krakow, Poland") == {:ok, :nice}
      end

  ## Broadway

  If you're using `Req.Test` with [Broadway](https://hex.pm/broadway), you may need to use
  `allow/3` to make stubs available in the Broadway processors. A great way to do that is
  to hook into the [Telemetry](https://hex.pm/telemetry) events that Broadway publishes to
  manually allow the processors and batch processors to access the stubs. This approach is
  similar to what is [documented in Broadway
  itself](https://hexdocs.pm/broadway/Broadway.html#module-testing-with-ecto).

  First, you should add the test PID (which is allowed to use the Req stub) to the metadata
  for the test events you're publishing:

      Broadway.test_message(MyApp.Pipeline, message, metadata: %{req_stub_owner: self()})

  Then, you'll need to define a test helper to hook into the Telemetry events. For example,
  in your `test/test_helper.exs` file:

      defmodule BroadwayReqStubs do
        def attach(stub) do
          events = [
            [:broadway, :processor, :start],
            [:broadway, :batch_processor, :start],
          ]

          :telemetry.attach_many({__MODULE__, stub}, events, &__MODULE__.handle_event/4, %{stub: stub})
        end

        def handle_event(_event_name, _event_measurement, %{messages: messages}, %{stub: stub}) do
          with [%Broadway.Message{metadata: %{req_stub_owner: pid}} | _] <- messages do
            :ok = Req.Test.allow(stub, pid, self())
          end

          :ok
        end
      end

  Last but not least, attach the helper in your `test/test_helper.exs`:

      BroadwayReqStubs.attach(MyStub)

  """

  require Logger

  @typep name() :: term()

  if Code.ensure_loaded?(Plug.Conn) do
    @typep plug() ::
             module()
             | {module(), term()}
             | (Plug.Conn.t() -> Plug.Conn.t())
             | (Plug.Conn.t(), term() -> Plug.Conn.t())
  else
    @typep plug() ::
             module()
             | {module, term()}
             | (conn :: term() -> term())
             | (conn :: term(), term() -> term())
  end

  @ownership Req.Test.Ownership

  @doc """
  Sends JSON response.

  ## Examples

      iex> plug = fn conn ->
      ...>   Req.Test.json(conn, %{celsius: 25.0})
      ...> end
      iex>
      iex> resp = Req.get!(plug: plug)
      iex> resp.headers["content-type"]
      ["application/json; charset=utf-8"]
      iex> resp.body
      %{"celsius" => 25.0}

  """
  if Code.ensure_loaded?(Plug.Test) do
    @spec json(Plug.Conn.t(), term()) :: Plug.Conn.t()
    def json(%Plug.Conn{} = conn, data) do
      send_resp(conn, conn.status || 200, "application/json", Jason.encode_to_iodata!(data))
    end

    defp send_resp(conn, default_status, default_content_type, body) do
      conn
      |> ensure_resp_content_type(default_content_type)
      |> Plug.Conn.send_resp(conn.status || default_status, body)
    end

    defp ensure_resp_content_type(%Plug.Conn{resp_headers: resp_headers} = conn, content_type) do
      if List.keyfind(resp_headers, "content-type", 0) do
        conn
      else
        content_type = content_type <> "; charset=utf-8"
        %{conn | resp_headers: [{"content-type", content_type} | resp_headers]}
      end
    end
  else
    def json(_conn, _data) do
      Logger.error("""
      Could not find plug dependency.

      Please add :plug to your dependencies:

          {:plug, "~> 1.0"}
      """)

      raise "missing plug dependency"
    end
  end

  @doc """
  Sends HTML response.

  ## Examples

      iex> plug = fn conn ->
      ...>   Req.Test.html(conn, "<h1>Hello, World!</h1>")
      ...> end
      iex>
      iex> resp = Req.get!(plug: plug)
      iex> resp.headers["content-type"]
      ["text/html; charset=utf-8"]
      iex> resp.body
      "<h1>Hello, World!</h1>"

  """
  if Code.ensure_loaded?(Plug.Test) do
    @spec html(Plug.Conn.t(), iodata()) :: Plug.Conn.t()
    def html(%Plug.Conn{} = conn, data) do
      send_resp(conn, conn.status || 200, "text/html", data)
    end
  else
    def html(_conn, _data) do
      Logger.error("""
      Could not find plug dependency.

      Please add :plug to your dependencies:

          {:plug, "~> 1.0"}
      """)

      raise "missing plug dependency"
    end
  end

  @doc """
  Sends text response.

  ## Examples

      iex> plug = fn conn ->
      ...>   Req.Test.text(conn, "Hello, World!")
      ...> end
      iex>
      iex> resp = Req.get!(plug: plug)
      iex> resp.headers["content-type"]
      ["text/plain; charset=utf-8"]
      iex> resp.body
      "Hello, World!"

  """
  if Code.ensure_loaded?(Plug.Test) do
    @spec text(Plug.Conn.t(), iodata()) :: Plug.Conn.t()
    def text(%Plug.Conn{} = conn, data) do
      send_resp(conn, conn.status || 200, "text/plain", data)
    end
  else
    def text(_conn, _data) do
      Logger.error("""
      Could not find plug dependency.

      Please add :plug to your dependencies:

          {:plug, "~> 1.0"}
      """)

      raise "missing plug dependency"
    end
  end

  @doc """
  Simulates a network transport error.

  ## Examples

      iex> plug = fn conn ->
      ...>   Req.Test.transport_error(conn, :timeout)
      ...> end
      iex>
      iex> Req.get(plug: plug, retry: false)
      {:error, %Req.TransportError{reason: :timeout}}

  """
  @doc since: "0.5.0"
  def transport_error(conn, reason)

  if Code.ensure_loaded?(Plug.Conn) do
    @spec transport_error(Plug.Conn.t(), reason :: atom()) :: Plug.Conn.t()
    def transport_error(%Plug.Conn{} = conn, reason) do
      validate_transport_error!(reason)
      exception = Req.TransportError.exception(reason: reason)
      put_in(conn.private[:req_test_exception], exception)
    end

    defp validate_transport_error!(:protocol_not_negotiated), do: :ok
    defp validate_transport_error!({:bad_alpn_protocol, _}), do: :ok
    defp validate_transport_error!(:closed), do: :ok
    defp validate_transport_error!(:timeout), do: :ok

    defp validate_transport_error!(reason) do
      case :ssl.format_error(reason) do
        ~c"Unexpected error:" ++ _ ->
          raise ArgumentError, "unexpected Req.TransportError reason: #{inspect(reason)}"

        _ ->
          :ok
      end
    end
  else
    def transport_error(_conn, _reason) do
      Logger.error("""
      Could not find plug dependency.

      Please add :plug to your dependencies:

          {:plug, "~> 1.0"}
      """)

      raise "missing plug dependency"
    end
  end

  @doc false
  @deprecated "Don't manually fetch stubs. See the documentation for Req.Test instead."
  def stub(name) do
    __fetch_plug__(name)
  end

  def __fetch_plug__(name) do
    case Req.Test.Ownership.fetch_owner(@ownership, callers(), name) do
      {tag, owner} when is_pid(owner) and tag in [:ok, :shared_owner] ->
        result =
          Req.Test.Ownership.get_and_update(@ownership, owner, name, fn
            %{expectations: [value | rest]} = map ->
              {{:ok, value}, put_in(map[:expectations], rest)}

            %{stub: value} = map ->
              {{:ok, value}, map}

            %{expectations: []} = map ->
              {{:error, :no_expectations_and_no_stub}, map}
          end)

        case result do
          {:ok, {:ok, value}} ->
            value

          {:ok, {:error, :no_expectations_and_no_stub}} ->
            raise "no mock or stub for #{inspect(name)}"
        end

      :error ->
        raise "cannot find mock/stub #{inspect(name)} in process #{inspect(self())}"
    end
  end

  defguardp is_plug(value)
            when is_function(value, 1) or
                   is_function(value, 2) or
                   is_atom(value) or
                   (is_tuple(value) and tuple_size(value) == 2 and is_atom(elem(value, 0)))

  @doc """
  Creates a request stub with the given `name` and `plug`.

  Req allows running requests against _plugs_ (instead of over the network) using the
  [`:plug`](`Req.Steps.run_plug/1`) option. However, passing the `:plug` value throughout the
  system can be cumbersome. Instead, you can tell Req to find plugs by `name` by setting
  `plug: {Req.Test, name}`, and register plug stubs for that `name` by calling
  `Req.Test.stub(name, plug)`. In other words, multiple concurrent tests can register test stubs
  under the same `name`, and when Req makes the request, it will find the appropriate
  implementation, even when invoked from different processes than the test process.

  The `name` can be any term.

  The `plug` can be one of:

    * A _function_ plug: a `fun(conn)` or `fun(conn, options)` function that takes a
      `Plug.Conn` and returns a `Plug.Conn`.

    * A _module_ plug: a `module` name or a `{module, options}` tuple.

  ## Examples

      iex> Req.Test.stub(MyStub, fn conn ->
      ...>   send(self(), :req_happened)
      ...>   Req.Test.json(conn, %{})
      ...> end)
      :ok
      iex> Req.get!(plug: {Req.Test, MyStub}).body
      %{}
      iex> receive do
      ...>   :req_happened -> :ok
      ...> end
      :ok

  """
  @doc type: :mock
  @spec stub(name(), plug()) :: :ok
  def stub(name, plug) when is_plug(plug) do
    {:ok, :ok} =
      Req.Test.Ownership.get_and_update(@ownership, self(), name, fn map_or_nil ->
        {:ok, put_in(map_or_nil || %{}, [:stub], plug)}
      end)

    :ok
  end

  @doc """
  Creates a request expectation with the given `name` and `plug`, expected to be fetched at
  most `n` times, **in order**.

  This function allows you to expect a `n` number of request and handle them **in order** via the
  given `plug`. It is safe to use in concurrent tests. If you fetch the value under `name` more
  than `n` times, this function raises a `RuntimeError`.

  The `name` can be any term.

  The `plug` can be one of:

    * A _function_ plug: a `fun(conn)` or `fun(conn, options)` function that takes a
      `Plug.Conn` and returns a `Plug.Conn`.

    * A _module_ plug: a `module` name or a `{module, options}` tuple.

  See `stub/2` and module documentation for more information.

  ## Examples

  Let's simulate a server that is having issues: on the first request it is not responding
  and on the following two requests it returns an HTTP 500. Only on the third request it returns
  an HTTP 200. Req by default automatically retries transient errors (using `Req.Steps.retry/1`)
  so it will make multiple requests exercising all of our request expectations:

      iex> Req.Test.expect(MyStub, &Req.Test.transport_error(&1, :econnrefused))
      iex> Req.Test.expect(MyStub, 2, &Plug.Conn.send_resp(&1, 500, "internal server error"))
      iex> Req.Test.expect(MyStub, &Plug.Conn.send_resp(&1, 200, "ok"))
      iex> Req.get!(plug: {Req.Test, MyStub}).body
      # 15:57:06.309 [warning] retry: got exception, will retry in 1000ms, 3 attempts left
      # 15:57:06.309 [warning] ** (Req.TransportError) connection refused
      # 15:57:07.310 [warning] retry: got response with status 500, will retry in 2000ms, 2 attempts left
      # 15:57:09.311 [warning] retry: got response with status 500, will retry in 4000ms, 1 attempt left
      "ok"

      iex> Req.request!(plug: {Req.Test, MyStub})
      ** (RuntimeError) no mock or stub for MyStub

  """
  @doc since: "0.4.15"
  @doc type: :mock
  @spec expect(name(), pos_integer(), plug()) :: name()
  def expect(name, n \\ 1, plug) when is_integer(n) and n > 0 do
    plugs = List.duplicate(plug, n)

    {:ok, :ok} =
      Req.Test.Ownership.get_and_update(@ownership, self(), name, fn map_or_nil ->
        {:ok, Map.update(map_or_nil || %{}, :expectations, plugs, &(&1 ++ plugs))}
      end)

    name
  end

  @doc """
  Allows `pid_to_allow` to access `name` provided that `owner` is already allowed.
  """
  @doc type: :mock
  @spec allow(name(), pid(), pid() | (-> pid())) :: :ok | {:error, Exception.t()}
  def allow(name, owner, pid_to_allow) when is_pid(owner) do
    Req.Test.Ownership.allow(@ownership, owner, pid_to_allow, name)
  end

  @doc """
  Sets the `Req.Test` mode to "global", meaning that the stubs are shared across all tests
  and cannot be used concurrently.
  """
  @doc since: "0.5.0"
  @doc type: :mock
  @spec set_req_test_to_shared(ex_unit_context :: term()) :: :ok
  def set_req_test_to_shared(_context \\ %{}) do
    Req.Test.Ownership.set_mode_to_shared(@ownership, self())
  end

  @doc """
  Sets the `Req.Test` mode to "private", meaning that stubs can be shared across
  tests concurrently.
  """
  @doc type: :mock
  @doc since: "0.5.0"
  @spec set_req_test_to_private(ex_unit_context :: term()) :: :ok
  def set_req_test_to_private(_context \\ %{}) do
    Req.Test.Ownership.set_mode_to_private(@ownership)
  end

  @doc """
  Sets the `Req.Test` mode based on the given `ExUnit` context.

  This works as a ExUnit callback:

      setup :set_req_test_from_context

  """
  @doc since: "0.5.0"
  @doc type: :mock
  @spec set_req_test_from_context(ex_unit_context :: term()) :: :ok
  def set_req_test_from_context(_context \\ %{})

  def set_req_test_from_context(%{async: true} = context), do: set_req_test_to_private(context)
  def set_req_test_from_context(context), do: set_req_test_to_shared(context)

  @doc """
  Sets a ExUnit callback to verify the expectations on exit.

  Similar to calling `verify!/0` at the end of your test.
  """
  @doc since: "0.5.0"
  @doc type: :mock
  @spec verify_on_exit!(term()) :: :ok
  def verify_on_exit!(_context \\ %{}) do
    pid = self()
    Req.Test.Ownership.set_owner_to_manual_cleanup(@ownership, pid)

    # NOTE Uncomment this line to fix inconsistencies with verify_on_exit!
    # ExUnit.Callbacks.on_exit(Req, fn ->
    ExUnit.Callbacks.on_exit(Mox, fn ->
      verify(pid, :all)
      Req.Test.Ownership.cleanup_owner(@ownership, pid)
    end)
  end

  @doc """
  Verifies that all the plugs expected to be executed within any scope have been executed.
  """
  @doc since: "0.5.0"
  @doc type: :mock
  @spec verify!() :: :ok
  def verify! do
    verify(self(), :all)
  end

  @doc """
  Verifies that all the plugs expected to be executed within the scope of `name` have been
  executed.
  """
  @doc type: :mock
  @doc since: "0.5.0"
  @spec verify!(name()) :: :ok
  def verify!(name) do
    verify(self(), name)
  end

  defp verify(owner_pid, mock_or_all) do
    messages =
      for {name, stubs_and_expecs} <-
            Req.Test.Ownership.get_owned(@ownership, owner_pid, _default = %{}, 5000),
          name == mock_or_all or mock_or_all == :all,
          pending_count = stubs_and_expecs |> Map.get(:expectations, []) |> length(),
          pending_count > 0 do
        "  * expected #{inspect(name)} to be still used #{pending_count} more times"
      end

    if messages != [] do
      raise "error while verifying Req.Test expectations for #{inspect(owner_pid)}:\n\n" <>
              Enum.join(messages, "\n")
    end

    :ok
  end

  ## Helpers

  defp callers do
    [self() | Process.get(:"$callers") || []]
  end

  ## Plug callbacks

  if Code.ensure_loaded?(Plug) do
    @behaviour Plug
  end

  @doc false
  def init(name) do
    name
  end

  @doc false
  def call(conn, name) do
    case __fetch_plug__(name) do
      fun when is_function(fun, 1) ->
        fun.(conn)

      fun when is_function(fun, 2) ->
        fun.(conn, [])

      module when is_atom(module) ->
        module.call(conn, module.init([]))

      {module, options} when is_atom(module) ->
        module.call(conn, module.init(options))

      other ->
        raise """
        expected plug to be one of:

          * fun(conn)
          * fun(conn, options)
          * module
          * {module, options}

        got: #{inspect(other)}\
        """
    end
  end
end
