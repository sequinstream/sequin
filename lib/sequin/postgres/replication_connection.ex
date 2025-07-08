defmodule Sequin.Postgres.ReplicationConnection do
  @moduledoc ~S"""
  Vendored from the Postgrex library.

  A process that receives and sends PostgreSQL replication messages.

  > Note: this module is experimental and may be subject to changes
  > in the future.

  ## Logical replication

  Let's see how to use this module for connecting to PostgreSQL
  for logical replication. First of all, you need to configure the
  wal level in PostgreSQL to logical. Run this inside your PostgreSQL
  shell/configuration:

      ALTER SYSTEM SET wal_level='logical';
      ALTER SYSTEM SET max_wal_senders='10';
      ALTER SYSTEM SET max_replication_slots='10';

  Then **you must restart your server**. Alternatively, you can set
  those values when starting "postgres". This is useful, for example,
  when running it from Docker:

      services:
        postgres:
          image: postgres:14
          env:
            ...
          command: ["postgres", "-c", "wal_level=logical"]

  For CI, GitHub Actions do not support setting command, so you can
  update and restart Postgres instead in a step:

      - name: "Set PG settings"
        run: |
          docker exec ${{ job.services.postgres.id }} sh -c 'echo "wal_level=logical" >> /var/lib/postgresql/data/postgresql.conf'
          docker restart ${{ job.services.pg.id }}

  Then you must create a publication to be replicated.
  This can be done in any session:

      CREATE PUBLICATION postgrex_example FOR ALL TABLES;

  You can also filter if you want to publish insert, update,
  delete or a subset of them:

      # Skips updates (keeps inserts, deletes, begins, commits, etc)
      create PUBLICATION postgrex_example FOR ALL TABLES WITH (publish = 'insert,delete');

      # Skips inserts, updates, and deletes (keeps begins, commits, etc)
      create PUBLICATION postgrex_example FOR ALL TABLES WITH (publish = '');

  Now we are ready to create module that starts a replication slot
  and listens to our publication. Our example will use the pgoutput
  for logical replication and print all incoming messages to the
  terminal:

      Mix.install([:postgrex])

      defmodule Repl do
        use Postgrex.ReplicationConnection

        def start_link(opts) do
          Postgrex.ReplicationConnection.start_link(__MODULE__, :ok, extra_opts ++ opts)
        end

        @impl true
        def init(:ok) do
          {:ok, %{step: :disconnected}}
        end

        @impl true
        def handle_connect(state) do
          query = "CREATE_REPLICATION_SLOT postgrex TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT"
          {:query, query, %{state | step: :create_slot}}
        end

        @impl true
        def handle_result(results, %{step: :create_slot} = state) when is_list(results) do
          query = "START_REPLICATION SLOT postgrex LOGICAL 0/0 (proto_version '1', publication_names 'postgrex_example')"
          {:stream, query, [], %{state | step: :streaming}}
        end

        @impl true
        # https://www.postgresql.org/docs/14/protocol-replication.html
        def handle_data(<<?w, _wal_start::64, _wal_end::64, _clock::64, rest::binary>>, state) do
          IO.inspect(rest)
          {:noreply, state}
        end

        def handle_data(<<?k, wal_end::64, _clock::64, reply>>, state) do
          messages =
            case reply do
              1 -> [<<?r, wal_end + 1::64, wal_end + 1::64, wal_end + 1::64, current_time()::64, 0>>]
              0 -> []
            end

          {:noreply, messages, state}
        end

        @epoch DateTime.to_unix(~U[2000-01-01 00:00:00Z], :microsecond)
        defp current_time(), do: System.os_time(:microsecond) - @epoch
      end

      {:ok, pid} =
        Repl.start_link(
          host: "localhost",
          database: "demo_dev",
          username: "postgres",
        )

      Process.sleep(:infinity)

  ## `use` options

  `use Postgrex.ReplicationConnection` accepts a list of options which configures the
  child specification and therefore how it runs under a supervisor.
  The generated `child_spec/1` can be customized with the following options:

    * `:id` - the child specification identifier, defaults to the current module
    * `:restart` - when the child should be restarted, defaults to `:permanent`
    * `:shutdown` - how to shut down the child, either immediately or by giving
      it time to shut down

  For example:

      use Postgrex.ReplicationConnection, restart: :transient, shutdown: 10_000

  See the "Child specification" section in the `Supervisor` module for more
  detailed information. The `@doc` annotation immediately preceding
  `use Postgrex.ReplicationConnection` will be attached to the generated `child_spec/1`
  function.

  ## Name registration

  A `Postgrex.ReplicationConnection` is bound to the same name registration rules as a
  `GenServer`. Read more about them in the `GenServer` docs.
  """

  @behaviour :gen_statem

  use Sequin.ProcessMetrics.Decorator

  import Bitwise

  alias Postgrex.Protocol

  require Logger

  @doc false
  defstruct protocol: nil,
            state: nil,
            reconnect_backoff: 500,
            streaming: nil,
            buffering_sock_messages?: false,
            buffered_sock_message: nil

  ## PUBLIC API ##

  @type server :: :gen_statem.server_ref()
  @type state :: term
  @type ack :: iodata
  @type query :: iodata
  @type reason :: String.t()
  @type error :: term()
  @type actions :: [tuple()]
  @typedoc """
  The following options configure streaming:

    * `:max_messages` - The maximum number of replications messages that can be
      accumulated from the wire until they are relayed to `handle_data/2`.
      Defaults to `500`.

  """
  @type stream_opts :: [max_messages: pos_integer]
  @max_lsn_component_size 8
  @max_uint64 18_446_744_073_709_551_615
  @max_messages 500

  @doc """
  Callback for process initialization.

  This is called once and before the Postgrex connection is established.
  """
  @callback init(term) :: {:ok, state}

  @doc """
  Invoked after connecting.
  """
  @callback handle_connect(state) ::
              {:keep_state, state}
              | {:keep_state, state, actions}
              | {:keep_state_and_ack, ack, state}
              | {:query, query, state}
              | {:stream, query, stream_opts, state}
              | {:disconnect, reason, state}

  @doc """
  Invoked if connection fails.
  """
  @callback handle_connect_failed(reason, state) ::
              {:keep_state, state} | {:keep_state, state, actions} | {:stop, reason, state}

  @doc """
  Invoked after disconnecting.
  """
  @callback handle_disconnect(error, reason, state) :: {:keep_state, state} | {:keep_state, state, actions}

  @doc """
  Callback for `:stream` outputs.

  If any callback returns `{:stream, iodata, opts, state}`, then this
  callback will be eventually called with the result of the query.
  It receives `binary` streaming messages.

  This can be useful for replication and copy commands. For replication,
  the format of the messages are described [under the START_REPLICATION
  section in PostgreSQL docs](https://www.postgresql.org/docs/14/protocol-replication.html).
  Replication messages may require explicit acknowledgement, which can
  be done by returning a list of binaries according to the replication
  protocol.
  """
  @callback handle_data(binary | :done, state) ::
              {:keep_state, state}
              | {:keep_state, state, actions}
              | {:keep_state_and_ack, ack, state}
              | {:query, query, state}
              | {:stream, query, stream_opts, state}
              | {:disconnect, reason, state}

  @doc """
  Callback for `Kernel.send/2`.
  """
  @callback handle_info(term, state) ::
              {:keep_state, state}
              | {:keep_state, state, actions}
              | {:keep_state_and_ack, ack, state}
              | {:query, query, state}
              | {:stream, query, stream_opts, state}
              | {:disconnect, reason, state}
              | {:inactivate_socket, state}
              | {:activate_socket, state}

  @doc """
  Callback for `call/3`.

  Replies must be sent with `reply/2`.
  """
  @callback handle_call(term, :gen_statem.from(), state) ::
              {:keep_state, state}
              | {:keep_state, state, actions}
              | {:keep_state_and_ack, ack, state}
              | {:query, query, state}
              | {:stream, query, stream_opts, state}
              | {:disconnect, reason, state}

  @doc """
  Callback for `:query` outputs.

  If any callback returns `{:query, iodata, state}`,
  then this callback will be immediately called with
  the result of the query. Please note that even though
  replication connections use the simple query protocol,
  Postgres currently limits them to single command queries.
  Due to this constraint, this callback will be passed
  either a list with a single successful query result or
  an error.
  """
  @callback handle_result([Postgrex.Result.t()] | Postgrex.Error.t(), state) ::
              {:keep_state, state}
              | {:keep_state, state, actions}
              | {:keep_state_and_ack, ack, state}
              | {:query, query, state}
              | {:stream, query, stream_opts, state}
              | {:disconnect, reason, state}

  @optional_callbacks handle_call: 3,
                      handle_connect: 1,
                      handle_data: 2,
                      handle_info: 2,
                      handle_result: 2

  @doc """
  Replies to the given `call/3`.
  """
  defdelegate reply(client, reply), to: :gen_statem

  @doc """
  Calls the given replication server.
  """
  def call(server, message, timeout \\ 5000) do
    with {__MODULE__, reason} <- :gen_statem.call(server, message, timeout) do
      exit({reason, {__MODULE__, :call, [server, message, timeout]}})
    end
  end

  @doc false
  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do
      @behaviour Sequin.Postgres.ReplicationConnection

      if !Module.has_attribute?(__MODULE__, :doc) do
        @doc """
        Returns a specification to start this module under a supervisor.

        See `Supervisor`.
        """
      end

      def child_spec(init_arg) do
        default = %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [init_arg]}
        }

        Supervisor.child_spec(default, unquote(Macro.escape(opts)))
      end

      defoverridable child_spec: 1
    end
  end

  @doc """
  Starts a replication process with the given callback `module`.

  ## Options

  The options that this function accepts are the same as those
  accepted by `Postgrex.start_link/1`, except for `:idle_interval`.

  It also accepts extra options for connection management, documented below.
  Also note this function also automatically set `:replication` to `"database"`
  as part of the connection `:parameters` if none is set yet.

  ### Connection options

    * `:sync_connect` - controls if the connection should be established on boot
      or asynchronously right after boot. Defaults to `true`.
  """
  @spec start_link(module(), term(), Keyword.t()) ::
          {:ok, pid} | {:error, Postgrex.Error.t() | term}
  def start_link(module, arg, opts) do
    {name, opts} = Keyword.pop(opts, :name)
    opts = Keyword.put_new(opts, :sync_connect, true)
    connection_opts = Postgrex.Utils.default_opts(opts)
    start_args = {module, arg, connection_opts}

    case name do
      nil ->
        :gen_statem.start_link(__MODULE__, start_args, [])

      atom when is_atom(atom) ->
        :gen_statem.start_link({:local, atom}, __MODULE__, start_args, [])

      {:global, _term} = tuple ->
        :gen_statem.start_link(tuple, __MODULE__, start_args, [])

      {:via, via_module, _term} = tuple when is_atom(via_module) ->
        :gen_statem.start_link(tuple, __MODULE__, start_args, [])

      other ->
        raise ArgumentError, """
        expected :name option to be one of the following:
          * nil
          * atom
          * {:global, term}
          * {:via, module, term}
        Got: #{inspect(other)}
        """
    end
  end

  @doc """
  Returns the string representation of an LSN value, given its integer representation.

  It returns `:error` if the provided integer falls outside the range for a valid
  unsigned 64-bit integer.

  ## Log Sequence Numbers

  PostgreSQL uses two representations for the Log Sequence Number (LSN):

    * An unsigned 64-bit integer. Used internally by PostgreSQL and sent in the XLogData
    replication messages.

    * A string of two hexadecimal numbers of up to eight digits each, separated
    by a slash. e.g. `1/F73E0220`. This is the form accepted by `start_replication/2`.

  For more information on Log Sequence Numbers, see
  [PostgreSQL pg_lsn docs](https://www.postgresql.org/docs/current/datatype-pg-lsn.html) and
  [PostgreSQL WAL internals docs](https://www.postgresql.org/docs/current/wal-internals.html).
  """
  @spec encode_lsn(integer) :: {:ok, String.t()} | :error
  def encode_lsn(lsn) when is_integer(lsn) do
    if 0 <= lsn and lsn <= @max_uint64 do
      <<file_id::32, offset::32>> = <<lsn::64>>
      {:ok, Integer.to_string(file_id, 16) <> "/" <> Integer.to_string(offset, 16)}
    else
      :error
    end
  end

  @doc """
  Returns the integer representation of an LSN value, given its string representation.

  It returns `:error` if the provided string is not a valid LSN.

  ## Log Sequence Numbers

  PostgreSQL uses two representations for the Log Sequence Number (LSN):

    * An unsigned 64-bit integer. Used internally by PostgreSQL and sent in the XLogData
    replication messages.

    * A string of two hexadecimal numbers of up to eight digits each, separated
    by a slash. e.g. `1/F73E0220`. This is the form accepted by `start_replication/2`.

  For more information on Log Sequence Numbers, see
  [PostgreSQL pg_lsn docs](https://www.postgresql.org/docs/current/datatype-pg-lsn.html) and
  [PostgreSQL WAL internals docs](https://www.postgresql.org/docs/current/wal-internals.html).
  """
  @spec decode_lsn(String.t()) :: {:ok, integer} | :error
  def decode_lsn(lsn) when is_binary(lsn) do
    with [file_id, offset] <- :binary.split(lsn, "/"),
         true <- byte_size(file_id) <= @max_lsn_component_size,
         true <- byte_size(offset) <= @max_lsn_component_size,
         {file_id, ""} when file_id >= 0 <- Integer.parse(file_id, 16),
         {offset, ""} when offset >= 0 <- Integer.parse(offset, 16) do
      {:ok, file_id <<< 32 ||| offset}
    else
      _ -> :error
    end
  end

  ## CALLBACKS ##

  @state :no_state

  @doc false
  @impl :gen_statem
  def callback_mode, do: :handle_event_function

  defguardp is_socket_message(message)
            when elem(message, 0) in [:tcp, :tcp_closed, :tcp_error, :ssl, :ssl_closed, :ssl_error]

  @doc false
  @impl :gen_statem
  def init({mod, arg, opts}) do
    case mod.init(arg) do
      {:ok, mod_state} ->
        opts =
          Keyword.update(
            opts,
            :parameters,
            [replication: "database"],
            &Keyword.put_new(&1, :replication, "database")
          )

        {reconnect_backoff, opts} = Keyword.pop(opts, :reconnect_backoff, 500)

        state = %__MODULE__{
          reconnect_backoff: reconnect_backoff,
          state: {mod, mod_state},
          buffered_sock_message: nil
        }

        put_opts(opts)

        if opts[:sync_connect] do
          case handle_event(:internal, {:connect, :init}, @state, state) do
            {:keep_state, state} -> {:ok, @state, state}
            {:keep_state, state, actions} -> {:ok, @state, state, actions}
            {:stop, reason, _state} -> {:stop, reason}
          end
        else
          {:ok, @state, state, {:next_event, :internal, {:connect, :init}}}
        end
    end
  end

  @doc false
  @impl :gen_statem
  def handle_event(type, content, state, s)

  def handle_event({:timeout, :backoff}, nil, @state, s) do
    {:keep_state, s, {:next_event, :internal, {:connect, :backoff}}}
  end

  def handle_event(:internal, {:connect, _info}, @state, %{state: {mod, mod_state}} = s) do
    case Protocol.connect(opts()) do
      {:ok, protocol} ->
        maybe_handle(mod, :handle_connect, [mod_state], %{s | protocol: protocol})

      {:error, reason} ->
        maybe_handle(mod, :handle_connect_failed, [reason, mod_state], s)
    end
  end

  def handle_event({:call, from}, msg, @state, %{state: {mod, mod_state}} = s) do
    handle(mod, :handle_call, [msg, from, mod_state], from, s)
  end

  def handle_event(:info, msg, @state, %__MODULE__{buffering_sock_messages?: true, buffered_sock_message: nil} = s)
      when is_socket_message(msg) do
    {:keep_state, %{s | buffered_sock_message: msg}}
  end

  def handle_event(:info, msg, @state, %__MODULE__{buffering_sock_messages?: true}) when is_socket_message(msg) do
    raise "Unexpectedly received a second socket message while buffering sock messages"
  end

  def handle_event(:info, msg, @state, %__MODULE__{protocol: protocol, streaming: streaming} = s)
      when is_socket_message(msg) do
    case Protocol.handle_copy_recv(msg, streaming, protocol) do
      {:ok, copies, protocol} ->
        handle_data(copies, %{s | protocol: protocol})

      {error, reason, protocol} ->
        reconnect_or_stop(error, reason, protocol, s)
    end
  end

  def handle_event(:info, msg, _state, s) do
    %{state: {mod, mod_state}} = s
    maybe_handle(mod, :handle_info, [msg, mod_state], s)
  end

  def handle_event({:timeout, :reconnect}, nil, @state, s) do
    Logger.info("Reconnecting to replication slot")
    {:keep_state, s, {:next_event, :internal, {:connect, :reconnect}}}
  end

  ## Helpers

  defp handle_data([], s), do: {:keep_state, s}

  defp handle_data([:copy_done | copies], %{state: {mod, mod_state}} = s) do
    with {:keep_state, s} <-
           handle(mod, :handle_data, [:done, mod_state], nil, %{s | streaming: nil}) do
      handle_data(copies, s)
    end
  end

  defp handle_data([copy | copies], %{state: {mod, mod_state}} = s) do
    with {:keep_state, s} <- handle(mod, :handle_data, [copy, mod_state], nil, s) do
      handle_data(copies, s)
    end
  end

  defp maybe_handle(mod, fun, args, s) do
    if function_exported?(mod, fun, length(args)) do
      handle(mod, fun, args, nil, s)
    else
      {:keep_state, s}
    end
  end

  defp handle(mod, fun, args, from, %__MODULE__{streaming: streaming} = s) do
    case apply(mod, fun, args) do
      {:keep_state, mod_state} ->
        {:keep_state, %{s | state: {mod, mod_state}}}

      {:keep_state, mod_state, actions} ->
        {:keep_state, %{s | state: {mod, mod_state}}, actions}

      {:keep_state_and_ack, replies, mod_state} ->
        s = %{s | state: {mod, mod_state}}

        case Protocol.handle_copy_send(replies, s.protocol) do
          :ok -> {:keep_state, s}
          {error, reason, protocol} -> reconnect_or_stop(error, reason, protocol, s)
        end

      {:inactivate_socket, mod_state} ->
        s = %{s | buffering_sock_messages?: true, state: {mod, mod_state}}
        {:keep_state, s}

      {:activate_socket, mod_state} ->
        s = maybe_flush_buffered_message(s)
        {:keep_state, %{s | state: {mod, mod_state}}}

      {:stream, query, opts, mod_state} when streaming == nil ->
        s = %{s | state: {mod, mod_state}}
        max_messages = opts[:max_messages] || @max_messages

        with {:ok, protocol} <- Protocol.handle_streaming(query, s.protocol),
             {:ok, protocol} <- Protocol.checkin(protocol) do
          {:keep_state, %{s | protocol: protocol, streaming: max_messages}}
        else
          {error_or_disconnect, reason, protocol} ->
            reconnect_or_stop(error_or_disconnect, reason, protocol, s)
        end

      {:stream, _query, _opts, mod_state} ->
        stream_in_progress(:stream, mod, mod_state, from, s)

      {:query, query, mod_state} when streaming == nil ->
        case Protocol.handle_simple(query, [], s.protocol) do
          {:ok, results, protocol} when is_list(results) ->
            handle(mod, :handle_result, [results, mod_state], from, %{s | protocol: protocol})

          {:error, %Postgrex.Error{} = error, protocol} ->
            handle(mod, :handle_result, [error, mod_state], from, %{s | protocol: protocol})

          {:disconnect, reason, protocol} ->
            reconnect_or_stop(:disconnect, reason, protocol, %{s | state: {mod, mod_state}})
        end

      {:query, _query, mod_state} ->
        stream_in_progress(:query, mod, mod_state, from, s)

      {:disconnect, reason, mod_state} ->
        reconnect_or_stop(:disconnect, reason, s.protocol, %{s | state: {mod, mod_state}})

      {:stop, reason, mod_state} ->
        {:stop, reason, %{s | state: {mod, mod_state}}}
    end
  end

  defp maybe_flush_buffered_message(%__MODULE__{buffering_sock_messages?: true, buffered_sock_message: nil} = s),
    do: %{s | buffering_sock_messages?: false}

  defp maybe_flush_buffered_message(%__MODULE__{buffering_sock_messages?: true} = s) do
    send(self(), s.buffered_sock_message)

    %{s | buffering_sock_messages?: false, buffered_sock_message: nil}
  end

  defp maybe_flush_buffered_message(s), do: s

  defp stream_in_progress(command, mod, mod_state, from, s) do
    Logger.warning("received #{command} while stream is already in progress")
    from && reply(from, {__MODULE__, :stream_in_progress})
    {:keep_state, %{s | state: {mod, mod_state}}}
  end

  defp reconnect_or_stop(error, reason, protocol, s) when error in [:error, :disconnect] do
    %__MODULE__{state: {mod, mod_state}} = s

    # Exception is unused
    Protocol.disconnect(%RuntimeError{}, protocol)

    maybe_handle(mod, :handle_disconnect, [error, reason, mod_state], %{
      s
      | protocol: nil,
        streaming: nil,
        buffered_sock_message: nil
    })
  end

  # defp reconnect_or_stop(error, reason, _protocol, %{auto_reconnect: true} = s) when error in [:error, :disconnect] do
  #   %{state: {mod, mod_state}} = s

  #   Logger.error(
  #     "#{inspect(pid_or_name())} (#{inspect(mod)}) is reconnecting due to reason: #{Exception.format(:error, reason)}"
  #   )

  #   {:keep_state, s} = maybe_handle(mod, :handle_disconnect, [mod_state], s)
  #   {:keep_state, %{s | streaming: nil}, {:next_event, :internal, {:connect, :reconnect}}}
  # end

  # defp pid_or_name do
  #   case Process.info(self(), :registered_name) do
  #     {:registered_name, atom} when is_atom(atom) -> atom
  #     _ -> self()
  #   end
  # end

  defp opts, do: Process.get(__MODULE__)
  defp put_opts(opts), do: Process.put(__MODULE__, opts)
end
