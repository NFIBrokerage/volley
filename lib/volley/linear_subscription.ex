defmodule Volley.LinearSubscription do
  @moduledoc """
  A subscription which guarantees ordering

  A linear subscription consumes an EventStoreDB stream in order roughly
  similar to a subscription started via `Spear.subscribe/4`. Linear
  subscriptions are simpler than persistent subscriptions and can be used
  in cases where unordered processing is too cumbersome.

  ## Backpressure

  This producer primarily makes use of `GenStage`'s buffering capabilities to
  provide backpressure.

  A plain subscription through `Spear.subscribe/4` has no backpressure
  between EventStoreDB and the `Spear.Connection`, so the subscriber
  process cannot exert any backpressure on the connection. For very
  large streams, a subscriber process may become overwhelmed as the
  process mailbox fills up with events as fast as they can be read from
  the EventStoreDB.

  This producer has two modes:

  - subscription mode, where the stream is subscribed with `Spear.subscribe/4`
    and events are emitted as soon as available
  - reading mode, in which events are emitted on-demand of the consumer

  This producer starts up in reading mode and emits events on-demand as long
  as there are more events to be read. Once the producer reaches the current
  end of the stream, it subscribes using `Spear.subscribe/4` in the switch
  to subscription mode.

  Once the producer has caught up to the end of the stream, it will only
  receive newly appended events, and so may be less likely to become
  overwhelmed. Sustained bursts in appends to the stream may eventually
  overfill the `GenStage` buffer, though.

  ## Writing handlers for linear subscriptions

  Special care must be taken when writing a consumer for linear subscriptions.
  Consumers must implement head-of-line blocking in order to keep
  correct ordering of events.

  In general, a consumer must meet these four requirements

  - only one consumer may subscribe to each producer
  - the consumer must `Process.link/1` itself to the producer process
      - the `c:GenStage.init/1` callback is a suitable place to perform this
        linking
  - the consumer must only ever exert a demand of `0` or `1`
  - the consumer must curate its stream position

  Let's build a basic event handler for a linear subscription with the
  `GenStage` basics

  ```elixir
  defmodule MyHandler do
    use GenStage

    def start_link(_) do
      GenStage.start_link(__MODULE__, :ok)
    end

    @impl GenStage
    def init(:ok) do
      {:consumer, :ok, subscribe_to: [MyProducer]}
    end

    @impl GenStage
    def handle_events(events, _from, state) do
      IO.inspect(events, label: "events")

      {:noreply, [], state}
    end
  end
  ```

  This is a very minimal consumer that starts up, subscribes to `MyProducer`,
  and handles events by outputting them with `IO.inspect/2`. We can start
  up this handler and the producer like so:

  ```elixir
  linear_subscription_settings = [
    name: MyProducer,
    connection: MySpearClient,
    stream_name: "some_stream",
    ..
  ]

  [
    {Volley.LinearSubscription, linear_subscription_settings},
    MyHandler
  ]
  |> Supervisor.start_link(strategy: :one_for_one)
  ```

  This consumer doesn't follow our rules though. If we start it up, we'll see
  that we're handling multiple events at once, and if we restart it we'll see
  it start from the beginning of the stream. Let's restrict the number of
  events it can process at once by tuning the `:max_demand` down to `1`:

  ```elixir
  @impl GenStage
  def init(:ok) do
    {:consumer, :ok, subscribe_to: [{MyProducer, max_demand: 1}]}
  end
  ```

  Now we're handling events one-by-one in our handler so we can match on a
  single event in `c:GenStage.handle_events/3`

  ```elixir
  def handle_events([event], _from, state) do
    IO.inspect(event.metadata.stream_revision, label: "handling event no.")
    ..
  ```

  Now if we start up our handler, we'll see it churning through each event
  in order. Let's introduce a bit of failure into the handler though.
  Defensive programming is typically discouraged in OTP applications, so let's
  do a `raise/1` in our handling code to simulate a situation like a failing
  bang (`!`) function or bad match:

  ```elixir
  def handle_events([event], _from, state) do
    revision = event.metadata.stream_revision

    IO.inspect(revision, label: "handling event no.")

    if revision == 42 do
      raise "aaaaah!"
    end

    {:noreply, [], state}
  end
  ```

  Now if we run our pipeline on a stream longer than 42 events, the handler
  will crash. Since producers and consumers are not linked by default in
  `GenStage`, the exit of the consumer will leave the producer running.
  This means that we will see output like

  ```
  handling event no.: 41
  handling event no.: 42
  21:03:07.107 [error] GenServer #PID<0.266.0> terminating
  ** (RuntimeError) aaaaah!
  handling event no.: 43
  ```

  The default restart strategy of our consumer will start a new process which
  will subscribe to the producer and handle the next event. This means that
  event 42 is effectively skipped which breaks ordered processing. To remedy
  this we need to notify the producer that the processing for an event has
  failed by linking together the producer and consumer.

  ```elixir
  @impl GenStage
  def init(:ok) do
    MyProducer |> GenServer.whereis() |> Process.link()

    {:consumer, :ok, subscribe_to: [{MyProducer, max_demand: 1}]}
  end
  ```

  Now when the handler exits on event 42, it will also exit the producer.
  With the producer and consumer that we have so far, this will result in
  the consumer restarting processing from the beginning of the stream.
  Again, this breaks ordering. We need the consumer to curate its position
  in the stream in order to keep a consistent order.

  For this example, we'll use `:ets` to hold the stream position of our handler
  in memory. This is useful and easy to set up for an example, but `:ets`
  is an in-memory cache which will clear out when the service stops. Production
  storage for stream positions should be more persistent: e.g. a PostgreSQL
  row or an mnesia record.

  Outside of our supervision tree for the producer and consumer we'll create
  an `:ets` table:

  ```elixir
  :ets.new(:stream_positions, [:set, :public, :named_table])
  ```

  And now add a function to our handler so the producer can restore a stream
  position from this table

  ```elixir
  def fetch_stream_position! do
    case :ets.lookup(:stream_positions, __MODULE__) do
      [{__MODULE__, position}] -> position
      [] -> :start
    end
  end
  ```

  And add that MFA to the producer's options:

  ```elixir
  linear_subscription_settings = [
    name: MyProducer,
    connection: MySpearClient,
    stream_name: "some_stream",
    restore_stream_position!: {MyHandler, :fetch_stream_position!, []}
  ]
  ```

  Now the producer will fetch the current stream position on start-up, so
  even if the processes crash and need to be restarted, the handler will
  keep a consistent position in the subscription.

  Finally we'll store the stream position in the consumer. This should
  only occur after the consumer has done any side-effects or processing
  prone to failure. Ideally, the stream position should be persisted in a
  transaction with any side-effects.

  ```elixir
  @impl GenStage
  def handle_events([event], _from, state) do
    revision = event.metadata.stream_revision

    IO.inspect(revision, label: "handling event no.")

    if revision == 42 do
      raise "aaaaah!"
    end

    :ets.insert(:stream_positions, {__MODULE__, revision})

    {:noreply, [], state}
  end
  ```

  With this final change our consumer will read each event in the stream
  in order, reach event 42, raise, retry event 42, raise, and then the
  supervisor process will shut down. This is the essence of head-of-line
  blocking: once the pipeline reaches an event which it cannot process,
  the entire pipeline is halted. This is generally an undesirable
  behavior: a code-wise or manual change is usually needed to resolve the
  blockage. Persistent subscriptions (see `Volley.PersistentSubscription`)
  offer much more flexibility around ordering, batching, and concurrency
  thanks to the asynchronous ack and nack workflow and the EventStoreDB's
  parking system.

  Altogether our handler looks like this:

  ```elixir
  defmodule MyHandler do
    use GenStage

    def start_link(_) do
      GenStage.start_link(__MODULE__, :ok)
    end

    @impl GenStage
    def init(:ok) do
      MyProducer |> GenServer.whereis() |> Process.link()

      {:consumer, :ok, subscribe_to: [{MyProducer, max_demand: 1}]}
    end

    @impl GenStage
    def handle_events([event], _from, state) do
      revision = event.metadata.stream_revision

      IO.inspect(revision, label: "handling event no.")

      if revision == 42 do
        raise "aaaaah!"
      end

      :ets.insert(:stream_positions, {__MODULE__, revision})

      {:noreply, [], state}
    end

    def fetch_stream_position! do
      case :ets.lookup(:stream_positions, __MODULE__) do
        [{__MODULE__, position}] -> position
        [] -> :start
      end
    end
  end
  ```

  ## Configuration

  TODO
  """

  @default_read_size 100
  @genserver_option_keys ~w[debug name timeout spawn_opt hibernate_after]a
  @producer_option_keys ~w[buffer_size buffer_keep dispatcher demand]a

  use GenStage

  def start_link(opts) do
    {start_link_opts, opts} = pop_genserver_opts(opts)

    GenStage.start_link(__MODULE__, opts, start_link_opts)
  end

  @impl GenStage
  def init(opts) do
    {producer_opts, opts} = pop_producer_opts(opts)

    {:producer, Map.new(opts), producer_opts}
  end

  @impl GenStage
  def handle_demand(demand, state) do
    case read_stream(state, demand) do
      {:ok, events} ->
        {:noreply, events, save_position(state, events)}

      {:done, events} ->
        GenStage.async_info(self(), :subscribe)

        {:noreply, events, save_position(state, events)}

      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  @impl GenStage
  def handle_info(:subscribe, state) do
    case subscribe(state) do
      {:ok, sub} ->
        {:noreply, [], Map.put(state, :subscription, sub)}

      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  def handle_info(%Spear.Event{} = event, state) do
    {:noreply, [event], save_position(state, event)}
  end

  def handle_info(%Spear.Filter.Checkpoint{}, state) do
    {:noreply, [], state}
  end

  def handle_info({:eos, reason}, state) do
    {:shutdown, reason, state}
  end

  defp read_stream(state, demand) do
    read_size = Keyword.get(state[:read_opts] || [], :max_count, @default_read_size)
    read_size = max(demand, read_size)
    position = position(state)

    opts =
      Map.get(state, :read_opts, [])
      |> Keyword.merge(
        from: position,
        max_count: read_size + 1
      )

    drop_count = if position == :start, do: 0, else: 1

    with {:ok, events} <- Spear.read_stream(state.connection, state.stream_name, opts),
         events when length(events) < read_size <- events |> Stream.drop(drop_count) |> Enum.to_list() do
      {:done, events}
    else
      events when is_list(events) -> {:ok, events}
      error -> error
    end
  end

  defp subscribe(state) do
    opts =
      Map.get(state, :read_opts, [])
      |> Keyword.merge(from: state.position)

    Spear.subscribe(state.connection, self(), state.stream_name, opts)
  end

  defp position(%{position: position}), do: position
  defp position(%{restore_stream_position!: {m, f, a}}) do
    apply(m, f, a)
  end

  defp save_position(state, []), do: state

  defp save_position(state, events) when is_list(events) do
    save_position(state, List.last(events))
  end

  defp save_position(state, event) do
    Map.put(state, :position, event)
  end

  defp pop_genserver_opts(opts) do
    {Keyword.take(opts, @genserver_option_keys), Keyword.drop(opts, @genserver_option_keys)}
  end

  defp pop_producer_opts(opts) do
    {Keyword.take(opts, @producer_option_keys), Keyword.drop(opts, @producer_option_keys)}
  end
end
