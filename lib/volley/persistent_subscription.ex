defmodule Volley.PersistentSubscription do
  @moduledoc """
  A GenStage/Broadway producer for persistent subscriptions

  Persistent subscriptions are a feature of EventStoreDB which offload the
  responsibilities of tracking processed events, back-pressure, and subscriber
  dispatch to the EventStoreDB server. This allows subscribers to more easily
  implement complex subscription strategies, such as allowing multiple
  subscribers across services without gossip, avoiding head-of-line blocking,
  and enabling concurrent and batch processing schemes and rate-limiting.

  Broadway features an acknowledgement interface which integrates well with
  the persistent subscription `Spear.ack/3` and `Spear.nack/4` system. Consumers
  intending to use this producer should prefer writing handlers with
  `Broadway` (over `GenStage`) where possible.

  ## Configuration

  * `:broadway?` - (default: `false`) whether to emit events as
    `t:Broadway.Message.t/0` messages or as `t:Spear.Event.t/0` events.
    `true` should be set for this option if this producer is being used
    in a Broadway topology, `false` for use in a `GenStage` pipeline.
    When set as `true`, this producer will set the ack interface for
    each message to `Volley.PersistentSubscription.Acknowledger` with the
    proper connection details for sending acks and nacks. When `false`, the
    `Spear.Event.metadata.subscription` field will be replaced with a
    `t:Volley.PersistentSubscription.Subscription.t/0` struct with any necessary
    connection details.

  * `:subscriptions` - (default: `[]`) a list of subscription configurations.
    Broadway does not currently allow more than one producer in a topology,
    however one may wish to subscribe to multiple persistent subscriptions,
    potentially across EventStoreDBs. Since back-pressure is controlled
    on the EventStoreDB side, a handler may specify multiple subscriptions
    in a single producer without any special considerations. The schema of
    each subscription is as follows

    * `:connection` - (required) a `t:Spear.Connection.t/0` process which
      can either be specified as a PID or any valid `t:GenServer.name/0`

    * `:stream_name` - (required) the EventStoreDB stream

    * `:group_name` - (required) the EventStoreDB group name

    * `:opts` - (default: `[]`) options to pass to
      `Spear.connect_to_persistent_subscription/5`. The main use of this
      options field is to configure the `:buffer_size` option (default: `1`).
      The `:buffer_size` option controls the number of events allowed to be
      sent to this producer before any events are acknowledged.

  Remaining options are passed to `GenStage.start_link/3` and the
  `{:producer, state, opts}` tuple in `c:GenStage.init/1`.

  ## Examples

  ```elixir
  defmodule MyHandler do
    use Broadway

    alias Broadway.Message

    def start_link(_opts) do
      subscription_opts = [
        broadway?: true,
        subscriptions: [
          [
            stream_name: "MyStream",
            group_name: inspect(__MODULE__),
            connection: MyApp.SpearClient,
            opts: [
              # 10 events allowed in-flight at a time
              buffer_size: 10
            ]
          ]
        ]
      ]

      Broadway.start_link(__MODULE__,
        name: __MODULE__,
        producer: [
          module: {Volley.PersistentSubscription, subscription_opts}
        ],
        processors: [
          default: [concurrency: 2]
        ],
        batchers: [
          default: [concurrency: 1, batch_size: 5]
        ]
      )
    end

    @impl Broadway
    def handle_message(:default, %Message{} = message, _context) do
      message
      |> Message.put_batcher(:default)
    end

    @impl Broadway
    def handle_batch(:default, messages, _batch_info, context) do
      # do something batchy with messages...
    end
  end
  ```
  """

  use GenStage
  import Volley
  alias __MODULE__.Subscription

  defstruct [:config, subscriptions: %{}]

  # coveralls-ignore-start
  @doc false
  def start_link(opts) do
    {start_link_opts, opts} = pop_genserver_opts(opts)

    GenStage.start_link(__MODULE__, opts, start_link_opts)
  end

  # coveralls-ignore-stop

  @impl GenStage
  def init(opts) do
    {producer_opts, opts} = pop_producer_opts(opts)

    subscriptions =
      opts
      |> Keyword.get(:subscriptions, [])
      |> Enum.map(&Subscription.from_config/1)

    Enum.each(subscriptions, fn sub -> send(self(), {:connect, sub}) end)

    config =
      opts
      |> Map.new()
      |> Map.put(:subscriptions, subscriptions)

    {:producer, %__MODULE__{config: config}, producer_opts}
  end

  @impl GenStage
  def handle_info({:connect, subscription}, state) do
    state =
      case Subscription.connect(subscription) do
        {:ok, %Subscription{ref: ref} = subscription} ->
          put_in(state.subscriptions[ref], subscription)

        :error ->
          state
      end

    {:noreply, [], state}
  end

  def handle_info({:eos, subscription, _reason}, state) do
    {%Subscription{} = sub, state} = pop_in(state.subscriptions[subscription])
    Subscription.reconnect(sub)

    {:noreply, [], state}
  end

  def handle_info(%Spear.Event{} = event, state) do
    {:noreply, [map_event(event, state)], state}
  end

  @impl GenStage
  def handle_demand(_demand, state) do
    {:noreply, [], state}
  end

  if_broadway do
    defp map_event(event, %__MODULE__{config: %{broadway?: true}} = state) do
      subscription = state.subscriptions[event.metadata.subscription]

      %Broadway.Message{
        data: event,
        acknowledger:
          {Volley.PersistentSubscription.Acknowledger, subscription, %{}}
      }
    end
  end

  defp map_event(event, state) do
    update_in(event.metadata.subscription, &state.subscriptions[&1])
  end
end
