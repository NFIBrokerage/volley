alias Broadway.Message

client_settings = [
  connection_string: "esdb://admin:changeit@localhost:2113?tls=true",
  mint_opts: [
    transport_opts: [
      cacertfile: Path.join([__DIR__ | ~w[certs ca ca.crt]])
    ]
  ]
]

defmodule Volley.Client do
  use Spear.Client
end

stream = "volley_test"
group = "volley_iex"

psub_settings = [
  connection: Volley.Client,
  stream_name: stream,
  group_name: group,
  broadway?: true,
  subscription_opts: [
    buffer_size: 10
  ]
]

linear_settings = [
  connection: Volley.Client,
  stream_name: stream,
  read_opts: [
  ]
]

defmodule Volley.PsubHandler do
  use Broadway

  def start_link(opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module: {Volley.PersistentSubscription, opts}
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
  def handle_batch(:default, messages, _batch_info, _context) do
    messages
    |> Enum.map(& &1.data.metadata.stream_revision)
    |> IO.inspect(label: "batching event nos.")

    messages
  end
end

defmodule Volley.LinearHandler do
  use GenStage

  def start_link() do
    GenStage.start_link(__MODULE__, :ok)
  end

  @impl GenStage
  def init(:ok) do
    {:consumer, :ok, subscribe_to: [{Foo, max_demand: 1}]}
  end

  @impl GenStage
  def handle_events([event], _from, state) do
    event |> stream_revision |> IO.inspect(label: "handling event no.")

    if stream_revision(event) == 42 do
      raise "aaaaah!"
    end

    {:noreply, [], state}
  end

  def stream_revision(event), do: event.metadata.stream_revision
end

settings = %Spear.PersistentSubscription.Settings{message_timeout: 20_000}

Volley.Client.start_link(client_settings)

reset_psub = fn ->
  Volley.Client.delete_persistent_subscription(stream, group)
  Volley.Client.create_persistent_subscription(stream, group, settings)
end
