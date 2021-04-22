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
  use Broadway

  def start_link(opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module: {Volley.LinearSubscription, opts},
        transformer: {__MODULE__, :transform, []}
      ],
      processors: [
        default: [concurrency: 1]
      ]
    )
  end

  @impl Broadway
  def handle_message(:default, %Message{} = message, _context) do
    position = stream_revision(message)
    IO.inspect(position, label: "handling no.")
    if position == 42 do
      raise "nooo!"
    end
    message
  end

  def transform(event, _opts) do
    %Message{
      data: event,
      acknowledger: {__MODULE__, :default, []}
    }
  end

  def ack(:default, successful, _failed) do
    messages = Enum.map(successful, &stream_revision/1)
    in_order? = Enum.sort(messages) == messages
    messages = Enum.map(messages, &to_string/1) |> Enum.join(",")
    IO.inspect({in_order?, messages}, label: "{in order?, positions}")
  end

  def stream_revision(message), do: message.data.metadata.stream_revision
end

settings = %Spear.PersistentSubscription.Settings{message_timeout: 20_000}

Volley.Client.start_link(client_settings)

reset_psub = fn ->
  Volley.Client.delete_persistent_subscription(stream, group)
  Volley.Client.create_persistent_subscription(stream, group, settings)
end
