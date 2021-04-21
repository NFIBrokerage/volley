defmodule Volley.Handler do
  @moduledoc "TODO remove me or make me a fixture"

  use Broadway

  alias Broadway.Message

  def start_link(_opts) do
    producer_opts = Application.fetch_env!(:volley, __MODULE__)

    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module: {Volley.PersistentSubscription, producer_opts}
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

defmodule Volley.Client do
  @moduledoc "TODO remove me or make me a fixture"
  use Spear.Client, otp_app: :volley
end
