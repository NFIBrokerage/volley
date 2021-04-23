defmodule Volley.BroadwayPersistentHandler do
  @moduledoc """
  A broadway-backed testing fixture for a persistent subscriptions handler

  The `Volley.PersistentSubscription` producer is best used with Broadway for
  ease of setting up pipelines with batching and concurrent processing.
  Batch processing is network-wise efficient for persistent subscriptions
  because of the batching behavior of `Spear.ack/3` and `Spear.nack/4`.
  """

  use Broadway

  alias Broadway.Message

  def start_link(opts) do
    Broadway.start_link(__MODULE__,
      name: opts[:name],
      producer: [
        module: {Volley.PersistentSubscription, opts}
      ],
      processors: [
        default: [concurrency: 2]
      ],
      batchers: [
        default: [concurrency: 1, batch_size: 5]
      ],
      context: %{test_proc: opts[:test_proc]}
    )
  end

  @impl Broadway
  def handle_message(:default, %Message{} = message, _context) do
    message
    |> Message.put_batcher(:default)
  end

  @impl Broadway
  def handle_batch(:default, messages, _batch_info, context) do
    send(context.test_proc, messages |> Enum.map(& &1.data))

    messages
  end
end
