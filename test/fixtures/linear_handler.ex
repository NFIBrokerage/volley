defmodule Volley.LinearHandler do
  @moduledoc """
  A test fixture for a GenStage handler for linear subscriptions
  """

  use GenStage

  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  @impl GenStage
  def init(opts) do
    producer = Keyword.fetch!(opts, :producer)

    producer |> GenServer.whereis() |> Process.link()

    {:consumer, Map.new(opts), subscribe_to: [{producer, max_demand: 1}]}
  end

  @impl GenStage
  def handle_events([%Spear.Event{body: %{"poison?" => true}}], _from, _state) do
    raise "poison event detected!"
  end

  def handle_events([event], _from, state) do
    send(state.test_proc, event)

    :ets.insert(:stream_positions, {state.id, event})

    {:noreply, [], state}
  end

  def fetch_stream_position!(id) do
    case :ets.lookup(:stream_positions, id) do
      [{^id, position}] -> position
      [] -> :start
    end
  end
end
