defmodule Volley.GenStagePersistentHandler do
  @moduledoc """
  A test fixture for consuming persistent subscriptions as a GenStage instead
  of a Broadway topology
  """

  use GenStage

  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts)
  end

  @impl GenStage
  def init(opts) do
    producer = Keyword.fetch!(opts, :producer)

    {:consumer, Map.new(opts), subscribe_to: [producer]}
  end

  @impl GenStage
  def handle_events(
        [%Spear.Event{metadata: %{subscription: subscription}} | _] = events,
        _from,
        state
      ) do
    send(state.test_proc, events)

    Spear.ack(
      subscription.connection,
      subscription.ref,
      events |> Enum.map(& &1.id)
    )

    {:noreply, [], state}
  end
end
