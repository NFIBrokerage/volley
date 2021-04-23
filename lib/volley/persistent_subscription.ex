defmodule Volley.PersistentSubscription do
  @moduledoc """
  A GenStage producer for persistent subscriptions

  TODO

  ## Configuration

  TODO
  """

  use GenStage
  import Volley

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

    {:producer, Map.new(opts), producer_opts}
  end

  @impl GenStage
  def handle_info({:eos, _reason}, state) do
    {:noreply, [], Map.delete(state, :subscription)}
  end

  def handle_info(%Spear.Event{} = event, state) do
    {:noreply, [map_event(event, state)], state}
  end

  @impl GenStage
  def handle_demand(_demand, state) do
    with nil <- state[:subscription],
         {:ok, subscription} <- subscribe(state) do
      {:noreply, [], Map.put(state, :subscription, subscription)}
    else
      _ -> {:noreply, [], state}
    end
  end

  defp subscribe(state) do
    Spear.connect_to_persistent_subscription(
      state.connection,
      self(),
      state.stream_name,
      state.group_name,
      state[:subscription_opts] || []
    )
  end

  if_broadway do
    defp map_event(event, %{broadway?: true} = state) do
      %Broadway.Message{
        data: event,
        acknowledger:
          {Volley.PersistentSubscription.Acknowledger,
           {state.connection, state.subscription}, %{}}
      }
    end
  end

  # coveralls-ignore-start
  defp map_event(event, _state), do: event

  # coveralls-ignore-stop
end
