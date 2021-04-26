defmodule Volley.PersistentSubscription do
  @moduledoc """
  A GenStage producer for persistent subscriptions

  TODO

  ## Configuration

  TODO
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
