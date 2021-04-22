defmodule Volley.LinearSubscription do
  @moduledoc """
  A subscription which guarantees ordering

  A linear subscription consumes an EventStoreDB stream in order roughly
  similar to a subscription started via `Spear.subscribe/4`. Linear
  subscriptions are simpler than persistent subscriptions and can be used
  in cases where unordered processing is too cumbersome. Linear subscriptions
  are subject to head-of-line blocking as each event must be handled
  synchronously so as to keep ordering.

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
