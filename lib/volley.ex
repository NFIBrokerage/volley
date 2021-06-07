defmodule Volley do
  @moduledoc """
  GenStage and Broadway producers for EventStoreDB

  Volley provides a GenStage producer `Volley.InOrderSubscription` and a
  GenStage/Broadway producer `Volley.PersistentSubscription`. Both of these
  subscription producers can read a stream from beginning to end and then
  keep up-to-date as new events are published to the EventStoreDB.

  These producers can be used to build a reactive, event-driven, eventually
  consistent system suitable for Event Sourcing. In terms of Event Sourcing,
  these producers can be used to build process managers, sagas, and read
  models.

  ## InOrder vs. persistent subscriptions

  The `Volley.InOrderSubscription` producer is a simpler subscription
  model which uses `Spear.read_stream/3` and `Spear.subscribe/4` to read an
  EventStoreDB stream in order. `Volley.InOrderSubscription` is a client-side
  subscription: the client is responsible for storing its stream revision.

  `Volley.PersistentSubscription`s use the Persistent Subscription feature
  of EventStoreDB to store stream revisions and perform back-pressure on the
  EventStoreDB server-side. Persistent subscriptions do not have strict
  ordering guarantees, which allows features like competing consumers,
  batch processing, and message-parking (with a built-in dead letter approach).
  See the EventStoreDB documentation on persistent subscriptions and
  `Spear.connect_to_persistent_subscription/5` for more information.

  InOrder subscriptions have less resource impact on the EventStoreDB but are
  less flexible than persistent subscriptions. InOrder subscriptions are subject
  to head-of-line blocking: failing to process an event must halt the
  subscription in order to keep ordering. Persistent subscriptions offer more
  complex subscription strategies and can avoid head-of-line blocking but
  handlers may be more complex or difficult to write as they need to account
  for events potentially arriving out of order.

  Systems do not need to be limited to only one kind of event listener: a mix
  of in-order and persistent subscriptions may be wise.
  """

  @genserver_option_keys ~w[debug name timeout spawn_opt hibernate_after]a
  @producer_option_keys ~w[buffer_size buffer_keep dispatcher demand]a

  # coveralls-ignore-start
  @doc false
  defmacro if_broadway(do: body) do
    case Code.ensure_compiled(Broadway) do
      {:module, Broadway} ->
        body

      _ ->
        quote(do: :ok)
    end
  end

  # coveralls-ignore-stop

  @doc false
  def pop_genserver_opts(opts) do
    {Keyword.take(opts, @genserver_option_keys),
     Keyword.drop(opts, @genserver_option_keys -- [:name])}
  end

  @doc false
  def pop_producer_opts(opts) do
    {Keyword.take(opts, @producer_option_keys),
     Keyword.drop(opts, @producer_option_keys)}
  end
end
