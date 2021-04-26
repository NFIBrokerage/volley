defmodule Volley.PersistentSubscription.Subscription do
  @moduledoc """
  A structure which stores information about a subscription
  """

  @typedoc """
  A subscription structure which stores connection details for any
  persistent subscription

  Acks and nacks may be sent with the `:connection` and `:ref` fields.

  ```elixir
  Spear.ack(subscription.connection, :subscription.ref, event_ids)
  Spear.nack(subscription.connection, :subscription.ref, event_ids, action: :retry)
  ```

  If the `Volley.PersistentSubscription` producer is used in GenStage-mode
  (with `broadway?: false`), each `t:Spear.Event.t/0` will have one of these
  structs in its `.metadata.subscription` field which can be used for acks
  and nacks.
  """
  @type t :: %__MODULE__{
          connection: Spear.Connection.t(),
          stream_name: String.t(),
          group_name: String.t(),
          ref: reference(),
          opts: Keyword.t()
        }

  defstruct [:connection, :stream_name, :group_name, :ref, opts: []]

  @doc false
  def from_config(config) do
    struct(__MODULE__, config)
  end

  @doc false
  def connect(subscription) do
    Spear.connect_to_persistent_subscription(
      subscription.connection,
      self(),
      subscription.stream_name,
      subscription.group_name,
      subscription.opts
    )
    |> case do
      {:ok, ref} ->
        {:ok, %__MODULE__{subscription | ref: ref}}

      {:error, _reason} ->
        reconnect(subscription)

        :error
    end
  end

  @doc false
  def reconnect(subscription) do
    Process.send_after(self(), {:connect, subscription}, 100)
  end
end
