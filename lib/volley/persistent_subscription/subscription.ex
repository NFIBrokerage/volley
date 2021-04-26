defmodule Volley.PersistentSubscription.Subscription do
  @moduledoc false

  defstruct [:connection, :stream_name, :group_name, :ref, opts: []]

  def from_config(config) do
    struct(__MODULE__, config)
  end

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

  def reconnect(subscription) do
    Process.send_after(self(), {:connect, subscription}, 100)
  end
end
