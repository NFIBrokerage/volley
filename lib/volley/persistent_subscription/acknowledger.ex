import Volley

if_broadway do
  defmodule Volley.PersistentSubscription.Acknowledger do
    @moduledoc """
    A Broadway acknowledger for persistent subscription messages

    Makes use of the batch capabilities of `Spear.ack/3` and `Spear.nack/4`
    to efficiently acknowledge messages.

    This module is only availble if the `:broadway` dependency is available.
    """

    @behaviour Broadway.Acknowledger

    @impl Broadway.Acknowledger
    def ack(subscription, successful_messages, failed_messages) do
      successful_ids = Enum.map(successful_messages, & &1.data.id)
      failed_ids = Enum.map(failed_messages, & &1.data.id)

      Spear.ack(subscription.connection, subscription.ref, successful_ids)

      Spear.nack(subscription.connection, subscription.ref, failed_ids,
        action: :retry
      )
    end
  end
end
