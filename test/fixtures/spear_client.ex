defmodule Volley.SpearClient do
  @moduledoc """
  A `Spear.Client` for connecting to an EventStoreDB for tests
  """

  use Spear.Client, otp_app: :volley
end
