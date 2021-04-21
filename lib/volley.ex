defmodule Volley do
  @moduledoc """
  GenStage producers for EventStoreDB 20+ with Spear
  """

  @doc false
  defmacro if_broadway(do: body) do
    case Code.ensure_compiled(Broadway) do
      {:module, Broadway} ->
        body

      _ ->
        quote(do: :ok)
    end
  end
end
