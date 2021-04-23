defmodule Volley do
  @moduledoc """
  GenStage and Broadway producers for EventStoreDB

  TODO
  """

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
end
