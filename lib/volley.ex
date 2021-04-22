defmodule Volley do
  @moduledoc false

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
