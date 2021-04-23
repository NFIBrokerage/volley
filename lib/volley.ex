defmodule Volley do
  @moduledoc """
  GenStage and Broadway producers for EventStoreDB

  TODO
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
     Keyword.drop(opts, @genserver_option_keys)}
  end

  @doc false
  def pop_producer_opts(opts) do
    {Keyword.take(opts, @producer_option_keys),
     Keyword.drop(opts, @producer_option_keys)}
  end
end
