defmodule Volley.InOrderSubscriptionTest do
  use ExUnit.Case, async: true

  import Spear.Uuid, only: [uuid_v4: 0]

  @client Volley.SpearClient
  @producer Volley.InOrderSubscription
  @consumer Volley.InOrderHandler

  @moduletag :capture_log

  setup do
    [
      stream_name: "Volley.Test-" <> uuid_v4(),
      subscription_name: String.to_atom(uuid_v4()),
      handler_ref: make_ref()
    ]
  end

  describe "given a stream has events" do
    setup :write_events

    setup c do
      [
        subscription_opts: [
          connection: @client,
          stream_name: c.stream_name,
          restore_stream_position!:
            {@consumer, :fetch_stream_position!, [c.handler_ref]},
          name: c.subscription_name,
          read_opts: [
            max_count: 25
          ]
        ],
        handler_opts: [
          test_proc: self(),
          producer: c.subscription_name,
          id: c.handler_ref
        ]
      ]
    end

    test "a linear subscription receives the events in order", c do
      assert {:ok, _pid} = start_supervised({@producer, c.subscription_opts})
      assert {:ok, _pid} = start_supervised({@consumer, c.handler_opts})

      assert collect_events() |> Enum.map(& &1.body) == Enum.to_list(1..50)

      write_events(c.stream_name, 25, 51)

      assert collect_events() |> Enum.map(& &1.body) == Enum.to_list(51..75)
    end

    test "poison events tank the producer and consumer", c do
      assert {:ok, producer} =
               start_supervised({@producer, c.subscription_opts})

      assert {:ok, consumer} = start_supervised({@consumer, c.handler_opts})
      producer_ref = Process.monitor(producer)
      consumer_ref = Process.monitor(consumer)

      Spear.Event.new("poison", %{"poison?" => true})
      |> List.wrap()
      |> @client.append(c.stream_name, expect: :exists)

      assert_receive {:DOWN, ^producer_ref, :process, _object, _reason}
      assert_receive {:DOWN, ^consumer_ref, :process, _object, _reason}
    end
  end

  defp write_events(c) do
    write_events(c.stream_name, 50)
  end

  defp write_events(stream_name, n, offset \\ 1) do
    Stream.iterate(offset, &(&1 + 1))
    |> Stream.map(&Spear.Event.new("volley_test", &1))
    |> Stream.take(n)
    |> @client.append(stream_name)
  end

  defp collect_events(acc \\ []) do
    receive do
      %Spear.Event{} = event -> collect_events([event | acc])
    after
      200 -> :lists.reverse(acc)
    end
  end
end
