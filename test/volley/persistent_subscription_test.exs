defmodule Volley.BroadwayPersistentSubscriptionTest do
  use ExUnit.Case, async: true

  import Spear.Uuid, only: [uuid_v4: 0]

  @client Volley.SpearClient

  @moduletag :capture_log

  setup do
    stream = "Volley.Test-" <> uuid_v4()
    group = uuid_v4()
    settings = %Spear.PersistentSubscription.Settings{message_timeout: 20_000}

    @client.create_persistent_subscription(stream, group, settings)

    on_exit(fn ->
      @client.delete_persistent_subscription(stream, group)
    end)

    [
      settings: settings,
      stream_name: stream,
      group_name: group,
      subscriptions: [
        [
          connection: @client,
          stream_name: stream,
          group_name: group,
          opts: [
            buffer_size: 10
          ]
        ]
      ]
    ]
  end

  describe "(broadway) given a stream has events" do
    setup :write_events

    setup c do
      [
        opts: [
          subscriptions: c.subscriptions,
          broadway?: true,
          name: String.to_atom(uuid_v4()),
          test_proc: self()
        ]
      ]
    end

    test "a persistent subscription receives the events but possibly not in order",
         c do
      assert {:ok, _pid} =
               start_supervised({Volley.BroadwayPersistentHandler, c.opts})

      assert MapSet.equal?(collect_events(), MapSet.new(1..50))

      write_events(c.stream_name, 25, 51)

      assert MapSet.equal?(collect_events(), MapSet.new(51..75))
    end

    test "when a persistent subscription is deleted, the producer stays alive",
         c do
      assert {:ok, _pid} =
               start_supervised({Volley.BroadwayPersistentHandler, c.opts})

      assert MapSet.equal?(collect_events(), MapSet.new(1..50))

      assert @client.delete_persistent_subscription(c.stream_name, c.group_name)

      write_events(c.stream_name, 25, 51)

      assert MapSet.equal?(collect_events(), MapSet.new([]))
    end
  end

  describe "(gen_stage) given a stream has events" do
    setup :write_events

    setup c do
      [
        opts: [
          subscriptions: c.subscriptions,
          broadway?: false,
          name: String.to_atom(uuid_v4())
        ]
      ]
    end

    test "a persistent subscription receives the events in chunks", c do
      start_supervised!({Volley.PersistentSubscription, c.opts})

      assert {:ok, _pid} =
               start_supervised(
                 {Volley.GenStagePersistentHandler,
                  producer: c.opts[:name], test_proc: self()}
               )

      assert MapSet.equal?(collect_events(), MapSet.new(1..50))
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
      events when is_list(events) -> collect_events([events | acc])
      %Spear.Event{} = event -> collect_events([event | acc])
    after
      1000 -> acc |> List.flatten() |> MapSet.new(& &1.body)
    end
  end
end
