:ets.new(:stream_positions, [:set, :public, :named_table])

[Volley.SpearClient] |> Supervisor.start_link(strategy: :one_for_one)

ExUnit.configure(assert_receive_timeout: 1_000)
ExUnit.start()
