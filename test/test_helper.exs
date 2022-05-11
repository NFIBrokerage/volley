:ets.new(:stream_positions, [:set, :public, :named_table])
Application.ensure_started(:telemetry)

{:ok, _pid} =
  [Volley.SpearClient] |> Supervisor.start_link(strategy: :one_for_one)

ExUnit.configure(assert_receive_timeout: 1_000)
ExUnit.start()
