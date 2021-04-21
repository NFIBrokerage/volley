import Config

config :volley, Volley.Client,
  connection_string: "esdb://admin:changeit@localhost:2113?tls=true",
  mint_opts: [
    transport_opts: [
      cacertfile: Path.join([__DIR__ | ~w(.. certs ca ca.crt)])
    ]
  ]

config :volley, Volley.Handler,
  connection: Volley.Client,
  stream_name: "volley_test",
  group_name: "volley_iex",
  broadway?: true,
  subscription_opts: [
    buffer_size: 10
  ]
