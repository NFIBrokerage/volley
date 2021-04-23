import Config

config :volley, Volley.SpearClient,
  connection_string: "esdb://admin:changeit@localhost:2113?tls=true",
  mint_opts: [
    transport_opts: [
      cacertfile: Path.join([__DIR__ | ~w[.. certs ca ca.crt]])
    ]
  ]
