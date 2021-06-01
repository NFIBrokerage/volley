import Config

case System.get_env("EVENTSTORE_HOST") do
  nil ->
    config :volley, Volley.SpearClient,
      connection_string: "esdb://admin:changeit@localhost:2113?tls=true",
      mint_opts: [
        transport_opts: [
          cacertfile: Path.join([__DIR__ | ~w[.. certs ca ca.crt]])
        ]
      ]

  host ->
    config :volley, Volley.SpearClient,
      connection_string: "esdb://admin:changeit@#{host}:2113"
end
