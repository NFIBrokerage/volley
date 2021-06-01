import Config

host = System.get_env("EVENTSTORE_HOST") || "localhost"

config :volley, Volley.SpearClient,
  connection_string: "esdb://admin:changeit@#{host}:2113"
