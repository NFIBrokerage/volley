defmodule Volley.MixProject do
  use Mix.Project

  @source_url "https://github.com/NFIBrokerage/volley"
  @version_file Path.join(__DIR__, ".version")
  @external_resource @version_file
  @version (case Regex.run(~r/^v([\d\.\w-]+)/, File.read!(@version_file),
                   capture: :all_but_first
                 ) do
              [version] -> version
              nil -> "0.0.0"
            end)

  def project do
    [
      app: :volley,
      version: @version,
      elixir: "~> 1.6",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      preferred_cli_env: [
        credo: :test,
        coveralls: :test,
        "coveralls.html": :test,
        bless: :test,
        test: :test
      ],
      test_coverage: [tool: ExCoveralls],
      package: package(),
      description: description(),
      source_url: @source_url,
      name: "Volley",
      docs: docs()
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/fixtures", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    []
  end

  defp deps do
    [
      # docs
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false},
      # test
      {:bless, "~> 1.0", only: [:dev, :test]},
      {:credo, "~> 1.0", only: [:dev, :test]},
      {:excoveralls, "~> 0.7", only: :test}
    ]
  end

  defp package do
    [
      name: "volley",
      files: ~w(lib .formatter.exs mix.exs README.md .version),
      licenses: [],
      links: %{
        "GitHub" => @source_url,
        "CHANGELOG" => @source_url <> "/blobs/main/CHANGELOG.md"
      }
    ]
  end

  defp description do
    "GenStage producers for EventStoreDB 20+ with Spear"
  end

  defp docs do
    [
      deps: [],
      extras: ~w[
        CHANGELOG.md
      ],
      groups_for_extras: [
        Guides: Path.wildcard("guides/*.md")
      ],
      skip_undefined_reference_warnings_on: [
        "CHANGELOG.md"
      ]
    ]
  end
end
