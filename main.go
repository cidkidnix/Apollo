package main

import (
  "Apollo/src/CliParser"
  "Apollo/src/Apollo"
  "Apollo/src/Config"
)

type ApolloRun struct {
    ConfigPath string `cli:"config" description:"Config to load" default_value:"config.toml"`
}

func main() {
  subCommandRun := CLI.SubCommand[ApolloRun]{
    Name: "run",
    Flags: ApolloRun{},
  }

  if run, unset := subCommandRun.ParseSubCommand(); !unset {
    Apollo.WatchTransactionTrees(Config.GetConfig(run.ConfigPath))
  }
}
