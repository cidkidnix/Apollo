package Config

import (
  "github.com/BurntSushi/toml"
  "Apollo/src/Database"
  "os"
  "fmt"
)


type (
  ApolloConfig struct {
    LedgerOffset string `toml:"offset"`
    User User `toml:"user"`
    Sandbox bool `toml:"sandbox"`
    LedgerConnection LedgerConnection `toml:"ledger"`
    DatabaseSettings Database.DatabaseConnection `toml:"database"`
  }

  LedgerConnection struct {
    Host string `toml:"host"`
    Port uint64 `toml:"port"`
  }

  User struct {
    AuthToken string `toml:"auth_token"`
    ApplicationId string `toml:"application_id"`
  }
)

func GetConfig(configPath string) ApolloConfig {
  if _, err := os.Stat(configPath); err != nil {
    fmt.Printf("File doesn't exist!")
    panic(err)
  }

  var config ApolloConfig
  _, err := toml.DecodeFile(configPath, &config)
  if err != nil { panic(err) }

  return config
}
