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
    LedgerConnection LedgerConnection `toml:"ledger"`
    DatabaseSettings Database.DatabaseConnection `toml:"database"`
  }

  LedgerConnection struct {
    Host string
    Port uint64
  }

  User struct {
    Name string
    AuthToken string
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
