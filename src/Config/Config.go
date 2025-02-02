package Config

import (
  "github.com/BurntSushi/toml"
  "Apollo/src/Database"
  "os"
  "fmt"
  "github.com/rs/zerolog"

)


type (
  ApolloConfig struct {
    LedgerOffset string `toml:"offset"`
    User *User `toml:"user"`
    Oauth *Oauth `toml:"oauth"`
    Sandbox bool `toml:"sandbox"`
    LedgerConnection LedgerConnection `toml:"ledger"`
    DatabaseSettings Database.DatabaseConnection `toml:"database"`
    GC GC `toml:"gc"`
    LogLevel string `toml:"log_level"`
    BatchSize int `toml:"batch_size"`
  }

  LedgerConnection struct {
    Host string `toml:"host"`
    Port uint64 `toml:"port"`
  }

  User struct {
    AuthToken string `toml:"auth_token"`
    ApplicationId string `toml:"application_id"`
  }

  Oauth struct {
    Url string `toml:"url"`
    ClientSecret string `toml:"client_secret"`
    ClientId string `toml:"client_id"`
    GrantType string `toml:"grant_type"`
    ExtraParams map[string]string `toml:"extra_params"`
  }

  GC struct {
    ManualGCPause bool `toml:"allow_manual_pause"`
    ManualGCRun bool `toml:"allow_manual_run"`
    ManualGCRunAmount int `toml:"manual_offset_max"`
    MemoryLimit int64 `toml:"memory_limit"`
  }
)

func ParseLogLevel(level string) zerolog.Level {
  switch level {
    case "INFO":
        return zerolog.InfoLevel
    case "DEBUG":
        return zerolog.DebugLevel
    case "TRACE":
        return zerolog.TraceLevel
    default:
        return zerolog.InfoLevel
  }
}

func GetConfig(configPath string) ApolloConfig {
  if _, err := os.Stat(configPath); err != nil {
    fmt.Printf("File doesn't exist!")
    panic(err)
  }

  config := ApolloConfig{
    LogLevel: "Info",
    BatchSize: 300,
    Oauth: nil,
    GC: GC{
      ManualGCRun: false,
      ManualGCPause: false,
      ManualGCRunAmount: 1000,
      MemoryLimit: 2048,
    },
  }
  _, err := toml.DecodeFile(configPath, &config)
  if err != nil { panic(err) }

  return config
}
