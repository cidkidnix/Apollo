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
    BatchSize BatchSizes `toml:"batch_size"`
    Filter Filter `toml:"filters"`
    TxMaxPull int `toml:"tx_max_pull"`
  }

  LedgerConnection struct {
    Host string `toml:"host"`
    Port uint64 `toml:"port"`
  }

  User struct {
    AuthToken string `toml:"auth_token"`
    ApplicationId string `toml:"application_id"`
    Timeout int `toml:"timeout"`
  }

  Filter struct {
      Template TemplateFilter `toml:"template_filter"`
  }

  TemplateFilter struct {
    IgnoreModules []string `toml:"ignore_modules"`
    IgnorePackages []string `toml:"ignore_package"`
    IgnoreEntities []string `toml:"ignore_entity"`
  }

  Oauth struct {
    Url string `toml:"url"`
    ClientSecret string `toml:"client_secret"`
    ClientId string `toml:"client_id"`
    GrantType string `toml:"grant_type"`
    ExtraParams map[string]string `toml:"extra_params"`
  }

  BatchSizes struct {
    Creates int `toml"create"`
    Exercises int `toml"exercise"`
    Transactions int `toml"transaction"`
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
    BatchSize: BatchSizes {
      Creates: -1,
      Exercises: 200,
      Transactions: 200,
    },
    Oauth: nil,
    TxMaxPull: 2,
    DatabaseSettings: Database.DatabaseConnection{
      MinConnections: 4,
      MaxConnections: 16,
    },
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
