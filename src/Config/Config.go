package Config

import (
  "github.com/BurntSushi/toml"
  "Apollo/src/Database"
  "os"
  "fmt"
  "github.com/rs/zerolog"
  "reflect"
  "strings"
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
    TLS *TLS `toml:"tls"`
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

  TLS struct {
    CertFile string `toml:"cert_file"`
    ServerNameOverride string `toml:"server_name_override"`
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

var envPrefix string = "$APOLLO_ENV:"

// Recursively look through a type and replace any strings with an associated environment variable
func ReplaceWithEnv[T any](internal T) T {
  v := reflect.ValueOf(internal)
  typeCopy := reflect.New(v.Type()).Elem()
  typeCopy.Set(v)

  for i := 0; i < v.NumField(); i++ {
    if v.Field(i).CanInterface() {
      field := v.Field(i).Interface()
      replaceVal := v.Field(i)
      switch reflect.TypeOf(field).Kind() {
        case reflect.Struct:
          replaceVal = reflect.ValueOf(ReplaceWithEnv(field))
        case reflect.String:
          valueO := field.(string)
          if strings.HasPrefix(valueO, envPrefix) {
            environmentName := valueO[len(envPrefix):]
            if value, ok := os.LookupEnv(environmentName); ok {
              replaceVal = reflect.ValueOf(value)
            } else {
              fmt.Printf("Failed to get environment variable %s\n", environmentName)
              os.Exit(1)
            }
          }
        case reflect.Ptr:
          if !v.Field(i).IsNil() {
            innerVal := v.Field(i).Elem()
            if innerVal.IsValid() && !innerVal.IsZero() {
              a := ReplaceWithEnv(innerVal.Interface())
              newVal := reflect.New(innerVal.Type())
              newVal.Elem().Set(reflect.ValueOf(a))
              replaceVal = newVal
            }
          }
        default:
            continue

      }

      typeCopy.Field(i).Set(replaceVal)
    }
  }

  return typeCopy.Interface().(T)
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
    TLS: nil,
    GC: GC{
      ManualGCRun: false,
      ManualGCPause: false,
      ManualGCRunAmount: 1000,
      MemoryLimit: 2048,
    },
  }
  _, err := toml.DecodeFile(configPath, &config)
  if err != nil { panic(err) }

  config = ReplaceWithEnv(config)
  return config
}
