package CLI

import (
  "flag"
  "os"
  "reflect"
  "fmt"
  "strconv"
)


type SubCommand[S any] struct {
    Name string
    Flags S
}

func (subCommand *SubCommand[S]) ParseSubCommand() (S, bool) {
  type internalCmd struct {
    name string
    sType reflect.Type
    tag string
    unFilteredTag reflect.StructTag
  }
  v := reflect.ValueOf(subCommand.Flags)
  typ := v.Type()

  values := make(map[string]internalCmd, v.NumField())

  for i := 0; i < v.NumField(); i++ {
    if v.Field(i).CanInterface() {
      values[typ.Field(i).Name] = internalCmd {
          name: typ.Field(i).Name,
          sType: v.Field(i).Type(),
          tag: typ.Field(i).Tag.Get("cli"),
          unFilteredTag: typ.Field(i).Tag,
      }
    }
  }

  var structFields []reflect.StructField
  collect := make(map[string]any)
  gFlag := flag.NewFlagSet(subCommand.Name, flag.ExitOnError)
  for k, v := range(values) {
    switch v.sType.Kind() {
      case reflect.String:
        flagPtr := gFlag.String(v.tag, v.unFilteredTag.Get("default_value"), v.unFilteredTag.Get("description"))
        collect[v.name] = flagPtr
        structFields = append(structFields, reflect.StructField{
          Name: k,
          Type: v.sType,
          Tag: reflect.StructTag(v.unFilteredTag),
        })
      case reflect.Int64:
        var defaultVal int64
        defaultVal, err := strconv.ParseInt(v.unFilteredTag.Get("default_value"), 10, 64)
        if err != nil {
          defaultVal = 0
        }
        flagPtr := gFlag.Int64(v.tag, defaultVal, v.unFilteredTag.Get("description"))
        collect[v.name] = flagPtr
        structFields = append(structFields, reflect.StructField{
          Name: k,
          Type: v.sType,
          Tag: reflect.StructTag(v.unFilteredTag),
        })
      case reflect.Bool:
        flagPtr := gFlag.Bool(v.tag, false, v.unFilteredTag.Get("description"))
        collect[v.name] = flagPtr
        structFields = append(structFields, reflect.StructField{
          Name: k,
          Type: v.sType,
          Tag: reflect.StructTag(v.unFilteredTag),
        })


      default:
        panic(v.sType)
    }
  }

  t := reflect.New(reflect.TypeOf(subCommand.Flags)).Elem()

  if len(os.Args) < 2 {
    fmt.Println("Missing subcommand!")
    os.Exit(1)
  }

  switch os.Args[1] {
    case subCommand.Name:
      gFlag.Parse(os.Args[2:])
      for k, v := range(collect) {
        t.FieldByName(k).Set(reflect.ValueOf(v).Elem())
      }

      return t.Interface().(S), false
    default:
      return t.Interface().(S), true
  }

}
