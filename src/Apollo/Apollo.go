package Apollo

import (
  "Apollo/src/Ledger"
  "Apollo/src/Database"
  "github.com/digital-asset/dazl-client/v7/go/api/com/daml/ledger/api/v1"
  "gorm.io/gorm"
  "fmt"
  "encoding/json"
  "time"
)

func WatchTransactionTrees() {
  db := Database.SetupDatabase(Database.DatabaseConnection {
    Host: "localhost",
    User: "cidkid",
    Password: "null",
    DBName: "apollo",
    Port: 5432,
    SSLMode: "disable",
  })
  ledgerContext := Ledger.CreateLedgerContext("localhost:4001", "", "daml-script", true) // STUB!

  db.FirstOrCreate(&Database.Watermark{
    Offset: "000000000000000000",
    InitialOffset: "GENESIS",
    InstanceID: "Apollo",
    PK: 1,
  }, &Database.Watermark{ PK: 1 })

  f := func(transactionTree *v1.TransactionTree, error uint64) {
    if error != 0 { panic("err") }
    ParseAndWriteTransactionTree(db, transactionTree)
  }
  ledgerContext.GetTransactionTrees(GetLedgerOffset(db, "OLDEST"), f)
}

func GetLedgerOffset(db *gorm.DB, offset string) Ledger.TaggedOffset {
  var watermark Database.Watermark
  switch offset {
    case "OLDEST":
      if err := db.First(&watermark, 1); err.Error != nil {
         return Ledger.TaggedOffset { Ledger.Genesis, nil }
      } else {
        fmt.Printf("WATERMARK: %s\n", watermark.Offset)
        return Ledger.TaggedOffset { Ledger.AtOffset, &watermark.Offset }
      }
    default:
      return Ledger.TaggedOffset { Ledger.Genesis, nil }
  }
}

func ParseAndWriteTransactionTree(db *gorm.DB, transactionTree *v1.TransactionTree) {
  var eventIds []string
  for eventId, data := range(transactionTree.EventsById) {
    eventIds = append(eventIds, eventId)
    if created := data.GetCreated(); created != nil {
      contractKey := ParseLedgerData(created.ContractKey)
      contractKeyJSON, _ := json.Marshal(contractKey)

      createArguments := ParseLedgerData(&v1.Value {
        Sum: &v1.Value_Record {
          Record: created.CreateArguments,
        },
      })

      createArgumentsJSON, _ := json.Marshal(createArguments)

      templateID := created.TemplateId
      flatTemplateID := fmt.Sprintf("%s:%s:%s", templateID.PackageId, templateID.ModuleName, templateID.EntityName)

      //fmt.Println("Writing Create Event")
      db.Create(&Database.CreatesTable{
        ContractID: created.ContractId,
        ContractKey: contractKeyJSON,
        CreateArguments: createArgumentsJSON,
        Observers: created.Observers,
        Signatories: created.Signatories,
        Witnesses: created.WitnessParties,
        TemplateFqn: flatTemplateID,
        EventID: eventId,
        ArchiveEvent: nil,
      })
    }

    if exercised := data.GetExercised(); exercised != nil {
      templateID := exercised.TemplateId
      flatTemplateID := fmt.Sprintf("%s:%s:%s", templateID.PackageId, templateID.ModuleName, templateID.EntityName)

      choiceArguments := ParseLedgerData(exercised.ChoiceArgument)
      choiceArgumentsJSON, _ := json.Marshal(choiceArguments)

      exerciseResult := ParseLedgerData(exercised.ExerciseResult)
      exerciseResultJSON, _ := json.Marshal(exerciseResult)


      //fmt.Println("Writing Exercised Event")
      db.Create(&Database.ExercisedTable {
        EventID: eventId,
        ContractID: exercised.ContractId,
        TemplateFqn: flatTemplateID,
        Choice: exercised.Choice,
        ChoiceArgument: choiceArgumentsJSON,
        ActingParties: exercised.ActingParties,
        Consuming: exercised.Consuming,
        Witnesses: exercised.WitnessParties,
        ChildEventIds: exercised.ChildEventIds,
        ExerciseResult: exerciseResultJSON,
      })
    }
  }

  //ixOffset, err := strconv.ParseUint(transaction.Offset, 16, 64)
  //if err != nil { panic("Failed to convert offset to uint64") }

  //fmt.Println("Writing Transaction")
  db.Create(&Database.TransactionTable {
    TransactionId: transactionTree.TransactionId,
    WorkflowId: transactionTree.WorkflowId,
    CommandId: transactionTree.CommandId,
    Offset: transactionTree.Offset,
    EventIds: eventIds,
  })
}

func ParseLedgerData(value *v1.Value) (any) {
  switch x := value.GetSum().(type) {
    case (*v1.Value_Record):
      record := x.Record
      nMap := make(map[string]any)
      if record != nil {
        fields := record.GetFields()
        if fields != nil {
          for _, v := range(fields) {
            nMap[v.Label] = ParseLedgerData(v.Value)
          }
        }
      }
      return nMap
    case (*v1.Value_Party):
      return x.Party
    case (*v1.Value_Text):
      return x.Text
    case (*v1.Value_List):
      emptyList := []string{}
      if x.List.Elements != nil {
        var lMap [](any)
        for _, v := range(x.List.Elements) {
          lMap = append(lMap, ParseLedgerData(v))
        }
        return lMap
      } else {
        return emptyList
      }
    case (*v1.Value_Date):
      epoch := time.Unix(0, 0).UTC()
      timestamp := epoch.AddDate(0, 0, int(x.Date)).UTC().Format(time.RFC3339)
      return fmt.Sprintf("%s", timestamp)
    case (*v1.Value_Optional):
      emptyMap := make(map[string]any)
      if x.Optional.GetValue() != nil {
        emptyMap["Some"] = ParseLedgerData(x.Optional.Value)
        return emptyMap
      } else {
        emptyMap["None"] = nil
        return emptyMap
      }

    case (*v1.Value_Int64):
      return x.Int64
    case (*v1.Value_Numeric):
      return x.Numeric
    case (*v1.Value_Timestamp):
      timestamp := time.UnixMicro(x.Timestamp).UTC().Format(time.RFC3339)
      return fmt.Sprintf("%s", timestamp)
    case (*v1.Value_Bool):
      return x.Bool
    case (*v1.Value_ContractId):
      return x.ContractId
    case (*v1.Value_Map):
      newMap := make(map[string]any)
      for _, v := range(x.Map.Entries) {
        newMap[v.Key] = ParseLedgerData(v.Value)
      }
      return newMap
    case (*v1.Value_GenMap):
      mapList := [][]any{}
      for _, v := range(x.GenMap.Entries) {
        var inner []any
        inner = append(inner, ParseLedgerData(v.Key))
        inner = append(inner, ParseLedgerData(v.Value))
        mapList = append(mapList, inner)
      }
      return mapList
    case (*v1.Value_Variant):
      newMap := make(map[string]any)
      newMap[x.Variant.Constructor] = ParseLedgerData(x.Variant.Value)
      return newMap
    case (*v1.Value_Enum):
      return x.Enum.Constructor
    case (*v1.Value_Unit):
      emptyMap := make(map[string]any)
      return emptyMap
  }
  if value == nil {
    emptyMap := make(map[string]any)
    return emptyMap
  }
  panic(fmt.Sprintf("%s", value))
  // We should never hit this case, which is why panic is produced above
  return nil
}
