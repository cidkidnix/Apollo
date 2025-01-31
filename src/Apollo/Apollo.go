package Apollo

import (
  "Apollo/src/Ledger"
  "Apollo/src/Database"
  "Apollo/src/Config"
  "github.com/digital-asset/dazl-client/v7/go/api/com/daml/ledger/api/v1"
  "gorm.io/gorm"
  "fmt"
  "encoding/json"
  "time"
  "sync"
  //"github.com/schollz/progressbar/v3"
  "strconv"
)


// TODO(cidkid): Rename!
type Transaction struct {
  txTable *Database.TransactionTable
  creates []*Database.CreatesTable
  exercises []*Database.ExercisedTable
}

type AtomicMap[K comparable, V any] struct {
  mutex *sync.RWMutex
  atomMap map[K]V
}

type AtomicSlice[K any] struct {
  mutex *sync.RWMutex
  slice []K
}


type Map[K comparable, V any] map[K]V
type Slice[K any] []K

type Copy[T any] interface {
    Copy() T
}

func (cMap Map[K, V]) Copy() Map[K, V] {
  newMap := make(map[K]V)
  for k, v := range(cMap) {
    newMap[k] = v
  }

  return newMap
}

func (slice Slice[K]) Copy() []K {
  var newS []K
  for _, i := range(slice) {
    newS = append(newS, i)
  }
  return newS
}

func (atomMap *AtomicMap[K, V]) Modify(cb func(set map[K]V)(map[K]V)) {
  atomMap.mutex.Lock()
  defer atomMap.mutex.Unlock()
  atomMap.atomMap = cb(atomMap.atomMap)
}

func (atomMap *AtomicMap[K, V]) GetMap() map[K]V {
  atomMap.mutex.RLock()
  defer atomMap.mutex.RUnlock()

  return Map[K, V](atomMap.atomMap).Copy()
}

func (atomSlice *AtomicSlice[K]) Modify(cb func(slice []K)([]K)) {
  atomSlice.mutex.Lock()
  defer atomSlice.mutex.Unlock()
  atomSlice.slice = cb(atomSlice.slice)
}

func (atomSlice *AtomicSlice[K]) GetSlice() []K {
  atomSlice.mutex.RLock()
  defer atomSlice.mutex.RUnlock()
  return Slice[K](atomSlice.slice).Copy()
}


func WatchTransactionTrees(config Config.ApolloConfig) {
  db := Database.SetupDatabase(config.DatabaseSettings)
  ledgerContext := Ledger.CreateLedgerContext(fmt.Sprintf("%s:%d", config.LedgerConnection.Host, config.LedgerConnection.Port), config.User.AuthToken, config.User.ApplicationId, config.Sandbox)

  db.FirstOrCreate(&Database.Watermark{
    Offset: "000000000000000000",
    OffsetIx: 0,
    InitialOffset: config.LedgerOffset,
    InstanceID: "Apollo",
    PK: 1,
  }, &Database.Watermark{ PK: 1 })

  //bar := progressbar.Default(-1)

  insertSlice := &AtomicMap[string, Transaction]{
    mutex: &sync.RWMutex{},
    atomMap: make(map[string]Transaction),
  }
  f := func(transactionTree *v1.TransactionTree, error error) {
    if error != nil { panic(error) }
    //bar.Add(1)
    //fmt.Printf("Got Transaction: %s\n", transactionTree.TransactionId)
    initial := time.Now()
    ParseAndModifySlice(db, insertSlice, transactionTree)
    end := time.Since(initial)
    fmt.Printf("Time to ingest offset %v\n", end)
  }
  go WriteInsert(db, insertSlice)
  ledgerContext.GetTransactionTrees(GetLedgerOffset(db, config.LedgerOffset), f)
}

func GetLedgerOffset(db *gorm.DB, offset string) Ledger.TaggedOffset {
  var watermark Database.Watermark
  switch offset {
    case "OLDEST":
      if err := db.First(&watermark, 1); err.Error != nil {
         return Ledger.TaggedOffset { Ledger.Genesis, nil }
      } else {
        fmt.Printf("Starting Offset: %s\n", watermark.Offset)
        return Ledger.TaggedOffset { Ledger.AtOffset, &watermark.Offset }
      }
    default:
      return Ledger.TaggedOffset { Ledger.Genesis, nil }
  }
}

func ParseAndModifySlice(db *gorm.DB, atomMap *AtomicMap[string, Transaction], transactionTree *v1.TransactionTree) {
  var eventIds []string

  var createsS []*Database.CreatesTable
  var exercisesS []*Database.ExercisedTable
  creates := &AtomicSlice[*Database.CreatesTable]{
    mutex: &sync.RWMutex{},
    slice: createsS,
  }
  exercises := &AtomicSlice[*Database.ExercisedTable]{
    mutex: &sync.RWMutex{},
    slice: exercisesS,
  }



  var wg sync.WaitGroup

  ixOffset, err := strconv.ParseUint(transactionTree.Offset, 16, 64)
  if err != nil { panic("Failed to parse offset") }

  for eventId, data := range(transactionTree.EventsById) {
    eventIds = append(eventIds, eventId)
    if created := data.GetCreated(); created != nil {
      wg.Add(1)
      go func()(){
        defer wg.Done()

        contractKeyTime := time.Now()
        contractKey := ParseLedgerData(created.ContractKey)
        cKend := time.Since(contractKeyTime)

        fmt.Printf("Time to parse contract key: %s\n", cKend)
        contractKeyJSON, _ := json.Marshal(contractKey)



        createParseTime := time.Now()
        createArguments := ParseLedgerData(&v1.Value {
          Sum: &v1.Value_Record {
            Record: created.CreateArguments,
          },
        })
        createParseEndTime := time.Since(createParseTime)
        createArgumentsJSON, _ := json.Marshal(createArguments)

        fmt.Printf("Time to parse payload: %s\n", createParseEndTime)

        templateID := created.TemplateId
        flatTemplateID := fmt.Sprintf("%s:%s:%s", templateID.PackageId, templateID.ModuleName, templateID.EntityName)

        f := Database.CreatesTable{
          ContractID: created.ContractId,
          ContractKey: contractKeyJSON,
          CreateArguments: createArgumentsJSON,
          Observers: created.Observers,
          Signatories: created.Signatories,
          Witnesses: created.WitnessParties,
          TemplateFqn: flatTemplateID,
          EventID: eventId,
          CreatedAt: ixOffset,
          ArchiveEvent: nil,
        }

        creates.Modify(func(slice []*Database.CreatesTable)([]*Database.CreatesTable){
          slice = append(slice, &f)
          return slice
        })
      }()
    }

    if exercised := data.GetExercised(); exercised != nil {
      wg.Add(1)
      go func()(){
        defer wg.Done()
        templateID := exercised.TemplateId
        flatTemplateID := fmt.Sprintf("%s:%s:%s", templateID.PackageId, templateID.ModuleName, templateID.EntityName)

        choiceArguments := ParseLedgerData(exercised.ChoiceArgument)
        choiceArgumentsJSON, _ := json.Marshal(choiceArguments)

        exerciseResult := ParseLedgerData(exercised.ExerciseResult)
        exerciseResultJSON, _ := json.Marshal(exerciseResult)


        f := Database.ExercisedTable {
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
          OffsetIx: ixOffset,
        }
        exercises.Modify(func(slice []*Database.ExercisedTable)([]*Database.ExercisedTable){
          slice = append(slice, &f)
          return slice
        })
      }()
    }
  }

  wg.Wait()

  txTable := &Database.TransactionTable {
    TransactionId: transactionTree.TransactionId,
    WorkflowId: transactionTree.WorkflowId,
    CommandId: transactionTree.CommandId,
    Offset: transactionTree.Offset,
    OffsetIx: ixOffset,
    EventIds: eventIds,
  }

  atomMap.Modify(func(atomMap map[string]Transaction)(map[string]Transaction) {
    insert := Transaction {
      txTable: txTable,
      creates: creates.GetSlice(),
      exercises: exercises.GetSlice(),
    }
    atomMap[txTable.TransactionId] = insert
    return atomMap
  })
}

func WriteInsert(db *gorm.DB, atomSet *AtomicMap[string, Transaction]) {
  for {
    // We can't lock the whole map while doing DB operations
    // using this without caution will cause transactions to be missed
    t := atomSet.GetMap()
    if len(t) <= 0 { continue }

    var creates []*Database.CreatesTable
    var exercises []*Database.ExercisedTable
    var transactions []*Database.TransactionTable

    for _, v := range(t) {
      creates = append(creates, v.creates...)
      exercises = append(exercises, v.exercises...)
      transactions = append(transactions, v.txTable)
    }

    // TODO(Dylan): More threads!
    if len(creates) > 0 { db.CreateInBatches(creates, 100) }
    if len(exercises) > 0 { db.CreateInBatches(exercises, 100) }
    if len(transactions) > 0 { db.CreateInBatches(transactions, 100) }

    // Remove all the transactionIds from this map that we've already written
    atomSet.Modify(func(atomMap map[string]Transaction)(map[string]Transaction) {
      for k, _ := range(t) {
        delete(atomMap, k)
      }
      return atomMap
    })
    time.Sleep(time.Nanosecond * 200)
  }
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
