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
  "runtime/debug"
  "runtime"
  "github.com/rs/zerolog/log"
  "github.com/rs/zerolog"
  "net/http"
  "strings"
  "io"
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
  type internalAuthentication struct {
    ClientId string
    AccessToken string
  }

  auth := &internalAuthentication{}

  if config.Oauth != nil {
    access_token := GetLedgerAuthentication(*config.Oauth)
    auth.ClientId = config.Oauth.ClientId
    auth.AccessToken = access_token
  } else {
    auth.ClientId = config.User.ApplicationId
    auth.AccessToken = config.User.AuthToken
  }

  zerolog.SetGlobalLevel(Config.ParseLogLevel(config.LogLevel))
  zerolog.TimeFieldFormat = time.RFC3339Nano
  log.Info().
        Str("ledger_offset", config.LedgerOffset).
        Interface("user", config.User).
        Bool("is_sandbox", config.Sandbox).
        Interface("ledger_connection", config.LedgerConnection).
        Interface("database_settings", config.DatabaseSettings).
        Interface("gc", config.GC).
        Str("loglevel", config.LogLevel).
        Str("component", "application_settings").
        Send()
  db := Database.SetupDatabase(config.DatabaseSettings)

  ledgerContext := Ledger.CreateLedgerContext(fmt.Sprintf("%s:%d", config.LedgerConnection.Host, config.LedgerConnection.Port), auth.AccessToken, auth.ClientId, config.Sandbox)

  parties := ledgerContext.GetParties()

  log.Info().
        Strs("read_parties", parties).
        Str("component", "parties").
        Send()

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
  gotCounter := 0
  currentOffset := ""
  debug.SetMemoryLimit(config.GC.MemoryLimit * 1024 * 1024)
  f := func(transactionTree *v1.TransactionTree, error error) {
    if error != nil { panic(error) }
    currentOffset = transactionTree.Offset
    if config.GC.ManualGCPause {
      debug.SetGCPercent(-1)
      if !config.GC.ManualGCRun {

      } else {
        defer debug.SetGCPercent(100)
      }
    }

    ParseAndModifySlice(db, insertSlice, transactionTree)
    gotCounter += 1
    if gotCounter > config.GC.ManualGCRunAmount {
      log.Info().
            Int("gc_offset_count", gotCounter).
            Str("component", "manual_gc_run").
            Send()
      runtime.GC()
      gotCounter = 0
    }
  }
  go WriteInsert(config.BatchSize, db, insertSlice)
  // Prints out stats
  go func()(){
    for {
      memStats := runtime.MemStats{}
      runtime.ReadMemStats(&memStats)
      log.Debug().
            Uint64("alloc", memStats.Alloc).
            Uint64("total_alloc", memStats.TotalAlloc).
            Uint64("sys", memStats.Sys).
            Uint64("lookups", memStats.Lookups).
            Uint64("frees", memStats.Frees).
            Uint64("heap_alloc", memStats.HeapAlloc).
            Uint64("heap_sys", memStats.HeapSys).
            Uint64("heap_idle", memStats.HeapIdle).
            Uint64("heap_in_use", memStats.HeapInuse).
            Uint64("heap_released", memStats.HeapReleased).
            Uint64("heap_objects", memStats.HeapObjects).
            Uint64("stack_in_use", memStats.StackInuse).
            Uint64("stack_sys", memStats.StackSys).
            Float64("gc_cpu_fraction", memStats.GCCPUFraction).
            Uint32("num_forced_gc", memStats.NumForcedGC).
            Uint32("num_gc", memStats.NumGC).
            Uint64("next_gc", memStats.NextGC).
            Time("last_gc", time.UnixMicro(int64(memStats.LastGC / 1000))).
            Int64("pause_total_ms", time.Duration(memStats.PauseTotalNs).Milliseconds()).
            Int("num_goroutine", runtime.NumGoroutine()).
            Str("component", "runtime_mem_stats").
            Send()
      log.Info().
            Str("current_offset", currentOffset).
            Str("component", "offset_tracker").
            Send()

      //fmt.Printf("[GC] Last GC: %v\n", gcStats.LastGC)
      //fmt.Printf("[GC] Collected Garbage %d times\n", gcStats.NumGC)
      //fmt.Printf("[GC] Total time in pause %v\n", gcStats.PauseTotal)
      //if len(gcStats.Pause) > 0 {
      //  fmt.Printf("[GC] Last GC Took %v\n", gcStats.Pause[0])
      //}

      log.Debug().
              Int("queue_size", len(insertSlice.GetMap())).
              Str("component", "transaction_queue").
              Send()

      time.Sleep(time.Millisecond * 500)
    }
  }()
  ledgerContext.GetTransactionTrees(GetLedgerOffset(db, config.LedgerOffset), f)
}



func GetLedgerAuthentication(oauthConfig Config.Oauth) string {
    transport := &http.Transport{
      MaxIdleConns: 10,
      DisableCompression: true,
    }
    client := &http.Client{
      Transport: transport,
    }

    var extraParams []string
    for k, v := range(oauthConfig.ExtraParams) {
      extraParams = append(extraParams, fmt.Sprintf("%s=%s", k, v))
    }
    body := []string{
      fmt.Sprintf("client_id=%s", oauthConfig.ClientId),
      fmt.Sprintf("client_secret=%s", oauthConfig.ClientSecret),
      fmt.Sprintf("grant_type=%s", oauthConfig.GrantType),
      fmt.Sprintf("redirect_url=%s", "http://localhost"),
    }

    body = append(body, extraParams...)

    req, err := http.NewRequest(
      "POST",
      oauthConfig.Url,
      strings.NewReader(strings.Join(body[:], "&")),
    )
    req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
    resp, err := client.Do(req)

    if err != nil {
      log.Fatal().
            Err(err).
            Str("component", "oauth").
            Msgf("Error fetching oauth for client_id %s", oauthConfig.ClientId)
    }

    respBody, err := io.ReadAll(resp.Body)
    if err != nil {
      log.Fatal().
            Err(err).
            Str("component", "oauth").
            Msgf("Error reading body of http response for client_id %s", oauthConfig.ClientId)
    }

    respMap := make(map[string]any)
    json.Unmarshal(respBody, &respMap)

    if tok, ok := respMap["access_token"]; ok {
      if _, ok := tok.(string); ok {
        log.Info().
            Str("component", "oauth").
            Msg(fmt.Sprintf("Successfully got authentication token for %s", oauthConfig.ClientId))
        return tok.(string)
      }
    } else {
      log.Fatal().
          Err(err).
          Str("component", "oauth").
          Msgf("access_token field is missing, or it's not a string", "")
    }

    return ""
}

func GetLedgerOffset(db *gorm.DB, offset string) Ledger.TaggedOffset {
  var watermark Database.Watermark
  switch offset {
    case "OLDEST":
      if err := db.First(&watermark, 1); err.Error != nil {
         return Ledger.TaggedOffset { Ledger.Genesis, nil }
      } else {
        log.Info().Str("starting_offset", watermark.Offset).Str("component", "get_offset").Send()
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
    EffectiveAt: transactionTree.EffectiveAt.AsTime(),
  }

  //time.Sleep(time.Second * 1)



  atomMap.Modify(func(atomMap map[string]Transaction)(map[string]Transaction) {
    insert := Transaction {
      txTable: txTable,
      creates: creates.GetSlice(),
      exercises: exercises.GetSlice(),
    }
    atomMap[txTable.TransactionId] = insert
    return atomMap
  })

  log.Debug().
            Int("creates_size", len(creates.GetSlice())).
            Int("exercises_size", len(exercises.GetSlice())).
            Str("component", "parsed_data").
            Send()
}

func WriteInsert(batchSize int, db *gorm.DB, atomSet *AtomicMap[string, Transaction]) {
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

    log.Debug().
            Int("creates_size", len(creates)).
            Int("exercises_size", len(exercises)).
            Int("transactions_size", len(transactions)).
            Str("component", "report_insert_size").
            Send()

    // TODO(Dylan): More threads!
    if len(creates) > 0 { db.CreateInBatches(creates, batchSize) }
    if len(exercises) > 0 { db.CreateInBatches(exercises, batchSize) }
    if len(transactions) > 0 { db.CreateInBatches(transactions, batchSize) }

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
  return ParseLedgerDataInternal(value, 0)
}

func ParseLedgerDataInternal(value *v1.Value, count int) (any) {
  switch x := value.GetSum().(type) {
    case (*v1.Value_Record):
      record := x.Record
      nMap := make(map[string]any)
      var wg sync.WaitGroup
      t := AtomicMap[string, any]{
        mutex: &sync.RWMutex{},
        atomMap: make(map[string]any),
      }
      if record != nil {
        fields := record.GetFields()
        if fields != nil {
          for _, v := range(fields) {
            wg.Add(1)
            go func()(){
              data := ParseLedgerDataInternal(v.Value, count + 1)
              t.Modify(func(atomMap map[string]any)(map[string]any) {
                atomMap[v.Label] = data
                return atomMap
              })
              return
            }()
          }
          nMap = t.GetMap()
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
          lMap = append(lMap, ParseLedgerDataInternal(v, count + 1))
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
        emptyMap["Some"] = ParseLedgerDataInternal(x.Optional.Value, count + 1)
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
        newMap[v.Key] = ParseLedgerDataInternal(v.Value, count + 1)
      }
      return newMap
    case (*v1.Value_GenMap):
      mapList := [][]any{}
      for _, v := range(x.GenMap.Entries) {
        var inner []any
        inner = append(inner, ParseLedgerDataInternal(v.Key, count + 1))
        inner = append(inner, ParseLedgerDataInternal(v.Value, count + 1))
        mapList = append(mapList, inner)
      }
      return mapList
    case (*v1.Value_Variant):
      newMap := make(map[string]any)
      newMap[x.Variant.Constructor] = ParseLedgerDataInternal(x.Variant.Value, count + 1)
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
