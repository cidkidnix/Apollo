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
  "os"
  //"github.com/schollz/progressbar/v3"
  "strconv"
  "runtime/debug"
  "runtime"
  "github.com/rs/zerolog/log"
  "github.com/rs/zerolog"
  "net/http"
  "strings"
  "io"
  "context"
  "slices"

  "github.com/jackc/pgx/v5"
  "github.com/jackc/pgx/v5/pgxpool"
  "crypto/tls"
  "crypto/x509"
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
  db, pgxConn := Database.SetupDatabase(config.DatabaseSettings)

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

  transactionQueueMap := &AtomicMap[uint64, Transaction]{
    mutex: &sync.RWMutex{},
    atomMap: make(map[uint64]Transaction),
  }

  //go func()(){
  //  pgxConn.Ping(context.Background())
  //  time.Sleep(time.Second * 10)
  //}()

  go WriteInsert(config.TxMaxPull, config.BatchSize, pgxConn, transactionQueueMap)

  debug.SetMemoryLimit(config.GC.MemoryLimit * 1024 * 1024)



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

      log.Debug().
              Int("queue_size", len(transactionQueueMap.GetMap())).
              Str("component", "transaction_queue").
              Send()


      poolStats := pgxConn.Stat()
      log.Info().
            Int32("acquired_connections", poolStats.AcquiredConns()).
            Int32("idle_connections", poolStats.IdleConns()).
            Str("component", "database_pool").
            Send()


      time.Sleep(time.Millisecond * 500)
    }
  }()
  RunTransactionListener(config, db, transactionQueueMap, GetLedgerOffset(db, config.LedgerOffset))
}

func RunTransactionListener(config Config.ApolloConfig, db *gorm.DB, insertSlice *AtomicMap[uint64, Transaction], ledgerOffset Ledger.TaggedOffset) {
  type internalAuthentication struct {
    ClientId string
    AccessToken string
    ExpireTime int
  }

  auth := &internalAuthentication{}

  if config.Oauth != nil {
    access_token, expire_time := GetLedgerAuthentication(*config.Oauth)
    auth.ClientId = config.Oauth.ClientId
    auth.AccessToken = access_token
    auth.ExpireTime = expire_time
  } else {
    auth.ClientId = config.User.ApplicationId
    auth.AccessToken = config.User.AuthToken
    auth.ExpireTime = config.User.Timeout
  }

  db.FirstOrCreate(&Database.Watermark{
    Offset: "000000000000000000",
    OffsetIx: 0,
    InitialOffset: config.LedgerOffset,
    InstanceID: "Apollo",
    PK: 1,
  }, &Database.Watermark{ PK: 1 })


  var tlsConfig *Ledger.GRPCTLSConfig
  if config.TLS != nil {
    _, err := os.Stat(config.TLS.CertFile)
    if err != nil {
      log.Fatal().
          Str("component", "tls-loader").
          Err(err).
          Msgf("Failed to find %s", config.TLS.CertFile)
    }
    log.Info().
        Str("component", "tls-loader").
        Msg("Loaded TLS Certificate")
    tlsConfig = &Ledger.GRPCTLSConfig{config.TLS.CertFile, config.TLS.ServerNameOverride}
  } else {
    tlsConfig = nil
  }

  ledgerContext := Ledger.CreateLedgerContext(fmt.Sprintf("%s:%d", config.LedgerConnection.Host, config.LedgerConnection.Port), auth.AccessToken, auth.ClientId, config.Sandbox, tlsConfig)

  parties := ledgerContext.GetParties()

  log.Info().
        Strs("read_parties", parties).
        Str("component", "parties").
        Send()

  ctx, cancel := context.WithCancel(context.Background())


  gotCounter := 0
  currentOffset := ""

  templateFilters := ParseTemplateFilters(config.Filter.Template)

  f := func(transactionTree *v1.TransactionTree, error error) {
    if error != nil { panic(error) }
    currentOffset = transactionTree.Offset
    if config.GC.ManualGCPause {
      debug.SetGCPercent(-1)
      if !config.GC.ManualGCRun {
        defer debug.SetGCPercent(100)
      }
    }

    ParseAndModifySlice(templateFilters, db, insertSlice, transactionTree)
    gotCounter += 1
    if gotCounter > config.GC.ManualGCRunAmount && config.GC.ManualGCRun {
      log.Info().
            Int("gc_offset_count", gotCounter).
            Str("component", "manual_gc_run").
            Send()
      runtime.GC()
      gotCounter = 0
    }

    log.Debug().
          Str("current_offset", currentOffset).
          Str("component", "offset_tracker").
          Send()
  }

  var wg sync.WaitGroup
  wg.Add(1)
  go func()() {
    defer wg.Done()
    currentTime := 0
    log.Info().
          Int("auth_time_seconds", auth.ExpireTime).
          Str("component", "transaction_streamer_oauth").
          Send()

    for {
      select {
        case <-ctx.Done():
          return
        default:
          time.Sleep(time.Second)
          if currentTime >= auth.ExpireTime {
            cancel()
            return
          } else { currentTime += 1 }
      }
    }
  }()

  _, returnErr := ledgerContext.GetTransactionTrees(ctx, ledgerOffset, f)

  cancel()
  wg.Wait()

  log.Info().
        Str("component", "transaction_streamer").
        Err(returnErr).
        Msg("GetTransactionTrees Returned an Error, Retrying")

  RunTransactionListener(config, db, insertSlice, Ledger.TaggedOffset{ Ledger.AtOffset, &currentOffset })

}

func GetLedgerAuthentication(oauthConfig Config.Oauth) (string, int) {
    systemPool, err := x509.SystemCertPool()
    if err != nil {
      log.Fatal().
          Err(err).
          Str("component", "oauth").
          Msgf("Failed to get system cert pool", "")
    }
    transport := &http.Transport{
      MaxIdleConns: 10,
      TLSClientConfig: &tls.Config {
        RootCAs: systemPool,
      },
    }
    client := &http.Client{
      Transport: transport,
      Timeout: time.Second * 10,
    }

    var extraParams []string
    for k, v := range(oauthConfig.ExtraParams) {
      extraParams = append(extraParams, fmt.Sprintf("%s=%s", k, v))
    }
    body := []string{
      fmt.Sprintf("client_id=%s", oauthConfig.ClientId),
      fmt.Sprintf("client_secret=%s", oauthConfig.ClientSecret),
      fmt.Sprintf("grant_type=%s", oauthConfig.GrantType),
    }

    body = append(body, extraParams...)

    req, err := http.NewRequest(
      "POST",
      oauthConfig.Url,
      strings.NewReader(strings.Join(body[:], "&")),
    )

    if err != nil {
        log.Fatal().
              Err(err).
              Str("component", "oauth").
              Msgf("Fatal error", "")
    }
    req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
    log.Info().
          Str("method", "POST").
          Str("url", oauthConfig.Url).
          Str("client_id", oauthConfig.ClientId).
          Strs("extra_params", extraParams).
          Str("component", "oauth").
          Send()
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

    if tok, ok := respMap["access_token"].(string); ok {
      if tok1, ok1 := respMap["expires_in"].(float64); ok1 {
        log.Info().
            Str("component", "oauth").
            Msg(fmt.Sprintf("Successfully got authentication token for %s", oauthConfig.ClientId))
        return tok, int(tok1)
      } else {
        log.Fatal().
              Err(err).
              Str("component", "oauth").
              Msgf("expires_in field is missing, or it's not a float or integer")
      }
    } else {
      log.Fatal().
          Err(err).
          Str("component", "oauth").
          Msgf("access_token field is missing, or it's not a string")
    }

    return "", 0
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
    case "GENESIS":
      return Ledger.TaggedOffset { Ledger.Genesis, nil }
    default:
      return Ledger.TaggedOffset { Ledger.AtOffset, &offset }
  }
}

type InternalFilter struct {
  IgnoreModules map[string]interface{}
  IgnorePackages map[string]interface{}
  IgnoreEntity map[string]interface{}
}

func ParseTemplateFilters(templateFilter Config.TemplateFilter) InternalFilter {
  modules := make(map[string]interface{})
  packages := make(map[string]interface{})
  entity := make(map[string]interface{})
  for _, i := range(templateFilter.IgnoreModules) {
    modules[i] = nil
  }

  for _, i := range(templateFilter.IgnorePackages) {
    packages[i] = nil
  }

  for _, i := range(templateFilter.IgnoreEntities) {
    packages[i] = nil
  }

  return InternalFilter{modules, packages, entity}
}

func IsFiltered(filter InternalFilter, template *v1.Identifier) bool {
  if _, ok := filter.IgnoreModules[template.ModuleName]; ok {
    return true
  }
  if _, ok := filter.IgnorePackages[template.PackageId]; ok {
    return true
  }
  if _, ok := filter.IgnoreEntity[template.EntityName]; ok {
    return true
  }
  return false
}

func ParseAndModifySlice(templateFilter InternalFilter, db *gorm.DB, atomMap *AtomicMap[uint64, Transaction], transactionTree *v1.TransactionTree) {
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
    if created := data.GetCreated(); created != nil {
      if IsFiltered(templateFilter, created.TemplateId) {
          log.Debug().
            Str("event_id", created.EventId).
            Str("event_type", "create").
            Str("component", "filter").
            Msg("Filtered event based on template filters")
          continue
      }
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
        }

        creates.Modify(func(slice []*Database.CreatesTable)([]*Database.CreatesTable){
          slice = append(slice, &f)
          return slice
        })
      }()
    }

    if exercised := data.GetExercised(); exercised != nil {
      if IsFiltered(templateFilter, exercised.TemplateId) {
          log.Debug().
            Str("event_id", exercised.EventId).
            Str("event_type", "exercise").
            Str("component", "filter").
            Msg("Filtered event based on template filters")
          continue
      }
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
    eventIds = append(eventIds, eventId)
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



  atomMap.Modify(func(atomMap map[uint64]Transaction)(map[uint64]Transaction) {
    insert := Transaction {
      txTable: txTable,
      creates: creates.GetSlice(),
      exercises: exercises.GetSlice(),
    }
    atomMap[ixOffset] = insert
    return atomMap
  })

  log.Debug().
            Int("creates_size", len(creates.GetSlice())).
            Int("exercises_size", len(exercises.GetSlice())).
            Str("component", "parsed_data").
            Send()
}

func chunkBy[T any](items []T, chunkSize int) (chunks [][]T) {
    if chunkSize < 0 {
      return append(chunks, items)
    }
    for chunkSize < len(items) {
        items, chunks = items[chunkSize:], append(chunks, items[0:chunkSize:chunkSize])
    }
    return append(chunks, items)
}

func WriteInsert(txMaxPull int, batchSize Config.BatchSizes, db *pgxpool.Pool, atomSet *AtomicMap[uint64, Transaction]) {
  for {
    // We can't lock the whole map while doing DB operations
    // using this without caution will cause transactions to be missed
    t := atomSet.GetMap()

    if len(t) <= 0 { continue }

    var creates []*Database.CreatesTable
    var exercises []*Database.ExercisedTable
    var transactions []*Database.TransactionTable

    maxTxs := txMaxPull
    if len(t) < txMaxPull {
      maxTxs = len(t)
    }

    var txKeys []uint64
    for k, _ := range(t) {
      txKeys = append(txKeys, k)
    }

    slices.Sort(txKeys)

    for i := 0; i < maxTxs; i++ {
      log.Debug().
            Uint64("offset", txKeys[i]).
            Str("component", "report_insert_offset").
            Send()
      creates = append(creates, t[txKeys[i]].creates...)
      exercises = append(exercises, t[txKeys[i]].exercises...)
      transactions = append(transactions, t[txKeys[i]].txTable)

    }

    log.Debug().
            Int("creates_size", len(creates)).
            Int("exercises_size", len(exercises)).
            Int("transactions_size", len(transactions)).
            Str("component", "report_insert_size").
            Send()

    logMsg := log.Info()

    fullCopy := time.Now()


    if len(creates) > 0 {
      initial := time.Now()
      batches := chunkBy(creates, batchSize.Creates)
      logMsg.Int("creates_batches", len(batches))
      var wg sync.WaitGroup
      for _, create := range(batches) {
        wg.Add(1)
        go func()(){
          defer wg.Done()
          _, err := db.CopyFrom(
            context.Background(),
            pgx.Identifier{"__creates"},
            []string{"event_id", "contract_id", "contract_key", "payload", "template_fqn", "witnesses", "observers", "signatories", "created_at"},
            pgx.CopyFromSlice(len(create), func(i int) ([]any, error) {
                return []any{
                    create[i].EventID,
                    create[i].ContractID,
                    create[i].ContractKey,
                    create[i].CreateArguments,
                    create[i].TemplateFqn,
                    create[i].Witnesses,
                    create[i].Observers,
                    create[i].Signatories,
                    create[i].CreatedAt,
               }, nil
            }),
          )

          if err != nil { panic(err) }
        }()

      }

      wg.Wait()

      logMsg.
        Int("creates_copy_count", len(creates)).
        Int64("creates_latency_ms", time.Since(initial).Milliseconds())
    }
    if len(exercises) > 0 {
      var wg sync.WaitGroup

      // Trigger updates are slow, workaround this by
      // dispatching batches of 100 in parallel
      chunks := chunkBy(exercises, batchSize.Exercises)
      initial := time.Now()
      logMsg.Int("exercises_batches", len(chunks))
      for _, exercise := range(chunks) {
        wg.Add(1)
        go func()(){
          defer wg.Done()
          _, err := db.CopyFrom(
            context.Background(),
            pgx.Identifier{"__exercised"},
            []string{
                "event_id",
                "contract_id",
                "template_fqn",
                "choice",
                "choice_argument",
                "acting_parties",
                "consuming",
                "child_event_ids",
                "witnesses",
                "exercise_result",
                "offset_ix",
            },
            pgx.CopyFromSlice(len(exercise), func(i int) ([]any, error) {
                return []any{
                  exercise[i].EventID,
                  exercise[i].ContractID,
                  exercise[i].TemplateFqn,
                  exercise[i].Choice,
                  exercise[i].ChoiceArgument,
                  exercise[i].ActingParties,
                  exercise[i].Consuming,
                  exercise[i].ChildEventIds,
                  exercise[i].Witnesses,
                  exercise[i].ExerciseResult,
                  exercise[i].OffsetIx,
               }, nil
            }),
          )
          if err != nil { panic(err) }
        }()
      }
      wg.Wait()
      logMsg.
        Int("exercises_copy_count", len(exercises)).
        Int64("exercises_latency_ms", time.Since(initial).Milliseconds())
    }
    if len(transactions) > 0 {
      var wg sync.WaitGroup
      initial := time.Now()

      // Database will be "unstable" during this event
      // We pull out the last offset out of the transaction queue to make sure
      // database state always becomes consistent with ledger state
      finalTx := []*Database.TransactionTable{transactions[len(transactions) - 1]}
      logMsg.Uint64("offset", finalTx[0].OffsetIx)
      if !(len(transactions) - 1 <= 0) {
        chunks := chunkBy(transactions[:len(transactions) - 1], batchSize.Transactions)
        logMsg.Int("transaction_batches", len(chunks))
        for _, transaction := range(chunks) {
          wg.Add(1)
          go func()(){
            defer wg.Done()
            _, err := db.CopyFrom(
              context.Background(),
              pgx.Identifier{"__transactions"},
              []string{
                  "transaction_id",
                  "workflow_id",
                  "command_id",
                  "offset",
                  "offset_ix",
                  "event_ids",
                  "effective_at",
              },
              pgx.CopyFromSlice(len(transaction), func(i int) ([]any, error) {
                  return []any{
                    transaction[i].TransactionId,
                    transaction[i].WorkflowId,
                    transaction[i].CommandId,
                    transaction[i].Offset,
                    transaction[i].OffsetIx,
                    transaction[i].EventIds,
                    transaction[i].EffectiveAt,
                 }, nil
              }),
            )

            if err != nil { panic(err) }
          }()
        }
      }

      wg.Wait()

      _, err := db.CopyFrom(
        context.Background(),
        pgx.Identifier{"__transactions"},
        []string{
            "transaction_id",
            "workflow_id",
            "command_id",
            "offset",
            "offset_ix",
            "event_ids",
            "effective_at",
        },
        pgx.CopyFromSlice(len(finalTx), func(i int) ([]any, error) {
            return []any{
              finalTx[i].TransactionId,
              finalTx[i].WorkflowId,
              finalTx[i].CommandId,
              finalTx[i].Offset,
              finalTx[i].OffsetIx,
              finalTx[i].EventIds,
              finalTx[i].EffectiveAt,
           }, nil
        }),
      )

      if err != nil { panic(err) }
      logMsg.
        Int("transactions_copy_count", len(transactions)).
        Int64("transactions_latency_ms", time.Since(initial).Milliseconds())
    }

    logMsg.
         Int64("total_time_ms", time.Since(fullCopy).Milliseconds()).
         Str("component", "database_copy").Send()

    // Remove all the transactionIds from this map that we've already written
    atomSet.Modify(func(atomMap map[uint64]Transaction)(map[uint64]Transaction) {
      for i := 0; i < maxTxs; i++  {
        delete(atomMap, txKeys[i])
      }
      return atomMap
    })
  }
}

func ParseLedgerData(value *v1.Value) (any) {
  initial := time.Now()
  r := ParseLedgerDataInternal(nil, value, 0)
  end := time.Since(initial)
  log.Debug().
        Int64("time_to_parse_value_ns", end.Nanoseconds()).
        Str("component", "parser").
        Send()
  return r
}

func ParseLedgerDataInternal(lastType *v1.Value, value *v1.Value, count int) (any) {
  initial := time.Now()
  logMsg := log.Trace().
        Str("type", fmt.Sprintf("%T", value.GetSum())).
        Int("recursion_depth", count)

  f := func()(){
    go func()() {
      logMsg.
        Int64("processing_time_ns", time.Since(initial).Nanoseconds()).
        Str("component", "parser_internal").Send()
    }()
  }
  defer f()

  if value == nil {
    return map[string]any{}
  }

  switch x := value.GetSum().(type) {
    case (*v1.Value_Record):
      record := x.Record
      if record != nil {
        fields := record.GetFields()
        if fields != nil {
          logMsg.Int("num_fields", len(fields))
          recordMap := make(map[string]any)
          for _, v := range(fields) {
            recordMap[v.Label] = ParseLedgerDataInternal(value, v.Value, count + 1)
          }
          return recordMap
        }
      }
      return map[string]any{}

    case (*v1.Value_Party):
      return x.Party
    case (*v1.Value_Text):
      return x.Text
    case (*v1.Value_List):
      if x.List.Elements != nil {
        logMsg.Int("num_elements", len(x.List.Elements))
        elements := make([]any, len(x.List.Elements))
        for i := 0; i < len(x.List.Elements); i++ {
          elements[i] = ParseLedgerDataInternal(value, x.List.Elements[i], count + 1)
        }
        return elements
      }
      return []any{}
    case (*v1.Value_Date):
      epoch := time.Unix(0, 0).UTC()
      timestamp := epoch.AddDate(0, 0, int(x.Date)).UTC().Format(time.RFC3339)
      return fmt.Sprintf("%s", timestamp)
    case (*v1.Value_Optional):
      canBeNil := false
      if lastType == nil { canBeNil = true }
      if lastType != nil {
        if _, ok := lastType.GetSum().(*v1.Value_Optional); !ok {
          canBeNil = true
        }
      }
      if x.Optional.GetValue() != nil {
        nextType, ok := x.Optional.GetValue().GetSum().(*v1.Value_Optional)
        if ok && nextType.Optional.GetValue() == nil {
            return make([]any, 0)
        }
        if !ok {
            return ParseLedgerDataInternal(value, x.Optional.Value, count + 1)
        }
        optionalVal := make([]any, 1)
        optionalVal[0] = ParseLedgerDataInternal(value, x.Optional.Value, count + 1)
        return optionalVal
      }
      if canBeNil { return nil } else { return make([]any, 0) }

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
      logMsg.Int("map_entries_length", len(x.Map.Entries))
      for _, v := range(x.Map.Entries) {
        newMap[v.Key] = ParseLedgerDataInternal(value, v.Value, count + 1)
      }
      return newMap
    case (*v1.Value_GenMap):
      logMsg.Int("genmap_entries_length", len(x.GenMap.Entries))
      genMapList := make([][]any, len(x.GenMap.Entries))
      for i := 0; i < len(x.GenMap.Entries); i++ {
        genMapList[i] = []any{
          ParseLedgerDataInternal(value, x.GenMap.Entries[i].Key, count + 1),
          ParseLedgerDataInternal(value, x.GenMap.Entries[i].Value, count + 1),
        }
      }
      return genMapList
    case (*v1.Value_Variant):
      return map[string]any{
          "tag": x.Variant.Constructor,
          "value": ParseLedgerDataInternal(value, x.Variant.Value, count + 1),
      }
    case (*v1.Value_Enum):
      return x.Enum.Constructor
    case (*v1.Value_Unit):
      emptyMap := make(map[string]any)
      return emptyMap
  }

  panic("should never reach here")

}
