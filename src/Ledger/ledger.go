package Ledger


import (
  "github.com/digital-asset/dazl-client/v7/go/api/com/daml/ledger/api/v1"
  "github.com/digital-asset/dazl-client/v7/go/api/com/daml/ledger/api/v1/admin"
  "google.golang.org/grpc"
  "google.golang.org/grpc/metadata"
  "context"
  "time"
  "log"
  "fmt"
  "io"
  "github.com/google/uuid"
)

type PartyRightsMode int
type LedgerOffset int

const (
  ONLY_READ_AS PartyRightsMode = iota
  ONLY_ACT_AS
  BOTH
)

var partyRightsMode = map[PartyRightsMode]string{
  ONLY_READ_AS: "OnlyReadAs",
  ONLY_ACT_AS: "OnlyActAs",
  BOTH: "BOTH",
}

func (prm PartyRightsMode) String() string {
  return partyRightsMode[prm]
}

const (
  Genesis LedgerOffset = iota
  AtOffset
)

var ledgerOffset = map[LedgerOffset]string{
  Genesis: "Genesis",
  AtOffset: "AtOffset",
}

func (offset LedgerOffset) String() string {
  return ledgerOffset[offset]
}

type TaggedOffset struct {
  Tag LedgerOffset
  Offset *string
}

type LedgerSettings struct {
  UserRightsMode PartyRightsMode
}

func NewLedgerSettings() (LedgerSettings) {
  return LedgerSettings {
    UserRightsMode: ONLY_READ_AS,
  }
}

type LedgerContext struct {
  GetConnection func()(ConnectionWrapper)
  GetConnectionWithoutTimeout func()(ConnectionWrapper)
  ApplicationId string
  LedgerId string
  Sandbox bool
  LedgerSettings LedgerSettings
}

type ConnectionWrapper struct {
  connection *grpc.ClientConn
  ctx *context.Context
  cancelCtx context.CancelFunc
}

type LedgerUser struct {
  ApplicationId string `json:"application_id"`
  Token string `json:"token"`
}

func CreateLedgerContextPerUser(connectionStr string, authTokens map[string]LedgerUser, sandbox bool) (map[string]LedgerContext) {
  lContextMap := make(map[string]LedgerContext)
  for k, v := range(authTokens) {
    ctx := CreateLedgerContext(connectionStr, v.Token, v.ApplicationId, sandbox)
    lContextMap[k] = ctx
  }
  return lContextMap
}

func CreateLedgerContext(connectionStr string, authToken string, applicationId string, sandbox bool) (LedgerContext) {
  return LedgerContext {
    GetConnection: func() (ConnectionWrapper) {
      ctx := context.Background()
      ctx, cancelCtx := context.WithTimeout(ctx, time.Second * 30)
      conn, err := grpc.DialContext(ctx, connectionStr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithUnaryInterceptor(GenTokenUnaryInterceptor(authToken)), grpc.WithStreamInterceptor(GenTokenStreamInterceptor(authToken)), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1024 * 1024 * 100)))
      if err != nil {
        log.Fatalf("did not connect")
      }
      return ConnectionWrapper {
        connection: conn,
        ctx: &ctx,
        cancelCtx: cancelCtx,
      }
    },
    GetConnectionWithoutTimeout: func() (ConnectionWrapper) {
      ctx := context.Background()
      conn, err := grpc.DialContext(ctx, connectionStr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithUnaryInterceptor(GenTokenUnaryInterceptor(authToken)), grpc.WithStreamInterceptor(GenTokenStreamInterceptor(authToken)), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1024 * 1024 * 100)))
      if err != nil {
        log.Fatalf("did not connect")
      }
      return ConnectionWrapper {
        connection: conn,
        ctx: &ctx,
        cancelCtx: nil,
      }
    },
    ApplicationId: applicationId,
    LedgerId: "",
    Sandbox: sandbox,
    LedgerSettings: NewLedgerSettings(),
  }
}


func (ledgerContext *LedgerContext) NoCloseConnection() (func()()) {
  connection := ledgerContext.GetConnectionWithoutTimeout()
  ledgerContext.GetConnectionWithoutTimeout = func()(ConnectionWrapper){
    return connection
  }
  ledgerContext.GetConnection = func()(ConnectionWrapper) {
    return connection
  }
  return func()() {
      connection.connection.Close()
  }
}

func (ledgerContext *LedgerContext) GetParties() (allParties []string) {
  if ledgerContext.Sandbox {
    return(ledgerContext.GetPartiesSandbox())
  } else {
    return(ledgerContext.GetPartiesFromUser())
  }
}

func (ledgerContext *LedgerContext) GetUser() {
  connection := ledgerContext.GetConnection()
  defer connection.cancelCtx()
  defer connection.connection.Close()

  client := admin.NewUserManagementServiceClient(connection.connection)
  client.GetUser(*connection.ctx, &admin.GetUserRequest{
    UserId: ledgerContext.ApplicationId,
  })
}

func (ledgerContext *LedgerContext) UploadDAR(dar []byte) {
  connection := ledgerContext.GetConnection()
  defer connection.cancelCtx()
  defer connection.connection.Close()


  submissionId := uuid.New()
  client := admin.NewPackageManagementServiceClient(connection.connection)
  _, err := client.UploadDarFile(*connection.ctx, &admin.UploadDarFileRequest {
    DarFile: dar,
    SubmissionId: fmt.Sprintf("strata-%s", submissionId),
  })

  if err != nil {
    panic("Failed to upload DAR")
  }
}


func (ledgerContext *LedgerContext) ListPackages() []string {
  connection := ledgerContext.GetConnection()
  defer connection.cancelCtx()
  defer connection.connection.Close()

  client := v1.NewPackageServiceClient(connection.connection)
  resp, err := client.ListPackages(*connection.ctx, &v1.ListPackagesRequest {
    LedgerId: ledgerContext.GetLedgerId(),
  })

  if err != nil { panic(err) }
  return resp.PackageIds
}



func (ledgerContext *LedgerContext) GetPartiesFromUser() (allParties []string) {
  connection := ledgerContext.GetConnection()
  defer connection.cancelCtx()
  defer connection.connection.Close()

  client := admin.NewUserManagementServiceClient(connection.connection)
  response, err := client.ListUserRights(*connection.ctx, &admin.ListUserRightsRequest{
    UserId: ledgerContext.ApplicationId,
  })
  if err != nil {
    panic(err)
  }

  var readAs []string
  parties := make(map[string]interface {})
  for _, v := range(response.Rights) {
    switch ledgerContext.LedgerSettings.UserRightsMode {
      case ONLY_READ_AS:
        if a := v.GetCanReadAs(); a != nil { parties[a.Party] = nil }
      case ONLY_ACT_AS:
        if a := v.GetCanActAs(); a != nil { parties[a.Party] = nil }
      case BOTH:
        if a := v.GetCanReadAs(); a != nil { parties[a.Party] = nil }
        if b := v.GetCanActAs(); b != nil { parties[b.Party] = nil }
    }
  }
  for k, _ := range(parties) {
    readAs = append(readAs, k)
  }
  return readAs
}

func (ledgerContext *LedgerContext) GetPartiesSandbox() (allParties []string) {
  connection := ledgerContext.GetConnection()
  defer connection.cancelCtx()
  defer connection.connection.Close()

  client := admin.NewPartyManagementServiceClient(connection.connection)
  response, err := client.ListKnownParties(*connection.ctx, &admin.ListKnownPartiesRequest{})
  if err != nil {
    log.Fatalf("%s", err)
  }
  var partyList []string
  for _, value := range response.PartyDetails {
    partyList = append(partyList, value.Party)
  }

  return partyList
}

func (ledgerContext *LedgerContext) GetLedgerId() string {
    connection := ledgerContext.GetConnection()
    defer connection.cancelCtx()
    defer connection.connection.Close()

    if ledgerContext.LedgerId != "" {
      return(ledgerContext.LedgerId)
    }

    client := v1.NewLedgerIdentityServiceClient(connection.connection)
    response, err := client.GetLedgerIdentity(*connection.ctx, &v1.GetLedgerIdentityRequest{})
    if err != nil {
      log.Fatalf("Failed to get ledger identity")
    }
    return response.LedgerId
}



func (ledgerContext *LedgerContext) SubmitAndWait(commands []*v1.Command) (error) {
  connection := ledgerContext.GetConnectionWithoutTimeout()

  ledgerId := ledgerContext.GetLedgerId()
  parties := ledgerContext.GetParties()

  client := v1.NewCommandServiceClient(connection.connection)

  uuid := uuid.New()

  _, err := client.SubmitAndWait(*connection.ctx, &v1.SubmitAndWaitRequest {
    Commands: &v1.Commands {
      LedgerId: ledgerId,
      WorkflowId: "strata",
      ApplicationId: ledgerContext.ApplicationId,
      CommandId: fmt.Sprintf("strata-%s", uuid),
      ActAs: parties,
      ReadAs: parties,
      Commands: commands,
    },
  })

  return err
}

func (ledgerContext *LedgerContext) SubmitAndWaitForEvents(commands []*v1.Command) (error, *[]*v1.Event) {
  connection := ledgerContext.GetConnectionWithoutTimeout()

  ledgerId := ledgerContext.GetLedgerId()
  parties := ledgerContext.GetParties()

  client := v1.NewCommandServiceClient(connection.connection)

  uuid := uuid.New()

  tx, err := client.SubmitAndWaitForTransaction(*connection.ctx, &v1.SubmitAndWaitRequest {
    Commands: &v1.Commands {
      LedgerId: ledgerId,
      WorkflowId: "strata",
      ApplicationId: ledgerContext.ApplicationId,
      CommandId: fmt.Sprintf("strata-%s", uuid),
      ActAs: parties,
      ReadAs: parties,
      Commands: commands,
    },
  })

  if err != nil {
    return err, nil
  }

  return err, &tx.Transaction.Events
}

func (ledgerContext *LedgerContext) ArchiveContract(command *v1.ExerciseCommand) {
  var commands []*v1.Command
  commands = append(commands, &v1.Command {
      Command: &v1.Command_Exercise {
          Exercise: command,
      },
  })
  err, _ := ledgerContext.SubmitAndWaitForEvents(commands)
  if err != nil {
    fmt.Printf("%s\n", command.ContractId)
    panic("Failed to archive contract")
  }
}

func (ledgerContext *LedgerContext) CreateContract(command *v1.CreateCommand) (error, *[]*v1.Event) {
  var commands []*v1.Command
  commands = append(commands, &v1.Command {
      Command: &v1.Command_Create {
          Create: command,
      },
  })

  err, events := ledgerContext.SubmitAndWaitForEvents(commands)
  return err, events
}

func (ledgerContext *LedgerContext) GetEventsByKey(templateId *v1.Identifier, key *v1.Value) (*v1.GetEventsByContractKeyResponse) {
  connection := ledgerContext.GetConnection()
  defer connection.cancelCtx()
  defer connection.connection.Close()

  parties := ledgerContext.GetParties()

  client := v1.NewEventQueryServiceClient(connection.connection)

  resp, _ := client.GetEventsByContractKey(*connection.ctx, &v1.GetEventsByContractKeyRequest {
    ContractKey: key,
    TemplateId: templateId,
    RequestingParties: parties,
  })

  return resp
}


func (ledgerContext *LedgerContext) GetActiveContractsWithCB(cb func(contract *v1.CreatedEvent)(), filter *v1.Filters) {
  connection := ledgerContext.GetConnectionWithoutTimeout()

  ledgerId := ledgerContext.GetLedgerId()
  parties := ledgerContext.GetParties()

  client := v1.NewActiveContractsServiceClient(connection.connection)
  partyMap := make(map[string]*v1.Filters)

  for _, value := range parties {
    partyMap[value] = filter
  }
  response, _ := client.GetActiveContracts(*connection.ctx, &v1.GetActiveContractsRequest{
    LedgerId: ledgerId,
    Filter: &v1.TransactionFilter{
      FiltersByParty: partyMap,
    },
    Verbose: true,
  })

  for {
    resp, err := response.Recv()
    if err == io.EOF {
      panic("Didn't get offset, invalid state, bailing!")
    }

    if err != nil {
      fmt.Printf("Failed to recieve repsone, bailing")
      panic(err)
    }

    if resp.Offset != "" {
      cb(nil)
      break
    }

    for _, value := range resp.ActiveContracts {
      cb(value)
    }
  }
}

func OffsetToLedgerOffset(taggedOffset TaggedOffset) (*v1.LedgerOffset) {
  var l_offset *v1.LedgerOffset
  switch (taggedOffset.Tag) {
    case Genesis:
      l_offset = &v1.LedgerOffset {
          Value: &v1.LedgerOffset_Boundary {
            Boundary: v1.LedgerOffset_LEDGER_BEGIN,
          },
      }
    case AtOffset:
      if taggedOffset.Offset == nil {
        l_offset = &v1.LedgerOffset {
          Value: &v1.LedgerOffset_Boundary {
            Boundary: v1.LedgerOffset_LEDGER_BEGIN,
          },
        }
      } else {
        l_offset = &v1.LedgerOffset {
          Value: &v1.LedgerOffset_Absolute {
            Absolute: *taggedOffset.Offset,
          },
        }
      }
  }

  return l_offset
}


func (ledgerContext *LedgerContext) GetTransactionTrees(taggedOffset TaggedOffset, cb func(transactionTree *v1.TransactionTree, errorState error)()) {
  offset := OffsetToLedgerOffset(taggedOffset)
  ledgerId := ledgerContext.GetLedgerId()

  var parties []string
  parties = ledgerContext.GetParties()

  partyMap := make(map[string]*v1.Filters)
  for _, value := range parties {
    partyMap[value] = &v1.Filters{}
  }

  connection := ledgerContext.GetConnectionWithoutTimeout()
  defer connection.connection.Close()


  client := v1.NewTransactionServiceClient(connection.connection)

  response, err := client.GetTransactionTrees(*connection.ctx, &v1.GetTransactionsRequest {
    LedgerId: ledgerId,
    Filter: &v1.TransactionFilter {
      FiltersByParty: partyMap,
    },
    Begin: offset,
    Verbose: true,
  })

  if err != nil {
    panic(err)
  }

  for {
    resp, err := response.Recv()
    if err != nil {
      cb(nil, err)
    }

    for _, tx := range(resp.Transactions) {
      cb(tx, nil)
    }
  }
}

type wrappedStream struct {
	grpc.ClientStream
}

func (w *wrappedStream) RecvMsg(m any) error {
	//logger("Receive a message (Type: %T) at %v", m, time.Now().Format(time.RFC3339))
	return w.ClientStream.RecvMsg(m)
}

func (w *wrappedStream) SendMsg(m any) error {
	//logger("Send a message (Type: %T) at %v", m, time.Now().Format(time.RFC3339))
	return w.ClientStream.SendMsg(m)
}

func newWrappedStream(s grpc.ClientStream) grpc.ClientStream {
	return &wrappedStream{s}
}

// streamInterceptor is an example stream interceptor.
func GenTokenStreamInterceptor(token string) func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
       nt := fmt.Sprintf("Bearer %s", token)
       ctx = metadata.AppendToOutgoingContext(ctx, "Authorization", nt)
	   s, err := streamer(ctx, desc, cc, method, opts...)
       if err != nil {
         return nil, err
       }
       return newWrappedStream(s), nil
    }
}


func GenTokenUnaryInterceptor(token string) func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
  return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
    nt := fmt.Sprintf("Bearer %s", token)
    ctx = metadata.AppendToOutgoingContext(ctx, "Authorization", nt)
	err := invoker(ctx, method, req, reply, cc, opts...)
	return err
  }
}
