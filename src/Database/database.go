package Database

import (
  "gorm.io/gorm"
  "gorm.io/driver/postgres"
  "fmt"
  pq "github.com/lib/pq"
)


type ExercisedTable struct {
  EventID string `gorm:"index:exercised_event_idx,unique;primaryKey"`
  ContractID string `gorm:"index:exercised_event_contractid_idx"`
  TemplateFqn string `gorm:"index:exercised_event_template_idx"`
  Choice string `gorm:"index:exercised_event_choice_idx"`
  ChoiceArgument []byte `gorm:"type:jsonb"`
  ActingParties pq.StringArray `gorm:"type:text[]"`
  Consuming bool `gorm:"exercised_event_consuming"`
  ChildEventIds pq.StringArray `gorm:"type:text[]"`
  Witnesses pq.StringArray `gorm:"type:text[]"`
  ExerciseResult []byte `gorm:"type:jsonb"`
}

func (ExercisedTable) TableName() string {
  return "__exercised"
}

type CreatesTable struct {
   EventID string `gorm:"index:creates_event_id,unique;column:event_id`
   ContractID string `gorm:"index:creates_idx_contract_id,unique"`
   ContractKey []byte `gorm:"type:jsonb"`
   CreateArguments []byte `gorm:"type:jsonb;column:payload"`
   TemplateFqn string `gorm:"index:contracts_idx_template_fqn"`
   Witnesses pq.StringArray `gorm:"type:text[]"`
   Observers pq.StringArray `gorm:"type:text[]"`
   Signatories pq.StringArray `gorm:"type:text[]"`
   ArchiveEvent *string `gorm:"index:archive_event_idx,column:archive_event"`
}

func (CreatesTable) TableName() string {
  return "__creates"
}

type TransactionTable struct {
  TransactionId string `gorm:"index:transactions_idx,unique;primaryKey"`
  WorkflowId string
  CommandId string `gorm:"index_transactions_idx_command_id"`
  Offset string `gorm:"index:transactions_idx_offset,unique"` // offsets are unique
  EventIds pq.StringArray `gorm:"type:text[]"`
}

func (TransactionTable) TableName() string {
  return "__transactions"
}


// "Long Lived" application state
// We want to know when we started (STORE_BEGIN) and where we're at (STORE_END)
type Watermark struct {
  PK int `gorm:"primarykey;column:pk;index:watermark_pk,unique"`
  Offset string `gorm:"column:offset"`
  InitialOffset string `gorm:"column:initial_offset"`
  InstanceID string `gorm:"column:instance_id"`
}

func (Watermark) TableName() string {
  return "__watermark"
}

func SetupDatabaseProc(db *gorm.DB) {
  // Function and trigger to automatically provision the
  // correct value after a contract has been archived
  db.Exec(`
    CREATE OR REPLACE FUNCTION __update_archived()
      RETURNS TRIGGER
      LANGUAGE PLPGSQL
    AS $$
    BEGIN
      UPDATE __creates
        SET archive_event = NEW.event_id
        WHERE contract_id = NEW.contract_id;
      RETURN NULL;
    END;
    $$
  `)

  db.Exec(`
    CREATE OR REPLACE TRIGGER update_archived
    AFTER INSERT OR UPDATE ON __exercised
    FOR EACH ROW WHEN (NEW.consuming) EXECUTE PROCEDURE __update_archived()
  `)

  // Function and trigger to automatically update the __watermark table after
  // a transaction is emitted
  db.Exec(`
    CREATE OR REPLACE FUNCTION __update_watermark()
      RETURNS TRIGGER
      LANGUAGE PLPGSQL
    AS $$
    BEGIN
      UPDATE __watermark
        SET "offset" = NEW.offset
        WHERE pk = 1;
      RETURN NULL;
    END;
    $$;
  `)

  db.Exec(`
    CREATE OR REPLACE TRIGGER update_watermark
    AFTER INSERT OR UPDATE OR DELETE ON __transactions
    FOR EACH ROW EXECUTE PROCEDURE __update_watermark()
  `)

  // Table view to show active contracts
  db.Exec(`
    CREATE OR REPLACE VIEW active AS
      SELECT __creates.contract_id, __creates.contract_key, __creates.payload, __creates.template_fqn, __creates.witnesses, __creates.observers, __creates.signatories
      FROM __creates
      WHERE archive_event IS NULL;
  `)

  // Table view to show all contracts we've seen
  db.Exec(`
    CREATE OR REPLACE VIEW __contracts AS
      SELECT __creates.contract_id, __creates.contract_key, __creates.payload, __creates.template_fqn, __creates.witnesses, __creates.observers, __creates.signatories
      FROM __creates
  `)
}

type DatabaseConnection struct {
  Host string
  User string
  Password string
  DBName string
  Port int
  SSLMode string
}

func SetupDatabase(conn DatabaseConnection) (db *gorm.DB) {
  connStr := fmt.Sprintf(
    "host=%s user=%s password=%s dbname=%s port=%d sslmode=%s",
    conn.Host,
    conn.User,
    conn.Password,
    conn.DBName,
    conn.Port,
    conn.SSLMode,
  )

  db, _ = gorm.Open(postgres.Open(connStr), &gorm.Config{})
  db.AutoMigrate(&TransactionTable{})
  db.AutoMigrate(&CreatesTable{})
  db.AutoMigrate(&ExercisedTable{})
  db.AutoMigrate(&Watermark{})
  SetupDatabaseProc(db)
  return db
}
