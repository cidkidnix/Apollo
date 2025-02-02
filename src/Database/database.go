package Database

import (
  "gorm.io/gorm"
  "gorm.io/driver/postgres"
  "fmt"
  pq "github.com/lib/pq"
  "time"
  "context"
  "gorm.io/gorm/logger"
  "github.com/rs/zerolog/log"
  "github.com/jackc/pgx/v5/pgxpool"
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
  OffsetIx uint64
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
   CreatedAt uint64
   ArchivedAt *uint64
   LifeIx uint64 `gorm:"->;type: int8range GENERATED ALWAYS AS (int8range(created_at, archived_at)) STORED;"`
}

func (CreatesTable) TableName() string {
  return "__creates"
}

type TransactionTable struct {
  TransactionId string `gorm:"index:transactions_idx,unique;primaryKey"`
  WorkflowId string
  CommandId string `gorm:"index_transactions_idx_command_id"`
  Offset string `gorm:"index:transactions_idx_offset,unique"` // offsets are unique
  OffsetIx uint64 `gorm:"index:transactions_idx_offset_ix,unique"`
  EventIds pq.StringArray `gorm:"type:text[]"`
  EffectiveAt time.Time `gorm:"type:timestamp with time zone"`
}

func (TransactionTable) TableName() string {
  return "__transactions"
}


// "Long Lived" application state
// We want to know when we started (STORE_BEGIN) and where we're at (STORE_END)
type Watermark struct {
  PK int `gorm:"primarykey;column:pk;index:watermark_pk,unique"`
  Offset string `gorm:"column:offset"`
  OffsetIx uint64 `gorm:"column:offset_ix"`
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
        SET archive_event = NEW.event_id, archived_at = NEW.offset_ix
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
        SET "offset" = NEW.offset, "offset_ix" = NEW.offset_ix
        WHERE pk = 1
        AND NEW."offset_ix" > "offset_ix";
      RETURN NULL;
    END;
    $$;
  `)

  db.Exec(`
    CREATE OR REPLACE TRIGGER update_watermark
    AFTER INSERT OR UPDATE OR DELETE ON __transactions
    FOR EACH ROW EXECUTE PROCEDURE __update_watermark()
  `)


  db.Exec(`
    CREATE OR REPLACE FUNCTION latest_offset()
      RETURNS text
      BEGIN ATOMIC
        select "offset" from __watermark LIMIT 1;
      END;
  `)

  db.Exec(`
    CREATE OR REPLACE FUNCTION __nearest_ix(off text default latest_offset())
      RETURNS BIGINT
      LANGUAGE plpgsql
      AS $$
        DECLARE
          value bigint;
        BEGIN
          select offset_ix INTO value from __transactions where "offset" = off
          ORDER BY "offset" DESC LIMIT 1;
          RETURN value;
        END;
      $$
  `)
  // Table view to show active contracts
  db.Exec(`
    CREATE OR REPLACE VIEW active AS
      SELECT __creates.contract_id, __creates.contract_key, __creates.payload, __creates.template_fqn, __creates.witnesses, __creates.observers, __creates.signatories
      FROM __creates
      WHERE __creates.life_ix @> __nearest_ix();
  `)

  // Table view to show all contracts we've seen
  db.Exec(`
    CREATE OR REPLACE VIEW __contracts AS
      SELECT __creates.contract_id, __creates.contract_key, __creates.payload, __creates.template_fqn, __creates.witnesses, __creates.observers, __creates.signatories
      FROM __creates
  `)

  db.Exec(`
    CREATE INDEX IF NOT EXISTS
        __creates_life_ix ON __creates USING gist(life_ix)
  `)

  db.Exec(`
    CREATE INDEX IF NOT EXISTS
       __creates_created_at_ix ON __creates USING btree(created_at)
  `)

  db.Exec(`
    CREATE INDEX IF NOT EXISTS
       __creates_archived_at_ix ON __creates USING btree(archived_at)
  `)

  db.Exec(`
    CREATE OR REPLACE FUNCTION history(start_offset bigint, end_offset bigint)
      RETURNS TABLE(contract_id text, offset_ix bigint, template_fqn text, choice text, payload jsonb)
      LANGUAGE plpgsql STABLE AS
      $$
        BEGIN
             RETURN QUERY SELECT
                foo.contract_id, foo.offset_ix, foo.template_fqn, foo.choice, foo.choice_argument
             FROM ((select __creates.contract_id, 'Create' as choice, __creates.payload as choice_argument, __creates.created_at as offset_ix, __creates.template_fqn from __creates WHERE int8range(start_offset, end_offset) @> __creates.created_at)
             UNION (select __exercised.contract_id, __exercised.choice, __exercised.choice_argument, __exercised.offset_ix, __exercised.template_fqn from __exercised WHERE int8range(start_offset, end_offset) @> __exercised.offset_ix))
             as foo ORDER BY offset_ix;
        END;
      $$

  `)

  db.Exec(`
    CREATE OR REPLACE FUNCTION active(template_id text default null, i_offset text default latest_offset())
      RETURNS SETOF __creates
      LANGUAGE plpgsql STABLE AS
      $$
        BEGIN
            RETURN QUERY
                SELECT * FROM __creates as c
            WHERE CASE
                WHEN template_id IS NOT NULL THEN c.template_fqn = template_id
                ELSE true
              END
            AND c.life_ix @> __nearest_ix(i_offset);
        END;
     $$
  `)


}

type DatabaseConnection struct {
  Host string `toml:"host"`
  User string `toml:"user"`
  Password string `toml:"password"`
  DBName string `toml:"dbname"`
  Port int `toml:"port"`
  SSLMode string `toml:"sslmode"`
}

func SetupDatabase(conn DatabaseConnection) (db *gorm.DB,  pool *pgxpool.Pool) {
  connStr := fmt.Sprintf(
    "host=%s user=%s password=%s dbname=%s port=%d sslmode=%s",
    conn.Host,
    conn.User,
    conn.Password,
    conn.DBName,
    conn.Port,
    conn.SSLMode,
  )

  db, _ = gorm.Open(postgres.Open(connStr), &gorm.Config{
    PrepareStmt: true,
    SkipDefaultTransaction: true,
    Logger: Logger{},
  })
  db.AutoMigrate(&TransactionTable{})
  db.AutoMigrate(&CreatesTable{})
  db.AutoMigrate(&ExercisedTable{})
  db.AutoMigrate(&Watermark{})
  SetupDatabaseProc(db)

  db.Exec(`
     DELETE FROM
        __exercised WHERE offset_ix
        NOT BETWEEN (select offset_ix from __transactions ORDER BY offset_ix LIMIT 1)
        AND (select offset_ix from __transactions ORDER BY offset_ix DESC LIMIT 1)
  `)
  db.Exec(`
     DELETE FROM __creates WHERE created_at
     NOT BETWEEN (select offset_ix from __transactions ORDER BY offset_ix LIMIT 1)
     AND (select offset_ix from __transactions ORDER BY offset_ix DESC LIMIT 1)
  `)
  config, _ := pgxpool.ParseConfig(connStr)
  runtimeParams := config.ConnConfig.RuntimeParams
  runtimeParams["application_name"] = "Apollo"
  //runtimeParams["keepalives_idle"] = "2"
  config.ConnConfig.RuntimeParams = runtimeParams
  t, _ := pgxpool.NewWithConfig(context.TODO(), config)
  return db, t
}

type Logger struct {}

func (l Logger) LogMode(logger.LogLevel) logger.Interface { return l }
func (l Logger) Error(ctx context.Context, msg string, opts ...interface{}) {
  log.Error().
      Ctx(ctx).
      Str("component", "database").
      Msg(fmt.Sprintf(msg, opts...))
}
func (l Logger) Warn(ctx context.Context, msg string, opts ...interface{}) {
  log.Warn().
      Ctx(ctx).
      Str("component", "database").
      Msg(fmt.Sprintf(msg, opts...))
}
func (l Logger) Info(ctx context.Context, msg string, opts ...interface{}) {
  log.Info().
      Ctx(ctx).
      Str("component", "database").
      Msg(fmt.Sprintf(msg, opts...))
}
func (l Logger) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
  _, ret2 := fc()

  log.Debug().
        Ctx(ctx).
        Int64("query_execution_time_ms", time.Since(begin).Milliseconds()).
        Int64("rows_affected", ret2).
        Str("component", "database").
        Send()

  //log.Trace().
  //      Ctx(ctx).
  //      Time("begin", begin).
  //      Str("sql", ret1).
  //      Int64("rows_affected", ret2).
  //      Err(err).
  //      Str("component", "gorm").
  //      Send()

}
