# Apollo

A Canton ODS (Operational Data Store) implementation. Currently Apollo only supports raw `Template` objects on the ledger, and currently does not filter or consume any `Interface` responses from the ledger

## Decoder / JSON Encoding
Currently Apollo aligns with the Official DAML-LF JSON serilization format. It deviates in the time format as currently Apollo uses RFC3339 instead of ISO8601

## Offset Tracking
Currently Apollo uses "Absolute" offsets turned into their Decimal representation for all offset index tracking


## Using Apollo's Database
Currently Apollo exposes a few functions for convience, for high-performance workloads the recommendation is to *not use* these SQL functions.

Instead you should explicitly use the `__creates` table to query any contract that apollo has seen

Alongside this Apollo currently does not support prepending it's datastore

### Resetting the database to an older offset
```sql
DELETE FROM __creates WHERE offset_ix > (my_reset_offset_ix)
```

```sql
DELETE FROM __exercised WHERE created_at > (my_reset_offset_ix)
```

```sql
DELETE FROM __transactions WHERE offset_ix > (my_reset_offset_ix)
```

```sql
UPDATE __watermark SET offset = 'my_reset_offset', offset_ix = 'my_reset_offset_ix' WHERE pk = 1
```


### Example Queries

Get Active Contracts
```sql
SELECT * FROM __creates WHERE life_ix @> (SELECT offset_ix FROM __watermark WHERE pk = 1)
```

Get All Contracts of one type
```sql
SELECT * FROM __creates WHERE template_fqn = 'my_fully:qualified:template'
```

Get History between 2 ledger offsets that Apollo is aware of
```sql
SELECT * FROM history(<offset_ix_1>, <offset_ix_2>)
```

Get Exercised Events of a particular template
```sql
SELECT * FROM __exercised WHERE template_fqn = 'my_fully:qualified:template'
```


### Adding Indexes
All self-provisioned indexes should be *NON-UNIQUE* to prevent any errors in the database writer

Otherwise All manual indexes should be added on the `__creates` table

#### Example Indexes
Creating an index for a particular field across *all* contracts
```sql
CREATE INDEX my_custom_index ON __creates USING gin ((payload->'my'->'custom'->>'path'))
```

Creating and index for a particular field across *one* template type
```sql
CREATE INDEX my_custom_index ON __creates USING gin ((payload->'my'->'custom'->>'path')) WHERE template_fqn = 'my_fully:qualified:template'
```

## Configuration

### Ledger Starting Point
Currently Apollo doesn't check where the start or the end of the ledger is,
be aware of this as it may cause Apollo to crash upon contacting canton if an
invalid offset is passed to the participant.

Apollo also currently does not support bringing up it's datastore from `ACS`

```
offset = "GENESIS" # Also supports "OLDEST" or providing an absolute offset
```


### Ledger Connection
```toml
[ledger]
host = "ledger_host"
port = 4001
```

### Max Transactions to Pull from the internal queue
Currently Apollo uses a shared Queue between the ledger reader and the
database writer, it is currently *guaranteed* that Apollo will write all offsets recieved in order.
This config option is to adjust how many transactions can be pulled out of the queue on the database writer side. Generally this should be up in the thousands to prevent starving the database writer.
```
tx_max_pull = 1000
```

### Ledger Authentication
Currently Apollo supports OAuth, "Manual" (user-defined JWT token) and "Sandbox" ledger authentication

#### OAuth
```toml
[oauth]
url = "oauth endpoint"
client_id = "<your_client_id>"
client_secret = "<your_client_secret>"
grant_type = "client_credentials"
```

Apollo also supports adding "extra parameters" into the Oauth POST requests:
```toml
[oauth.extra_params]
scope = "my_custom_scope"
my_custom_option = "custom_option_setting"
```

#### Manual
Ensure that you have no Oauth settings set, and then provide Apollo with the following
```toml
[user]
auth_token = "<jwt_token>"
application_id = "<user_id on ledger>"
timeout = "<time until Apollo restarts stream>"
```

#### Sandbox
```toml
sandbox = true

[user]
auth_token = ""
application_id = "daml-script"
timeout = 10000000
```


### Database Configuration

Base Database configuration:
```toml
[database]
host = "localhost"
user = "user"
password = "password"
dbname = "apollo"
port = 5432
sslmode = "disable"
max_connections = 24
min_connections = 8
```

Batch Size Configuration for DB `COPY`'s
```toml
[batch_size]
create = 200 # use -1 to *never* batch this particular event type
exercise = 200
transaction = 200
```


### Template Filtering
```toml
[filters.template_filter]
ignore_modules = [ "My.Ignored.Module" ]
ignore_packages = [ "my-ignored-package-id" ]
ignore_entities = [ "MyIgnoredTemplate" ]
```


### Garbage Collection (GC) Settings
Currently Apollo has a few options to tune how often and when the GC is ran
```toml
[gc]
allow_manual_run = false # Allow Apollo to manually run GC cycles
allow_manual_pause = true # Allow Apollo to manually pause the GC while doing intensive tasks
manual_offset_max = 1000 # How many offsets can Apollo pull before it manually runs GC, this only works with `allow_manual_run` set to `true`
memory_limit = 4096 # Amount of memory Apollo can use in MB
```
