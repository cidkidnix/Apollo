package parser_test

import (
  "Apollo/src/Apollo"
  "testing"
  "github.com/digital-asset/dazl-client/v7/go/api/com/daml/ledger/api/v1"
  "encoding/json"
  "fmt"
  "github.com/rs/zerolog"
  "os"
)

var optionalNone = &v1.Value {
  Sum: &v1.Value_Optional {
    Optional: &v1.Optional {
      Value: nil,
    },
  },
}

var testString = &v1.Value {
  Sum: &v1.Value_Text {
    Text: "test string",
  },
}

var testList = &v1.Value {
  Sum: &v1.Value_List {
    List: &v1.List {
      Elements: []*v1.Value{
        testString,
      },
    },
  },
}

var testRecord = &v1.Value {
  Sum: &v1.Value_Record{
    Record: &v1.Record {
      Fields: []*v1.RecordField{
        &v1.RecordField{
          Label: "test_member",
          Value: testString,
        },
        &v1.RecordField{
          Label: "test_member_2",
          Value: testString,
        },
      },
    },
  },
}

func TestMain(m *testing.M) {
  zerolog.SetGlobalLevel(zerolog.InfoLevel)
  code := m.Run()
  os.Exit(code)
}


func OptionalSome(val *v1.Value) *v1.Value {
  if value == nil { panic("Value cannot be nil!") }
  return &v1.Value {
     Sum: &v1.Value_Optional {
       Optional: &v1.Optional {
         Value: val,
       },
     },
   }
}


func TestRecord(t *testing.T) {
  expectedJSON := `{"test_member":"test string","test_member_2":"test string"}`
  parsedData := Apollo.ParseLedgerDataInternal(nil, testRecord, 0)
  val, _ := json.Marshal(parsedData)

  if string(val) != expectedJSON {
    fmt.Println(string(val))
    t.Fatalf("Serialized value not equal to expected value!")
  }
}

func TestText(t *testing.T) {
  expectedJSON := `"test string"`
  parsedData := Apollo.ParseLedgerDataInternal(nil, testString, 0)
  val, _ := json.Marshal(parsedData)

  if string(val) != expectedJSON {
    fmt.Println(string(val))
    t.Fatalf("Serialized value not equal to expected value!")
  }
}

func TestList(t *testing.T) {
  expectedJSON := `["test string"]`
  parsedData := Apollo.ParseLedgerDataInternal(nil, testList, 0)
  val, _ := json.Marshal(parsedData)

  if string(val) != expectedJSON {
    fmt.Println(string(val))
    t.Fatalf("Serialized value not equal to expected value!")
  }
}


// Optional Tests
func TestOptionalNonNested(t *testing.T) {
  expectedJSON := "null"

  parsedData :=Apollo.ParseLedgerDataInternal(nil, optionalNone, 0)

  val, _ := json.Marshal(parsedData)

  if string(val) != expectedJSON {
    fmt.Println(string(val))
    t.Fatalf("Serialized value not equal to expected value!")
  }

}

func TestOptionalNested(t *testing.T) {
  expectedJSON := "[]"

  parsedData := Apollo.ParseLedgerDataInternal(nil, OptionalSome(optionalNone), 0)

  val, _ := json.Marshal(parsedData)

  if string(val) != expectedJSON {
    fmt.Println(string(val))
    t.Fatalf("Serialized value not equal to expected value!")
  }
}

func TestOptionalMoreNested(t *testing.T) {
  expectedJSON := "[[]]"

  parsedData := Apollo.ParseLedgerDataInternal(nil, OptionalSome(OptionalSome(optionalNone)), 0)

  val, _ := json.Marshal(parsedData)
  if string(val) != expectedJSON {
    fmt.Println(string(val))
    t.Fatalf("Serialized value not equal to expected value!")
  }
}

func TestOptionalText(t *testing.T) {
  expectedJSON := "\"test string\""
  parsedData := Apollo.ParseLedgerDataInternal(nil, OptionalSome(testString), 0)

  val, _ := json.Marshal(parsedData)
  if string(val) != expectedJSON {
    fmt.Println(string(val))
    t.Fatalf("Serialized value not equal to expected value!")
  }
}

func TestOptionalRecord(t *testing.T) {
  expectedJSON := `{"test_member":"test string","test_member_2":"test string"}`
  parsedData := Apollo.ParseLedgerDataInternal(nil, OptionalSome(testRecord), 0)

  val, _ := json.Marshal(parsedData)
  if string(val) != expectedJSON {
    fmt.Println(string(val))
    t.Fatalf("Serialized value not equal to expected value!")
  }
}

func TestNestedOptionalRecord(t *testing.T) {
  expectedJSON := `[{"test_member":"test string","test_member_2":"test string"}]`
  parsedData := Apollo.ParseLedgerDataInternal(nil, OptionalSome(OptionalSome(testRecord)), 0)

  val, _ := json.Marshal(parsedData)
  if string(val) != expectedJSON {
    fmt.Println(string(val))
    t.Fatalf("Serialized value not equal to expected value!")
  }
}

func TestOptionalList(t *testing.T) {
  expectedJSON := `["test string"]`
  parsedData := Apollo.ParseLedgerDataInternal(nil, OptionalSome(testList), 0)

  val, _ := json.Marshal(parsedData)
  if string(val) != expectedJSON {
    fmt.Println(string(val))
    t.Fatalf("Serialized value not equal to expected value!")
  }
}

func TestNestedOptionalList(t *testing.T) {
  expectedJSON := `[["test string"]]`

  parsedData := Apollo.ParseLedgerDataInternal(nil, OptionalSome(OptionalSome(testList)), 0)

  val, _ := json.Marshal(parsedData)
  if string(val) != expectedJSON {
    fmt.Println(string(val))
    t.Fatalf("Serialized value not equal to expected value!")
  }
}
