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

var testGenMap = &v1.Value {
  Sum: &v1.Value_GenMap{
    GenMap: &v1.GenMap{
      Entries: []*v1.GenMap_Entry{
        { Key: testString, Value: &v1.Value{ Sum: &v1.Value_Unit{} }, },
      },
    },
  },
}

var testGenMapWithRecord = &v1.Value{
   Sum: &v1.Value_GenMap{
    GenMap: &v1.GenMap{
      Entries: []*v1.GenMap_Entry{
        { Key: testString, Value: testRecord, },
      },
    },
  },
}

var testGenMapWithRecordKey = &v1.Value{
   Sum: &v1.Value_GenMap{
    GenMap: &v1.GenMap{
      Entries: []*v1.GenMap_Entry{
        { Key: testRecord, Value: testString, },
      },
    },
  },
}

var testMap = &v1.Value{
  Sum: &v1.Value_Map{
    Map: &v1.Map{
      Entries: []*v1.Map_Entry{
        { Key: "test_me", Value: testString },
        { Key: "test_me_2", Value: testString },
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
  if val == nil { panic("Value cannot be nil!") }
  return &v1.Value {
     Sum: &v1.Value_Optional {
       Optional: &v1.Optional {
         Value: val,
       },
     },
   }
}

func TestMap(t *testing.T) {
  expectedJSON := `{"test_me":"test string","test_me_2":"test string"}`
  parsedData := Apollo.ParseLedgerDataInternal(nil, testMap, 0)
  val, _ := json.Marshal(parsedData)
  if string(val) != expectedJSON {
    fmt.Println(string(val))
    t.Fatalf("Serialized value not equal to expected value!")
  }
}

func TestGenMap(t *testing.T) {
  expectedJSON := `[["test string",{}]]`
  parsedData := Apollo.ParseLedgerDataInternal(nil, testGenMap, 0)
  val, _ := json.Marshal(parsedData)
  if string(val) != expectedJSON {
    fmt.Println(string(val))
    t.Fatalf("Serialized value not equal to expected value!")
  }
}

func TestGenMapWithRecord(t *testing.T) {
  expectedJSON := `[["test string",{"test_member":"test string","test_member_2":"test string"}]]`
  parsedData := Apollo.ParseLedgerDataInternal(nil, testGenMapWithRecord, 0)
  val, _ := json.Marshal(parsedData)
  if string(val) != expectedJSON {
    fmt.Println(string(val))
    t.Fatalf("Serialized value not equal to expected value!")
  }
}

func TestGenMapWithRecordKey(t *testing.T) {
  expectedJSON := `[[{"test_member":"test string","test_member_2":"test string"},"test string"]]`
  parsedData := Apollo.ParseLedgerDataInternal(nil, testGenMapWithRecordKey, 0)
  val, _ := json.Marshal(parsedData)
  if string(val) != expectedJSON {
    fmt.Println(string(val))
    t.Fatalf("Serialized value not equal to expected value!")
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
