package pq

import (
	"fmt"
	"github.com/lib/pq/oid"
	"testing"
	"time"
)

func TestScanTimestamp(t *testing.T) {
	var nt NullTime
	tn := time.Now()
	nt.Scan(tn)
	if !nt.Valid {
		t.Errorf("Expected Valid=false")
	}
	if nt.Time != tn {
		t.Errorf("Time value mismatch")
	}
}

func TestScanNilTimestamp(t *testing.T) {
	var nt NullTime
	nt.Scan(nil)
	if nt.Valid {
		t.Errorf("Expected Valid=false")
	}
}

func TestTimestampWithTimeZone(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	_, err = tx.Exec("create temp table test (t timestamp with time zone)")
	if err != nil {
		t.Fatal(err)
	}

	// try several different locations, all included in Go's zoneinfo.zip
	for _, locName := range []string{
		"UTC",
		"America/Chicago",
		"America/New_York",
		"Australia/Darwin",
		"Australia/Perth",
	} {
		loc, err := time.LoadLocation(locName)
		if err != nil {
			t.Logf("Could not load time zone %s - skipping", locName)
			continue
		}

		// Postgres timestamps have a resolution of 1 microsecond, so don't
		// use the full range of the Nanosecond argument
		refTime := time.Date(2012, 11, 6, 10, 23, 42, 123456000, loc)
		_, err = tx.Exec("insert into test(t) values($1)", refTime)
		if err != nil {
			t.Fatal(err)
		}

		for _, pgTimeZone := range []string{"US/Eastern", "Australia/Darwin"} {
			// Switch Postgres's timezone to test different output timestamp formats
			_, err = tx.Exec(fmt.Sprintf("set time zone '%s'", pgTimeZone))
			if err != nil {
				t.Fatal(err)
			}

			var gotTime time.Time
			row := tx.QueryRow("select t from test")
			err = row.Scan(&gotTime)
			if err != nil {
				t.Fatal(err)
			}

			if !refTime.Equal(gotTime) {
				t.Errorf("timestamps not equal: %s != %s", refTime, gotTime)
			}
		}

		_, err = tx.Exec("delete from test")
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestTimestampWithOutTimezone(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	test := func(ts, pgts string) {
		r, err := db.Query("SELECT $1::timestamp", pgts)
		if err != nil {
			t.Fatalf("Could not run query: %v", err)
		}

		n := r.Next()

		if n != true {
			t.Fatal("Expected at least one row")
		}

		var result time.Time
		err = r.Scan(&result)
		if err != nil {
			t.Fatalf("Did not expect error scanning row: %v", err)
		}

		expected, err := time.Parse(time.RFC3339, ts)
		if err != nil {
			t.Fatalf("Could not parse test time literal: %v", err)
		}

		if !result.Equal(expected) {
			t.Fatalf("Expected time to match %v: got mismatch %v",
				expected, result)
		}

		n = r.Next()
		if n != false {
			t.Fatal("Expected only one row")
		}
	}

	test("2000-01-01T00:00:00Z", "2000-01-01T00:00:00")

	// Test higher precision time
	test("2013-01-04T20:14:58.80033Z", "2013-01-04 20:14:58.80033")
}

func TestStringWithNul(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	hello0world := string("hello\x00world")
	_, err := db.Query("SELECT $1::text", &hello0world)
	if err == nil {
		t.Fatal("Postgres accepts a string with nul in it; " +
			"injection attacks may be plausible")
	}
}

func TestByteToText(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	b := []byte("hello world")
	row := db.QueryRow("SELECT $1::text", b)

	var result []byte
	err := row.Scan(&result)
	if err != nil {
		t.Fatal(err)
	}

	if string(result) != string(b) {
		t.Fatalf("expected %v but got %v", b, result)
	}
}

// Does not access database, simply tests the parser
func Test_DecodeArrayString(t *testing.T) {

	good := []string{
		"{}",
		"{1}",
		"{1,2}",
		"{1,2,3}",
		"{A}",
		"{A,B}",
		"{\"A\"}",                                 // {A,B}
		"{\"A\",B,\"\"}",                          // {"A",B,""}
		"{\"A\",\"B\"}",                           // {"A","B"}
		"{A,\"B\",\"Last word \\\"quoted\\\"\"}",  // {A,"B","Last word "quoted""}
		"{\"A\",\"More, {special}; chars\\\\ \"}", // "{"A","More, {special}; chars\\ "}"
	}

	expected := [][]string{
		{},
		{"1"},
		{"1", "2"},
		{"1", "2", "3"},
		{"A"},
		{"A", "B"},
		{"A"},
		{"A", "B", ""},
		{"A", "B"},
		{"A", "B", "Last word \"quoted\""},
		{"A", "More, {special}; chars\\ "},
	}

	for testNum, input := range good {
		iface, err := decodeArray([]byte(input), oid.T__varchar)

		if err != nil {
			t.Error(err)
		}

		results := iface.([]string) // we know this because we passed in oid.T__varchar

		if len(results) != len(expected[testNum]) {
			t.Errorf("For input <%s>, expected length %d, got %d <%v>", input, len(expected[testNum]), len(results), results)

		} else {

			for elementNum, resultBytes := range results {
				result := string(resultBytes)
				ex := expected[testNum][elementNum]
				if result != ex {
					t.Errorf("For input <%s> element %d, expected <%v>, got <%v>", input, elementNum, ex, result)
				}

			}
		}
	}
}

func Test_DecodeVarcharArrayFromDb(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	expectedArray := []string{"", "A", "B", `"C" quoted, with whitespace and delimiters`}
	gotArray := make([]string, 0)

	q := `SELECT '{"",A,"B","\"C\" quoted, with whitespace and delimiters"}'::varchar[]`
	row, err := db.Query(q)

	if err != nil {
		t.Fatal(err)
	}

	if !row.Next() {
		t.Fatal("Expected at least one row")
	}

	err = row.Scan(&gotArray)

	if err != nil {
		t.Fatal(err)
	}

	if len(gotArray) != len(expectedArray) {
		t.Errorf("Expected %d array elements, got %d", len(expectedArray), len(gotArray))
	}

	for i, v := range gotArray {
		if v != expectedArray[i] {
			t.Errorf("Error in element %d; expected %s, got %s", i, expectedArray[i], v)
		}
	}

}

func Test_DecodeInt64ArrayFromDb(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	expectedArray := []int64{1, 2, 3}
	gotArray := make([]int64, 0)

	q := `SELECT '{1,2,3}'::bigint[]`
	row, err := db.Query(q)

	if err != nil {
		t.Fatal(err)
	}

	if !row.Next() {
		t.Fatal("Expected at least one row")
	}

	err = row.Scan(&gotArray)

	if err != nil {
		t.Fatal(err)
	}

	if len(gotArray) != len(expectedArray) {
		t.Errorf("Expected %d array elements, got %d", len(expectedArray), len(gotArray))
	}

	for i, v := range gotArray {
		if v != expectedArray[i] {
			t.Errorf("Error in element %d; expected %d, got %d", i, expectedArray[i], v)
		}
	}
}

func Test_ArrayRoundtrip(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	expectedArray := []string{
		"NULL",
		"",
		"A",
		"\"B quoted\"",
		"C, with commas",
	}
	gotArray := make([]string, 0)

	row, err := db.Query("SELECT $1::varchar[]", &expectedArray)

	if err != nil {
		t.Fatal(err)
	}

	if !row.Next() {
		t.Fatal("Expected at least one row")
	}
	err = row.Scan(&gotArray)

	if err != nil {
		t.Fatal(err)
	}

	if len(gotArray) != len(expectedArray) {
		t.Errorf("Expected %d array elements, got %d", len(expectedArray), len(gotArray))
	}

	for i, v := range gotArray {
		if v != expectedArray[i] {
			t.Errorf("Error in element %d; expected %d, got %d", i, expectedArray[i], v)
		}
	}
}
