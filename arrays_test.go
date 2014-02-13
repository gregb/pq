package pq

import (
	"github.com/gregb/pq/oid"
	"testing"
)

// Does not access database, simply tests the parser
func TestDecodeArrayString(t *testing.T) {

	good := []string{
		"{}",
		"{1}",
		"{1,2}",
		"{1,2,3}",
		"{A}",
		"{A,B}",
		`{"A"}`,
		`{"A",B,""}`,
		`{"A","B"}`,
		`{A,"B","Last word \"quoted\""}`,
		`{"A","More, {special}; chars\ "}`,
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

	ac := arrayConverter{ArrayTyp: oid.T__varchar}

	for testNum, input := range good {
		iface, err := ac.decode([]byte(input))

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

func TestDecodeVarcharArrayFromDb(t *testing.T) {
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

func TestDecodeInt64ArrayFromDb(t *testing.T) {
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

func TestStringArrayRoundtrip(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	expectedArray := []string{
		"NULL",
		"",
		"A",
		"\"B quoted\"",
		"C, with commas",
		"internal and end \twhitespace ",
		"unicode snowman: \u2603",
		"€2.00",
		"whitespace\u2029using\u2009various\u2003unicode\u0009characters\u2028Next Line",
	}

	row, err := db.Query("SELECT $1::varchar[] as testColName", &expectedArray)

	if err != nil {
		t.Fatal(err)
	}

	if !row.Next() {
		t.Fatal("Expected at least one row")
	}

	gotArray := make([]string, 0)
	err = row.Scan(&gotArray)

	if err != nil {
		t.Fatal(err)
	}

	if len(gotArray) != len(expectedArray) {
		t.Fatalf("Expected %d array elements, got %d", len(expectedArray), len(gotArray))
	}

	for i, v := range gotArray {
		if v != expectedArray[i] {
			t.Errorf("Error in element %d; expected %s, got %s", i, expectedArray[i], v)
		}
	}
}

func TestIntArrayRoundtrip(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	expectedArray := []int64{
		0, 1, 2000, 3000000, 4000000000,
	}

	row, err := db.Query("SELECT $1::bigint[] as testColName", &expectedArray)

	if err != nil {
		t.Fatal(err)
	}

	if !row.Next() {
		t.Fatal("Expected at least one row")
	}

	gotArray := make([]int64, 0)
	err = row.Scan(&gotArray)

	if err != nil {
		t.Fatal(err)
	}

	if len(gotArray) != len(expectedArray) {
		t.Fatalf("Expected %d array elements, got %d", len(expectedArray), len(gotArray))
	}

	for i, v := range gotArray {
		if v != expectedArray[i] {
			t.Errorf("Error in element %d; expected %d, got %d", i, expectedArray[i], v)
		}
	}
}

func TestFloatArrayRoundtrip(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	expectedArray := []float64{
		1.1,               // simple
		-2,                // negative
		3.1415927,         // pi!
		123456789e10,      // scientific
		0.000000000000001, // smallest possible value that can be parsed in a float64
		999999999999999,   // largest parseable float64
	}

	row, err := db.Query("SELECT $1::float8[] as testColName", &expectedArray)

	if err != nil {
		t.Fatal(err)
	}

	if !row.Next() {
		t.Fatal("Expected at least one row")
	}

	gotArray := make([]float64, 0)
	err = row.Scan(&gotArray)

	if err != nil {
		t.Fatal(err)
	}

	if len(gotArray) != len(expectedArray) {
		t.Fatalf("Expected %d array elements, got %d", len(expectedArray), len(gotArray))
	}

	for i, v := range gotArray {
		if v != expectedArray[i] {
			t.Errorf("Error in element %d; expected %f, got %f", i, expectedArray[i], v)
		}
	}
}
