package pq

import (
	"testing"
)

func TestSimpleParseURL(t *testing.T) {
	expected := "host=hostname.remote"
	str, err := ParseURL("postgres://hostname.remote")
	if err != nil {
		t.Error(err)
	}

	if str != expected {
		t.Errorf("unexpected result from ParseURL:\n+ %v\n- %v", str, expected)
	}
}

func TestFullParseURL(t *testing.T) {
	expected := "dbname=database host=hostname.remote password=secret port=1234 user=username"
	str, err := ParseURL("postgres://username:secret@hostname.remote:1234/database")
	if err != nil {
		t.Error(err)
	}

	if str != expected {
		t.Errorf("unexpected result from ParseURL:\n+ %s\n- %s", str, expected)
	}
}

func TestInvalidProtocolParseURL(t *testing.T) {
	_, err := ParseURL("http://hostname.remote")
	switch err {
	case nil:
		t.Error("Expected an error from parsing invalid protocol")
	default:
		msg := "invalid connection protocol: http"
		if err.Error() != msg {
			t.Errorf("Unexpected error message:\n+ %s\n- %s",
				err.Error(), msg)
		}
	}
}

func TestMinimalURL(t *testing.T) {
	cs, err := ParseURL("postgres://")
	if err != nil {
		t.Error(err)
	}

	if cs != "" {
		t.Errorf("expected blank connection string, got: %q", cs)
	}
}

func TestParseMap(t *testing.T) {

	m := map[string]interface{}{
		"ssl-mode": "disable",
		"host":     "localhost",
		"port":     5432,
	}

	expected := "host=localhost port=5432 ssl-mode=disable"

	paramString, err := ParseMap(m)
	if err != nil {
		t.Error(err)
	}

	if paramString != expected {
		t.Errorf("expected %s, got %s", expected, paramString)
	}
}

func TestAccrue(t *testing.T) {

	strings := new(kvs)

	strings.accrue("a", "b")
	expected := "a=b"

	result := strings.String()

	if result != expected {
		t.Errorf("expected %s, got %s", expected, result)
	}

}
