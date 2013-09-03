package pq

import (
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"github.com/gregb/pq/oid"
	"log"
	"strconv"
	"time"
)

// Encoder is a function which turns a Go type into the string representation
// that postgres needs for the associated the column type
type Encoder func(interface{}) ([]byte, error)

type Decoder func([]byte) (interface{}, error)

var customEncoders = make(map[oid.Oid]Encoder)
var customDecoders = make(map[oid.Oid]Decoder)

func RegisterEncoder(typ oid.Oid, e Encoder) {
	customEncoders[typ] = e
}

func RegisterDecoder(typ oid.Oid, d Decoder) {
	customDecoders[typ] = d
}

func encode(x interface{}, typ oid.Oid) []byte {

	//log.Printf("Encoding item as oid %d: <%v>", typ, x)

	// prefer explicitly registered encoders over built ins
	encoder, ok := customEncoders[typ]
	if ok {
		encoded, err := encoder(x)
		if err != nil {
			// this method could really use an error return
			// i am pretty sure panicking is not a great thing to do here
			panic(err)
		}
		return encoded
	}

	switch v := x.(type) {
	case int64:
		return []byte(fmt.Sprintf("%d", v))
	case float32:
		return []byte(fmt.Sprintf("%f", v))
	case float64:
		return []byte(fmt.Sprintf("%g", v))
	case []byte:
		if typ == oid.T_bytea {
			return []byte(fmt.Sprintf("\\x%x", v))
		}
		return v
	case string:
		if typ == oid.T_bytea {
			return []byte(fmt.Sprintf("\\x%x", v))
		}
		return []byte(v)
	case bool:
		return []byte(fmt.Sprintf("%t", v))
	case time.Time:
		return []byte(v.Format(time.RFC3339Nano))
	default:
		errorf("encode: unknown type for %T", v)
	}

	panic("not reached")
}

func decode(s []byte, typ oid.Oid) interface{} {

	//log.Printf("Attempting to decode oid %d, value = <%s>", typ, string(s))
	// prefer explicitly registered codecs over built ins
	decoder, ok := customDecoders[typ]
	if ok {
		decoded, err := decoder(s)

		if err != nil {
			panic(err)
		}

		return decoded
	}

	if typ.IsArray() {
		// TODO: Cache by oid?  Creating the same thing all the time could be slow
		arrayConverter := &arrayConverter{ArrayTyp: typ}
		slice, err := arrayConverter.decode(s)

		if err != nil {
			panic(err)
		}

		return slice
	}

	switch typ {
	case oid.T_bytea:
		s = s[2:] // trim off "\\x"
		d := make([]byte, hex.DecodedLen(len(s)))
		_, err := hex.Decode(d, s)
		if err != nil {
			errorf("%s", err)
		}
		return d
	case oid.T_timestamptz:
		return mustParse("2006-01-02 15:04:05-07", typ, s)
	case oid.T_timestamp:
		return mustParse("2006-01-02 15:04:05", typ, s)
	case oid.T_time:
		return mustParse("15:04:05", typ, s)
	case oid.T_timetz:
		return mustParse("15:04:05-07", typ, s)
	case oid.T_date:
		return mustParse("2006-01-02", typ, s)
	case oid.T_bool:
		return s[0] == 't'
	case oid.T_int8, oid.T_int2, oid.T_int4:
		i, err := strconv.ParseInt(string(s), 10, 64)
		if err != nil {
			errorf("%s", err)
		}
		return i
	case oid.T_float4, oid.T_float8:
		bits := 64
		if typ == oid.T_float4 {
			bits = 32
		}
		f, err := strconv.ParseFloat(string(s), bits)
		if err != nil {
			errorf("%s", err)
		}
		return f
	case oid.T_point, oid.T_lseg, oid.T_line, oid.T_box, oid.T_circle, oid.T_path, oid.T_polygon:
		// Geometry types get turned into a []float64, for
		// further sql.Scan()-ing into the type of the user's choice
		floats, err := extractFloats(s)
		if err != nil {
			errorf("%s", err)
		}

		return floats
	case oid.T_varchar, oid.T_char:
		return string(s)
	}

	log.Printf("Leaving OID %d, value = %v as a []byte", typ, s)

	return s
}

func mustParse(f string, typ oid.Oid, s []byte) time.Time {
	str := string(s)

	// Special case until time.Parse bug is fixed:
	// http://code.google.com/p/go/issues/detail?id=3487
	if str[len(str)-2] == '.' {
		str += "0"
	}

	// check for a 30-minute-offset timezone
	if (typ == oid.T_timestamptz || typ == oid.T_timetz) &&
		str[len(str)-3] == ':' {
		f += ":00"
	}
	t, err := time.Parse(f, str)
	if err != nil {
		errorf("decode: %s", err)
	}
	return t
}

type NullTime struct {
	Time  time.Time
	Valid bool // Valid is true if Time is not NULL
}

// Scan implements the driver.Scanner interface.
func (nt *NullTime) Scan(value interface{}) error {
	nt.Time, nt.Valid = value.(time.Time)
	return nil
}

// Value implements the driver.Valuer interface.
func (nt NullTime) Value() (driver.Value, error) {
	if !nt.Valid {
		return nil, nil
	}
	return nt.Time, nil
}

// ExtractFloats extracts all floats from a string
// Parameter represents an ASCII string
// Returns a slice of all floats parsed out
// Returns an error if a float could not be parsed, plus all successfully parsed floats up until that point
func extractFloats(s []byte) ([]float64, error) {

	floats := make([]float64, 0, 4)

	start := 0
	inFloat := false

	for i, b := range s {

		// Float parts are 0 to 9, signs, and the decimal place
		isFloatPart := (b == '.') || (b == '+') || (b == '-') || (b >= '0' && b <= '9')

		if isFloatPart && !inFloat {
			// This char is the beginning of a float, mark it
			start = i
			inFloat = true
		}

		if !isFloatPart && inFloat {
			// The last char was the end of a float. parse it from where it started
			substr := string(s[start:i])
			f, err := strconv.ParseFloat(substr, 64)
			if err != nil {
				return floats, fmt.Errorf("Unable to parse %s as a float64", substr)
			}
			floats = append(floats, f)
			inFloat = false
		}

	}

	return floats, nil
}
