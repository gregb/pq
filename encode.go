package pq

import (
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"github.com/lib/pq/oid"
	"log"
	"reflect"
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

	log.Printf("Encoding item as oid %d: <%v>", typ, x)

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

	if oid.IsArray(typ) {
		log.Printf("Encoding slice of oid %d", typ)
		encoded, err := encodeArray(x)

		if err != nil {
			panic(err)
		}

		return encoded
	}

	switch v := x.(type) {
	case int64:
		return []byte(fmt.Sprintf("%d", v))
	case float32, float64:
		return []byte(fmt.Sprintf("%f", v))
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

	log.Printf("Attempting to decode oid %d, value = <%s>", typ, string(s))
	// prefer explicitly registered codecs over built ins
	decoder, ok := customDecoders[typ]
	if ok {
		decoded, err := decoder(s)

		if err != nil {
			panic(err)
		}

		return decoded
	}

	if oid.IsArray(typ) {
		slice, err := decodeArray(s, typ)

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
		// Geometry types get turned into a []float64, for
		// further sql.Scan()-ing into the type of the user's choice
	case oid.T_point, oid.T_lseg, oid.T_line, oid.T_box, oid.T_circle, oid.T_path, oid.T_polygon:
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

// Parses arrays retuned from postgres according to the docs at
// http://www.postgresql.org/docs/9.2/static/arrays.html#ARRAYS-IO
func decodeArray(s []byte, typ oid.Oid) (interface{}, error) {

	//log.Printf("------------ Decoding array <%s>", string(s))

	// Arrays can be null
	if s == nil {
		return nil, nil
	}

	length := len(s)

	// If there's anything, there should at least be empty braces: {}
	if length < 2 {
		return nil, fmt.Errorf("Malformed array string: %s", s)
	}

	if s[0] != '{' {
		return nil, fmt.Errorf("Malformed array string: Should start with '{', but found %s instead", s[0])
	}

	if s[length-1] != '}' {
		return nil, fmt.Errorf("Malformed array string: Should end with '}', but found %s instead", s[length-1])
	}

	// get the element type for this array type, and it's delimiter
	elementTyp := oid.ElementType[typ]
	delimiter := oid.GetArrayElementDelimiter(elementTyp)
	//log.Printf("Element type: %d, delimiter = %s", elementTyp, string(delimiter))

	// states for the decoder
	const (
		ready = iota
		backslash
		q_opened
		done
	)

	state := ready
	strings := make([][]byte, 0, 0)
	current := make([]byte, 0, 0)

	// loop through all chars except just-tested braces
	for i := 0; i < length; i++ {
		c := s[i]
		//log.Printf("current = <%s>, c = <%s>", string(current), string(c))

		switch state {
		case ready:
			switch c {
			case '{':
				// array opener.  do nothing (for now)
				// TODO: Array of arrays?  Maybe recurse here.
			case ' ':
				// whitespace outside of a quoted string shouldn't happen
				// ... but just ignore it if it does
			case '"':
				// starting a quoted element
				// throw the quote away, but remember we are quoted
				state = q_opened
			case '}':
				// array closer -- end of elements

				// TODO: Find a better way...?
				if length > 2 {
					// avoids adding an element if the empty array is present
					strings = append(strings, current)
				}

				//log.Printf("Done with element <%s>. Strings = %v", string(current), strings)
				//log.Printf("Done with array")
				current = make([]byte, 0, 0)
				state = done
			case delimiter:
				// an element just ended. record it
				strings = append(strings, current)

				//log.Printf("Done with element <%s>. Strings = %v", string(current), strings)
				current = make([]byte, 0, 0)
				state = ready
			default:
				// any other char is the part of a non-quoted element; include it
				current = append(current, c)
			}
		case backslash:
			// the last character was a backslash;
			// perhaps do something interesting with this character
			switch c {
			case '"', '\\':
				// if this is a special char, insert just the special char
				current = append(current, c)
				state = q_opened
			default:
				// otherwise insert both the backslash and the char
				current = append(current, '\\', c)
				state = q_opened
			}
		case q_opened:
			// a quote was opened, but not yet closed
			// delimiters and brackets not treated specially, but escape sequences are
			switch c {
			case '\\':
				//handle the next character specially depending on what it is
				state = backslash
			case '"':
				// the end quote
				state = ready
			default:
				// anything that's not escaped, or not an end quote is part of the element
				current = append(current, c)
			}
		case done:
			panic("You should not reach done state before the end of the string")
		}
	}

	// determine the Go type of elements
	goElementType := oid.GetGoType(elementTyp)
	//log.Printf("Element type is %v", goElementType)

	// then make a slice of that
	sliceType := reflect.SliceOf(goElementType)
	elements := reflect.MakeSlice(sliceType, 0, len(strings))
	//log.Printf("Made slice: %v", elements)

	// and populate it
	for _, v := range strings {
		// decode individually
		element := decode(v, elementTyp)
		//log.Printf("Decoded %s into %v", string(v), element)

		// and add to the slice
		elements = reflect.Append(elements, reflect.ValueOf(element))
	}

	return elements.Interface(), nil
}

func encodeArray(slice interface{}) ([]byte, error) {

	log.Printf("Encoding as array: %v", slice)
	return nil, nil
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

		// Float parts are 0 to 9, and the decimal place
		isFloatPart := (b == '.') || (b >= '0' && b <= '9')

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
