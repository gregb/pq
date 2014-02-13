package pq

import (
	"database/sql/driver"
	"fmt"
	"github.com/gregb/pq/oid"
	"reflect"
	"unicode"
)

// arrayConverter is a struct which remembers what type the array is, and provides
// methods for converting between slices and the postgres string representation of
// arrays.  Implements database/sql/driver.ValueConverter
// TODO: Why not just implement the methods on oid.Oid directly?
type arrayConverter struct {
	ArrayTyp        oid.Oid // The postgres typ of the array
	parameterStatus *parameterStatus
}

// Parses arrays returned from postgres.
// The parameter is an ASCII string of the format defined at
// http://www.postgresql.org/docs/9.2/static/arrays.html#ARRAYS-IO.
// The interface{} returned is a slice of the appropriate type of element.
func (c *arrayConverter) decode(s []byte) (interface{}, error) {

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
		return nil, fmt.Errorf("Malformed array string: Should start with '{', but found %s instead", string(s[0]))
	}

	if s[length-1] != '}' {
		return nil, fmt.Errorf("Malformed array string: Should end with '}', but found %s instead", string(s[length-1]))
	}

	// get the element type for this array type, and it's delimiter
	elementTyp := c.ArrayTyp.ElementType()
	delimiter := elementTyp.Delimiter()

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
	goElementType := elementTyp.GoType()

	// then make a slice of that
	sliceType := reflect.SliceOf(goElementType)
	elements := reflect.MakeSlice(sliceType, 0, len(strings))

	// and populate it
	for _, v := range strings {
		// decode individually and add to slice
		element := decode(c.parameterStatus, v, elementTyp)
		elements = reflect.Append(elements, reflect.ValueOf(element))
	}

	return elements.Interface(), nil
}

func (c *arrayConverter) encode(sliceAsIface interface{}) ([]byte, error) {
	val := reflect.ValueOf(sliceAsIface)

	if val.Kind() == reflect.Ptr {
		val = reflect.Indirect(val)
	}

	if val.Kind() != reflect.Slice {
		return nil, fmt.Errorf("arrayConverter.ConvertValue expects a slice parameter; received %v instead", val.Kind())
	}

	length := val.Len()

	// Dumb guess; underestimate at 2 braces plus 3 chars per element
	strLenEstimate := 2 + length*3
	bytes := make([]byte, 0, strLenEstimate)

	bytes = append(bytes, '{')

	elementType := c.ArrayTyp.ElementType()
	delimiter := elementType.Delimiter()

	var elementBytes []byte

	// append items
	for i := 0; i < length; i++ {
		element := val.Index(i).Interface()

		// have to treat certain strings specially...
		if elementType.Category() == oid.C_string {
			elementBytes = encodeArrayString(element.(string), rune(delimiter))
		} else {
			elementBytes = encode(c.parameterStatus, element, elementType)
		}

		if i > 0 {
			bytes = append(bytes, delimiter)
		}

		bytes = append(bytes, elementBytes...)
	}

	bytes = append(bytes, '}')

	return bytes, nil
}

// Implements driver.ValueConverter: ConvertValue(v interface{}) (Value, error)
func (c *arrayConverter) ConvertValue(sliceAsIface interface{}) (driver.Value, error) {

	bytes, err := c.encode(sliceAsIface)

	if err != nil {
		return nil, err
	}

	stringAsIface := reflect.ValueOf(bytes).Interface().(driver.Value)
	return stringAsIface, nil
}

func encodeArrayString(s string, delimiter rune) []byte {

	length := len(s)

	// check empty string
	if length == 0 {
		return []byte(`""`)
	}

	// check null
	if length == 4 && s[0] == 'N' && s[1] == 'U' && s[2] == 'L' && s[3] == 'L' {
		return []byte(`"NULL"`)
	}

	// check for special characters
	needsEscaping := false
	runes := []rune(s)

	// if first or last chars are whitespace, then quoting is needed
	if unicode.IsSpace(runes[0]) || unicode.IsSpace(runes[len(runes)-1]) {
		needsEscaping = true
	} else {
		// else check internally
		for _, r := range runes {
			if r == '"' || r == '\\' || r == delimiter {
				needsEscaping = true
				break
			}
		}
	}

	if !needsEscaping {
		return []byte(s)
	}

	// second pass to process
	modified := make([]byte, 0, len(s)+3)

	modified = append(modified, '"')

	for _, r := range s {
		// things to escape
		if r == '"' || r == '\\' {
			modified = append(modified, '\\')
		}
		modified = append(modified, byte(r))
	}

	modified = append(modified, '"')

	return modified
}
