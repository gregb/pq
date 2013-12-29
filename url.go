package pq

import (
	"fmt"
	nurl "net/url"
	"sort"
	"strings"
)

type kvs []string

// ParseURL no longer needs to be used by clients of this library since supplying a URL as a
// connection string to sql.Open() is now supported:
//
//	sql.Open("postgres", "postgres://bob:secret@1.2.3.4:5432/mydb?sslmode=verify-full")
//
// It remains exported here for backwards-compatibility.
//

// ParseURL converts a url to a connection string for driver.Open.
// Example:
//
//	"postgres://bob:secret@1.2.3.4:5432/mydb?sslmode=verify-full"
//
// converts to:
//
//	"user=bob password=secret host=1.2.3.4 port=5432 dbname=mydb sslmode=verify-full"
//
// A minimal example:
//
//	"postgres://"
//
// This will be blank, causing driver.Open to use all of the defaults
func ParseURL(url string) (string, error) {
	u, err := nurl.Parse(url)
	if err != nil {
		return "", err
	}

	if u.Scheme != "postgres" {
		return "", fmt.Errorf("invalid connection protocol: %s", u.Scheme)
	}

	params := new(kvs)

	if u.User != nil {
		v := u.User.Username()
		params.accrue("user", v)

		v, _ = u.User.Password()
		params.accrue("password", v)
	}

	i := strings.Index(u.Host, ":")
	if i < 0 {
		params.accrue("host", u.Host)
	} else {
		params.accrue("host", u.Host[:i])
		params.accrue("port", u.Host[i+1:])
	}

	if u.Path != "" {
		params.accrue("dbname", u.Path[1:])
	}

	q := u.Query()
	for k := range q {
		params.accrue(k, q.Get(k))
	}

	return params.String(), nil
}

func (kvs *kvs) accrue(k string, v interface{}) {

	if v != "" {
		s := fmt.Sprintf("%s=%v", k, v)
		*kvs = append(*kvs, s)
	}
}

func (kvs *kvs) String() string {
	sort.Strings(*kvs) // Makes testing easier (not a performance concern)
	return strings.Join(*kvs, " ")
}

func ParseMap(m map[string]interface{}) (string, error) {

	params := new(kvs)

	for k, v := range m {
		params.accrue(k, v)
	}

	return params.String(), nil
}
