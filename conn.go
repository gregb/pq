// Package pq is a pure Go Postgres driver for the database/sql package.
package pq

import (
	"bufio"
	"crypto/md5"
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/gregb/pq/oid"
	"io"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
)

// message codes from http://www.postgresql.org/docs/9.2/static/protocol-message-formats.html
type backendMessage byte
type frontendMessage byte

const (
	// backend messages.  received from server
	m_commandComplete      backendMessage = 'C'
	m_dataRow              backendMessage = 'D'
	m_error                backendMessage = 'E'
	m_keyData              backendMessage = 'K'
	m_authenticate         backendMessage = 'R'
	m_parameterStatus      backendMessage = 'S'
	m_rowDescription       backendMessage = 'T'
	m_parameterDescription backendMessage = 't'
	m_noData               backendMessage = 'n'
	m_notice               backendMessage = 'N'
	m_readyForQuery        backendMessage = 'Z'
	m_parseComplete        backendMessage = '1'
	m_bindComplete         backendMessage = '2'
	m_closeComplete        backendMessage = '3'
)

const (
	// frontend messages.  sent to server
	m_close     frontendMessage = 'C'
	m_describe  frontendMessage = 'D'
	m_execute   frontendMessage = 'E'
	m_parse     frontendMessage = 'P'
	m_password  frontendMessage = 'p'
	m_query     frontendMessage = 'Q'
	m_sync      frontendMessage = 'S'
	m_terminate frontendMessage = 'X'
)

var (
	ErrSSLNotSupported = errors.New("pq: SSL is not enabled on the server")
	ErrNotSupported    = errors.New("pq: invalid command")
)

type drv struct{}

func (d *drv) Open(name string) (driver.Conn, error) {
	return Open(name)
}

func init() {
	sql.Register("postgres", &drv{})
}

type conn struct {
	c       net.Conn
	buf     *bufio.Reader
	namei   int
	scratch [512]byte
}

func (c *conn) writeMessageType(b frontendMessage) *writeBuf {
	c.scratch[0] = byte(b)
	w := writeBuf(c.scratch[:5])
	return &w
}

func (c *conn) writeBuf(b byte) *writeBuf {
	c.scratch[0] = b
	w := writeBuf(c.scratch[:5])
	return &w
}

func Open(name string) (_ driver.Conn, err error) {
	defer errRecover(&err)
	defer errRecoverWithPGReason(&err)

	o := make(Values)

	// A number of defaults are applied here, in this order:
	//
	// * Very low precedence defaults applied in every situation
	// * Environment variables
	// * Explicitly passed connection information
	o.Set("host", "localhost")
	o.Set("port", "5432")

	for k, v := range parseEnviron(os.Environ()) {
		o.Set(k, v)
	}

	parseOpts(name, o)

	// If a user is not provided by any other means, the last
	// resort is to use the current operating system provided user
	// name.
	if o.Get("user") == "" {
		u, err := userCurrent()
		if err != nil {
			return nil, err
		} else {
			o.Set("user", u)
		}
	}

	c, err := net.Dial(network(o))
	if err != nil {
		return nil, err
	}

	cn := &conn{c: c}
	cn.ssl(o)
	cn.buf = bufio.NewReader(cn.c)
	cn.startup(o)
	return cn, nil
}

func network(o Values) (string, string) {
	host := o.Get("host")

	if strings.HasPrefix(host, "/") {
		sockPath := path.Join(host, ".s.PGSQL."+o.Get("port"))
		return "unix", sockPath
	}

	return "tcp", host + ":" + o.Get("port")
}

type Values map[string]string

func (vs Values) Set(k, v string) {
	vs[k] = v
}

func (vs Values) Get(k string) (v string) {
	return vs[k]
}

func parseOpts(name string, o Values) {
	if len(name) == 0 {
		return
	}

	name = strings.TrimSpace(name)

	ps := strings.Split(name, " ")
	for _, p := range ps {
		kv := strings.Split(p, "=")
		if len(kv) < 2 {
			errorf("invalid option: %q", p)
		}
		o.Set(kv[0], kv[1])
	}
}

func (cn *conn) Begin() (driver.Tx, error) {
	_, err := cn.Exec("BEGIN", nil)
	if err != nil {
		return nil, err
	}
	return cn, err
}

func (cn *conn) Commit() error {
	_, err := cn.Exec("COMMIT", nil)
	return err
}

func (cn *conn) Rollback() error {
	_, err := cn.Exec("ROLLBACK", nil)
	return err
}

func (cn *conn) gname() string {
	cn.namei++
	return strconv.FormatInt(int64(cn.namei), 10)
}

func (cn *conn) simpleQuery(q string) (res driver.Result, err error) {
	defer errRecover(&err)

	b := cn.writeMessageType(m_query)
	b.string(q)
	cn.send(b)

	// just reusing it's slice members because we're doing the same thing stmt is doing
	resultDataHolder := &stmt{}

	for {
		t, r := cn.recv1()
		switch t {
		case m_commandComplete:
			count := parseComplete(r.string())
			res = createResult(count, resultDataHolder.execData)
		case m_readyForQuery:
			// done
			return
		case m_error:
			err = parseError(r)
		case m_notice, m_parameterStatus:
			// ignore
		case m_rowDescription:
			resultDataHolder.parseRowDesciption(r)
		case m_dataRow:
			l := len(resultDataHolder.cols)
			resultDataHolder.execData = make([]driver.Value, l, l)
			resultDataHolder.parseDataRow(r, resultDataHolder.execData)
		default:
			errorf("unknown response for simple query: %q", t)
		}
	}
	panic("not reached")
}

func (cn *conn) prepareTo(q, stmtName string) (_ driver.Stmt, err error) {
	defer errRecover(&err)

	st := &stmt{cn: cn, name: stmtName, query: q}

	b := cn.writeMessageType(m_parse)
	b.string(st.name)
	b.string(q)
	b.int16(0)
	cn.send(b)

	b = cn.writeMessageType(m_describe)
	b.byte('S') // statement
	b.string(st.name)
	cn.send(b)

	cn.send(cn.writeMessageType(m_sync))

	for {
		t, r := cn.recv1()
		switch t {
		case m_parseComplete, m_bindComplete, m_notice:
		case m_parameterDescription:
			nparams := int(r.int16())
			st.paramTyps = make([]oid.Oid, nparams)

			for i := range st.paramTyps {
				st.paramTyps[i] = r.oid()
			}
		case m_rowDescription:
			st.parseRowDesciption(r)
		case m_noData:
			// no data
		case m_readyForQuery:
			return st, err
		case m_error:
			err = parseError(r)
		case m_commandComplete:
			// command complete
			return st, err
		default:
			errorf("unexpected describe rows response: %q", t)
		}
	}

	panic("not reached")
}

func (cn *conn) Prepare(q string) (driver.Stmt, error) {
	return cn.prepareTo(q, cn.gname())
}

func (cn *conn) Close() (err error) {
	defer errRecover(&err)
	cn.send(cn.writeMessageType(m_terminate))

	return cn.c.Close()
}

// Implement the optional "Execer" interface for one-shot queries
func (cn *conn) Exec(query string, args []driver.Value) (_ driver.Result, err error) {
	defer errRecover(&err)

	// Check to see if we can use the "simpleQuery" interface, which is
	// *much* faster than going through prepare/exec
	if len(args) == 0 {
		return cn.simpleQuery(query)
	}

	// Use the unnamed statement to defer planning until bind
	// time, or else value-based selectivity estimates cannot be
	// used.
	st, err := cn.prepareTo(query, "")
	if err != nil {
		panic(err)
	}

	r, err := st.Exec(args)
	if err != nil {
		panic(err)
	}

	return r, err
}

// Assumes len(*m) is > 5
func (cn *conn) send(m *writeBuf) {
	b := (*m)[1:]
	binary.BigEndian.PutUint32(b, uint32(len(b)))

	if (*m)[0] == 0 {
		*m = b
	}

	_, err := cn.c.Write(*m)
	if err != nil {
		panic(err)
	}
}

func (cn *conn) recv() (t backendMessage, r *readBuf) {
	for {
		t, r = cn.recv1()
		switch t {
		case m_error:
			panic(parseError(r))
		case m_notice:
			// ignore
		default:
			return
		}
	}

	panic("not reached")
}

func (cn *conn) recv1() (backendMessage, *readBuf) {
	x := cn.scratch[:5]
	_, err := io.ReadFull(cn.buf, x)
	if err != nil {
		panic(err)
	}
	c := backendMessage(x[0])

	b := readBuf(x[1:])
	n := b.int32() - 4
	var y []byte
	if n <= len(cn.scratch) {
		y = cn.scratch[:n]
	} else {
		y = make([]byte, n)
	}
	_, err = io.ReadFull(cn.buf, y)
	if err != nil {
		panic(err)
	}

	return c, (*readBuf)(&y)
}

func (cn *conn) ssl(o Values) {
	tlsConf := tls.Config{}
	switch mode := o.Get("sslmode"); mode {
	case "require", "":
		tlsConf.InsecureSkipVerify = true
	case "verify-full":
		// fall out
	case "disable":
		return
	default:
		errorf(`unsupported sslmode %q; only "require" (default), "verify-full", and "disable" supported`, mode)
	}

	w := cn.writeBuf(0)
	w.int32(80877103)
	cn.send(w)

	b := cn.scratch[:1]
	_, err := io.ReadFull(cn.c, b)
	if err != nil {
		panic(err)
	}

	if b[0] != 'S' {
		panic(ErrSSLNotSupported)
	}

	cn.c = tls.Client(cn.c, &tlsConf)
}

func (cn *conn) startup(o Values) {
	w := cn.writeBuf(0)
	w.int32(196608)
	w.string("user")
	w.string(o.Get("user"))
	w.string("database")
	w.string(o.Get("dbname"))
	w.string("")
	cn.send(w)

	for {
		t, r := cn.recv()
		switch t {
		case m_keyData, 'S':
		case m_authenticate:
			cn.auth(r, o)
		case m_readyForQuery:
			return
		default:
			errorf("unknown response for startup: %q", t)
		}
	}
}

func (cn *conn) auth(r *readBuf, o Values) {
	switch code := r.int32(); code {
	case 0:
		// OK
	case 3:
		w := cn.writeMessageType(m_password)
		w.string(o.Get("password"))
		cn.send(w)

		t, r := cn.recv()
		if t != m_authenticate {
			errorf("unexpected password response: %q", t)
		}

		if r.int32() != 0 {
			errorf("unexpected authentication response: %q", t)
		}
	case 5:
		s := string(r.next(4))
		w := cn.writeMessageType(m_password)
		w.string("md5" + md5s(md5s(o.Get("password")+o.Get("user"))+s))
		cn.send(w)

		t, r := cn.recv()
		if t != m_authenticate {
			errorf("unexpected password response: %q", t)
		}

		if r.int32() != 0 {
			errorf("unexpected authentication response: %q", t)
		}
	default:
		errorf("unknown authentication response: %d", code)
	}
}

func md5s(s string) string {
	h := md5.New()
	h.Write([]byte(s))
	return fmt.Sprintf("%x", h.Sum(nil))
}

// parseEnviron tries to mimic some of libpq's environment handling
//
// To ease testing, it does not directly reference os.Environ, but is
// designed to accept its output.
//
// Environment-set connection information is intended to have a higher
// precedence than a library default but lower than any explicitly
// passed information (such as in the URL or connection string).
func parseEnviron(env []string) (out map[string]string) {
	out = make(map[string]string)

	for _, v := range env {
		parts := strings.SplitN(v, "=", 2)

		accrue := func(keyname string) {
			out[keyname] = parts[1]
		}

		// The order of these is the same as is seen in the
		// PostgreSQL 9.1 manual, with omissions briefly
		// noted.
		switch parts[0] {
		case "PGHOST":
			accrue("host")
		case "PGHOSTADDR":
			accrue("hostaddr")
		case "PGPORT":
			accrue("port")
		case "PGDATABASE":
			accrue("dbname")
		case "PGUSER":
			accrue("user")
		case "PGPASSWORD":
			accrue("password")
			// skip PGPASSFILE, PGSERVICE, PGSERVICEFILE,
			// PGREALM
		case "PGOPTIONS":
			accrue("options")
		case "PGAPPNAME":
			accrue("application_name")
		case "PGSSLMODE":
			accrue("sslmode")
		case "PGREQUIRESSL":
			accrue("requiressl")
		case "PGSSLCERT":
			accrue("sslcert")
		case "PGSSLKEY":
			accrue("sslkey")
		case "PGSSLROOTCERT":
			accrue("sslrootcert")
		case "PGSSLCRL":
			accrue("sslcrl")
		case "PGREQUIREPEER":
			accrue("requirepeer")
		case "PGKRBSRVNAME":
			accrue("krbsrvname")
		case "PGGSSLIB":
			accrue("gsslib")
		case "PGCONNECT_TIMEOUT":
			accrue("connect_timeout")
		case "PGCLIENTENCODING":
			accrue("client_encoding")
			// skip PGDATESTYLE, PGTZ, PGGEQO, PGSYSCONFDIR,
			// PGLOCALEDIR
		}
	}

	return out
}
