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
	"github.com/gregb/pq/message"
	"github.com/gregb/pq/oid"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// Common error types
var (
	ErrSSLNotSupported     = errors.New("pq: SSL is not enabled on the server")
	ErrNotSupported        = errors.New("pq: Unsupported command")
	ErrInFailedTransaction = errors.New("pq: Could not complete operation in a failed transaction")
)

var TrafficLogging bool = false

type drv struct{}

func (d *drv) Open(name string) (driver.Conn, error) {
	return Open(name)
}

func init() {
	sql.Register("postgres", &drv{})
}

type parameterStatus struct {
	// server version in the same format as server_version_num, or 0 if
	// unavailable
	serverVersion int

	// the current location based on the TimeZone value of the session, if
	// available
	currentLocation *time.Location
}

type transactionStatus byte

const (
	txnStatusIdle                transactionStatus = 'I'
	txnStatusIdleInTransaction   transactionStatus = 'T'
	txnStatusInFailedTransaction transactionStatus = 'E'
)

func (s transactionStatus) String() string {
	switch s {
	case txnStatusIdle:
		return "idle"
	case txnStatusIdleInTransaction:
		return "idle in transaction"
	case txnStatusInFailedTransaction:
		return "in a failed transaction"
	default:
		errorf("unknown transactionStatus " + s)
	}
	panic("not reached")
}

type conn struct {
	c                 net.Conn
	buf               *bufio.Reader
	namei             int
	scratch           [512]byte
	txnStatus         transactionStatus
	parameterStatus   parameterStatus
	saveMessageType   message.Backend
	saveMessageBuffer *readBuf
}

func (c *conn) writeMessageType(b message.Frontend) *writeBuf {
	c.scratch[0] = byte(b)
	w := writeBuf(c.scratch[:5])

	// TODO: Better way to do this?
	// The saved message type and buffer from the workaround
	// should not be saved between queries. If a query message
	// is being sent, clear them out
	if b == message.Query {
		c.saveMessageBuffer = nil
		c.saveMessageType = 0
	}

	return &w
}

func (c *conn) writeBuf(b byte) *writeBuf {
	c.scratch[0] = b
	w := writeBuf(c.scratch[:5])
	return &w
}

func Open(name string) (_ driver.Conn, err error) {
	defer errRecover(&err)

	o := make(values)

	// A number of defaults are applied here, in this order:
	//
	// * Very low precedence defaults applied in every situation
	// * Environment variables
	// * Explicitly passed connection information
	o.Set("host", "localhost")
	o.Set("port", "5432")
	// N.B.: Extra float digits should be set to 3, but that breaks
	// Postgres 8.4 and older, where the max is 2.
	o.Set("extra_float_digits", "2")
	for k, v := range parseEnviron(os.Environ()) {
		o.Set(k, v)
	}

	if strings.HasPrefix(name, "postgres://") {
		name, err = ParseURL(name)
		if err != nil {
			return nil, err
		}
	}
	if err := parseOpts(name, o); err != nil {
		return nil, err
	}
	// We can't work with any client_encoding other than UTF-8 currently.
	// However, we have historically allowed the user to set it to UTF-8
	// explicitly, and there's no reason to break such programs, so allow that.
	// Note that the "options" setting could also set client_encoding, but
	// parsing its value is not worth it.  Instead, we always explicitly send
	// client_encoding as a separate run-time parameter, which should override
	// anything set in options.
	if enc := o.Get("client_encoding"); enc != "" && !isUTF8(enc) {
		return nil, errors.New("client_encoding must be absent or 'UTF8'")
	}
	o.Set("client_encoding", "UTF8")
	// DateStyle needs a similar treatment.
	if datestyle := o.Get("datestyle"); datestyle != "" {
		if datestyle != "ISO, MDY" {
			panic(fmt.Sprintf("setting datestyle must be absent or %v; got %v",
				"ISO, MDY", datestyle))
		}
	} else {
		o.Set("datestyle", "ISO, MDY")
	}

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

func (cn *conn) isInTransaction() bool {
	return cn.txnStatus == txnStatusIdleInTransaction ||
		cn.txnStatus == txnStatusInFailedTransaction
}
func (cn *conn) checkIsInTransaction(intxn bool) {
	if cn.isInTransaction() != intxn {
		errorf("unexpected transaction status %v", cn.txnStatus)
	}
}
func (cn *conn) Begin() (_ driver.Tx, err error) {
	defer errRecover(&err)
	cn.checkIsInTransaction(false)
	_, commandTag, err := cn.simpleExec("BEGIN")
	if err != nil {
		return nil, err
	}
	if commandTag != "BEGIN" {
		return nil, fmt.Errorf(`unexpected command tag "%s"; expected BEGIN`, commandTag)
	}
	if cn.txnStatus != txnStatusIdleInTransaction {
		return nil, fmt.Errorf("unexpected transaction status %v", cn.txnStatus)
	}
	return cn, nil
}

func (cn *conn) Commit() (err error) {
	defer errRecover(&err)
	cn.checkIsInTransaction(true)
	// We don't want the client to think that everything is okay if it tries
	// to commit a failed transaction.  However, no matter what we return,
	// database/sql will release this connection back into the free connection
	// pool so we have to abort the current transaction here.  Note that you
	// would get the same behaviour if you issued a COMMIT in a failed
	// transaction, so it's also the least surprising thing to do here.
	if cn.txnStatus == txnStatusInFailedTransaction {
		if err := cn.Rollback(); err != nil {
			return err
		}
		return ErrInFailedTransaction
	}
	_, commandTag, err := cn.simpleExec("COMMIT")
	if err != nil {
		return err
	}
	if commandTag != "COMMIT" {
		return fmt.Errorf(`unexpected command tag "%s"; expected COMMIT`, commandTag)
	}
	cn.checkIsInTransaction(false)
	return nil
}

func (cn *conn) Rollback() (err error) {
	defer errRecover(&err)
	cn.checkIsInTransaction(true)
	_, commandTag, err := cn.simpleExec("ROLLBACK")
	if err != nil {
		return err
	}
	if commandTag != "ROLLBACK" {
		return fmt.Errorf(`unexpected command tag "%s"; expected ROLLBACK`, commandTag)
	}
	cn.checkIsInTransaction(false)
	return nil
}

func (cn *conn) gname() string {
	cn.namei++
	return strconv.FormatInt(int64(cn.namei), 10)
}

func (cn *conn) simpleExec(q string) (res driver.Result, commandTag string, err error) {
	defer errRecover(&err)

	st := &stmt{cn: cn, name: "", query: q}
	b := cn.writeMessageType(message.Query)
	b.string(q)
	cn.send(b)

	for {
		t, r := cn.recv1()
		switch t {
		case message.CommandComplete:
			var rowsAffected int64
			rowsAffected, commandTag = parseComplete(r.string())

			if st.rowData != nil {
				res = createResult(rowsAffected, st.rowData)
			} else {
				res = driver.RowsAffected(rowsAffected)
			}
		case message.ReadyForQuery:
			cn.processReadyForQuery(r)
			// done
			return
		case message.Error:
			err = parseError(r)
		case message.RowDescription:
			st.parseRowDesciption(r)
		case message.DataRow:
			l := len(st.cols)
			st.rowData = make([]driver.Value, l, l)
			st.parseDataRow(r, st.rowData)
		default:
			errorf("unknown response for simple query: %q", t)
		}
	}
	panic("not reached")
}

func (cn *conn) simpleQuery(q string) (res driver.Rows, err error) {
	defer errRecover(&err)

	st := &stmt{cn: cn, name: "", query: q}
	b := cn.writeMessageType(message.Query)
	b.string(q)
	cn.send(b)
	for {
		t, r := cn.recv1()
		switch t {
		case message.CommandComplete:
			// We allow queries which don't return any results through Query as
			// well as Exec.  We still have to give database/sql a rows object
			// the user can close, though, to avoid connections from being
			// leaked.  A "rows" with done=true works fine for that purpose.
			if err != nil {
				errorf("unexpected CommandComplete in simple query execution")
			}
			res = &rows{st: st, done: true}
		case message.ReadyForQuery:
			cn.processReadyForQuery(r)
			// done
			return
		case message.Error:
			res = nil
			err = parseError(r)
		case message.Notice:
			// ignore
		case message.ParameterStatus:
			// ignore any results
		case message.RowDescription:
			st.parseRowDesciption(r)

			// After we get the meta, we want to kick out to Next()
			res = &rows{st: st, done: false}
			return
		default:
			errorf("unknown response for simple query: %q", t)
		}
	}
	panic("not reached")
}

func (cn *conn) prepareTo(q, stmtName string) (_ driver.Stmt, err error) {
	return cn.prepareToSimpleStmt(q, stmtName)
}
func (cn *conn) prepareToSimpleStmt(q, stmtName string) (_ *stmt, err error) {
	defer errRecover(&err)

	st := &stmt{cn: cn, name: stmtName, query: q}

	b := cn.writeMessageType(message.Parse)
	b.string(st.name)
	b.string(q)
	b.int16(0)
	cn.send(b)

	b = cn.writeMessageType(message.Describe)
	b.byte('S') // statement
	b.string(st.name)
	cn.send(b)

	cn.send(cn.writeMessageType(message.Sync))

	for {
		t, r := cn.recv1()
		switch t {
		case message.ParseComplete:
			// ignore
		case message.ParameterDescription:
			nparams := int(r.int16())
			st.paramTyps = make([]oid.Oid, nparams)

			for i := range st.paramTyps {
				st.paramTyps[i] = r.oid()
			}
		case message.RowDescription:
			st.parseRowDesciption(r)
		case message.NoData:
			// no data
		case message.ReadyForQuery:
			cn.processReadyForQuery(r)
			return st, err
		case message.Error:
			err = parseError(r)
		case message.CommandComplete:
			// command complete
			return st, err
		default:
			errorf("unexpected describe rows response: %q", t)
		}
	}

	panic("not reached")
}

func (cn *conn) Prepare(q string) (driver.Stmt, error) {
	if len(q) >= 4 && strings.EqualFold(q[:4], "COPY") {
		return cn.prepareCopyIn(q)
	}
	return cn.prepareTo(q, cn.gname())
}

func (cn *conn) Close() (err error) {
	defer errRecover(&err)
	cn.send(cn.writeMessageType(message.Terminate))

	return cn.c.Close()
}

// Let's NOT implement the "Queryer" interface...
// It interferes with array parameter preparation
// which is only available on statements (and Query()
// does not use a statement)
/*
func (cn *conn) Query(query string, args []driver.Value) (_ driver.Rows, err error) {
	defer errRecover(&err)

	// Check to see if we can use the "simpleQuery" interface, which is
	// *much* faster than going through prepare/exec
	if len(args) == 0 {
		return cn.simpleQuery(query)
	}

	st, err := cn.prepareToSimpleStmt(query, "")

	if err != nil {
		panic(err)
	}
	st.exec(args)
	return &rows{st: st}, nil
}
*/

// Implement the optional "Execer" interface for one-shot queries

func (cn *conn) Exec(query string, args []driver.Value) (_ driver.Result, err error) {
	defer errRecover(&err)

	// Check to see if we can use the "simpleExec" interface, which is
	// *much* faster than going through prepare/exec
	if len(args) == 0 {
		// ignore commandTag, our caller doesn't care
		r, _, err := cn.simpleExec(query)
		return r, err
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

	if TrafficLogging {
		log.Printf("Sending : (%c) %q", (*m)[0], b)
	}

	_, err := cn.c.Write(*m)
	if err != nil {
		panic(err)
	}
}

// recvMessage receives any message from the backend, or returns an error if
// a problem occurred while reading the message.
func (cn *conn) recvMessage() (message.Backend, *readBuf, error) {
	// workaround for a QueryRow bug, see exec
	if cn.saveMessageType != 0 {
		t, r := cn.saveMessageType, cn.saveMessageBuffer
		cn.saveMessageType = 0
		cn.saveMessageBuffer = nil

		if TrafficLogging {
			log.Printf("Returning worked-around saved message: (%c) %q", t, (*r))
		}

		return t, r, nil
	}

	x := cn.scratch[:5]
	_, err := io.ReadFull(cn.buf, x)
	if err != nil {
		return 0, nil, err
	}
	t := message.Backend(x[0])

	b := readBuf(x[1:])

	if TrafficLogging {
		log.Printf("Received: (%c) %q", t, b)
	}

	n := b.int32() - 4
	var y []byte
	if n <= len(cn.scratch) {
		y = cn.scratch[:n]
	} else {
		y = make([]byte, n)
	}
	_, err = io.ReadFull(cn.buf, y)
	if err != nil {
		return 0, nil, err
	}
	return t, (*readBuf)(&y), nil
}

// recv receives a message from the backend, but if an error happened while
// reading the message or the received message was an ErrorResponse, it panics.
// NoticeResponses are ignored.  This function should generally be used only
// during the startup sequence.
func (cn *conn) recv() (t message.Backend, r *readBuf) {
	for {
		var err error
		t, r, err = cn.recvMessage()
		if err != nil {
			panic(err)
		}
		switch t {
		case message.Error:
			panic(parseError(r))
		case message.Notice:
			// ignore
		default:
			return
		}
	}

	panic("not reached")
}

// recv1 receives a message from the backend, panicking if an error occurs
// while attempting to read it.  All asynchronous messages are ignored, with
// the exception of ErrorResponse.
func (cn *conn) recv1() (t message.Backend, r *readBuf) {
	for {
		var err error
		t, r, err = cn.recvMessage()
		if err != nil {
			panic(err)
		}

		switch t {
		case message.NotificationResponse, message.Notice:
			// ignore
		case message.ParameterStatus:
			cn.processParameterStatus(r)
		default:
			return
		}
	}

	panic("not reached")
}

func (cn *conn) ssl(o values) {
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

func (cn *conn) startup(o values) {
	w := cn.writeBuf(0)
	w.int32(196608)
	// Send the backend the name of the database we want to connect to, and the
	// user we want to connect as.  Additionally, we send over any run-time
	// parameters potentially included in the connection string.  If the server
	// doesn't recognize any of them, it will reply with an error.
	for k, v := range o {
		// skip options which can't be run-time parameters
		if k == "password" || k == "host" ||
			k == "port" || k == "sslmode" {
			continue
		}
		// The protocol requires us to supply the database name as "database"
		// instead of "dbname".
		if k == "dbname" {
			k = "database"
		}
		w.string(k)
		w.string(v)
	}
	w.string("")
	cn.send(w)

	for {
		t, r := cn.recv()
		switch t {
		case message.KeyData:
			// ?
		case message.ParameterStatus:
			cn.processParameterStatus(r)
		case message.Authenticate:
			cn.auth(r, o)
		case message.ReadyForQuery:
			cn.processReadyForQuery(r)
			return
		default:
			errorf("unknown response for startup: %q", t)
		}
	}
}

func (cn *conn) auth(r *readBuf, o values) {
	switch code := r.int32(); code {
	case 0:
		// OK
	case 3:
		w := cn.writeMessageType(message.Password)
		w.string(o.Get("password"))
		cn.send(w)

		t, r := cn.recv()
		if t != message.Authenticate {
			errorf("unexpected password response: %q", t)
		}

		if r.int32() != 0 {
			errorf("unexpected authentication response: %q", t)
		}
	case 5:
		s := string(r.next(4))
		w := cn.writeMessageType(message.Password)
		w.string("md5" + md5s(md5s(o.Get("password")+o.Get("user"))+s))
		cn.send(w)

		t, r := cn.recv()
		if t != message.Authenticate {
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

func (c *conn) processParameterStatus(r *readBuf) {
	var err error
	param := r.string()
	switch param {
	case "server_version":
		var major1 int
		var major2 int
		var minor int
		_, err = fmt.Sscanf(r.string(), "%d.%d.%d", &major1, &major2, &minor)
		if err == nil {
			c.parameterStatus.serverVersion = major1*10000 + major2*100 + minor
		}
	case "TimeZone":
		c.parameterStatus.currentLocation, err = time.LoadLocation(r.string())
		if err != nil {
			c.parameterStatus.currentLocation = nil
		}
	default:
		if TrafficLogging {
			val := r.string()
			log.Printf("Unhandled parameter status: %s = %s", param, val)
		}
	}
}

func (c *conn) processReadyForQuery(r *readBuf) {
	c.txnStatus = transactionStatus(r.byte())
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
		unsupported := func() {
			panic(fmt.Sprintf("setting %v not supported", parts[0]))
		}

		// The order of these is the same as is seen in the
		// PostgreSQL 9.1 manual. Unsupported but well-defined
		// keys cause a panic; these should be unset prior to
		// execution. Options which pq expects to be set to a
		// certain value are allowed, but must be set to that
		// value if present (they can, of course, be absent).
		switch parts[0] {
		case "PGHOST":
			accrue("host")
		case "PGHOSTADDR":
			unsupported()
		case "PGPORT":
			accrue("port")
		case "PGDATABASE":
			accrue("dbname")
		case "PGUSER":
			accrue("user")
		case "PGPASSWORD":
			accrue("password")
		case "PGPASSFILE", "PGSERVICE", "PGSERVICEFILE", "PGREALM":
			unsupported()
		case "PGOPTIONS":
			accrue("options")
		case "PGAPPNAME":
			accrue("application_name")
		case "PGSSLMODE":
			accrue("sslmode")
		case "PGREQUIRESSL", "PGSSLCERT", "PGSSLKEY", "PGSSLROOTCERT", "PGSSLCRL":
			unsupported()
		case "PGREQUIREPEER":
			unsupported()
		case "PGKRBSRVNAME", "PGGSSLIB":
			unsupported()
		case "PGCONNECT_TIMEOUT":
			unsupported()
		case "PGCLIENTENCODING":
			accrue("client_encoding")
		case "PGDATESTYLE":
			accrue("datestyle")
		case "PGTZ":
			accrue("timezone")
		case "PGGEQO":
			accrue("geqo")
		case "PGSYSCONFDIR", "PGLOCALEDIR":
			unsupported()
		}
	}

	return out
}

// isUTF8 returns whether name is a fuzzy variation of the string "UTF-8".
func isUTF8(name string) bool {
	// Recognize all sorts of silly things as "UTF-8", like Postgres does
	s := strings.Map(alnumLowerASCII, name)
	return s == "utf8" || s == "unicode"
}
func alnumLowerASCII(ch rune) rune {
	if 'A' <= ch && ch <= 'Z' {
		return ch + ('a' - 'A')
	}
	if 'a' <= ch && ch <= 'z' || '0' <= ch && ch <= '9' {
		return ch
	}
	return -1 // discard
}
