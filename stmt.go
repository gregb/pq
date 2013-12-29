package pq

import (
	"database/sql/driver"
	"errors"
	"github.com/gregb/pq/oid"
	"io"
	"log"
	"strconv"
	"strings"
)

type stmt struct {
	cn        *conn
	name      string
	query     string
	cols      []string
	rowTyps   []oid.Oid
	paramTyps []oid.Oid
	closed    bool
	lasterr   error
	rowData   []driver.Value
}

// ColumnConverter returns a ValueConverter for the provided
// column index.  If the type of a specific column isn't known
// or shouldn't be handled specially, DefaultValueConverter
// can be returned.
// Implements driver.ColumnConverter: ColumnConverter(idx int) ValueConverter
func (st *stmt) ColumnConverter(idx int) driver.ValueConverter {
	paramTyp := st.paramTyps[idx]

	log.Printf("st.ColumnConverter(%d)", idx)

	// TODO: If oid.Oid could implement ConvertValue directly, we wouldn't have to keep creating new ones?
	if paramTyp.IsArray() {
		return &arrayConverter{ArrayTyp: paramTyp}
	}

	return driver.DefaultParameterConverter
}

func (st *stmt) Close() (err error) {
	if st.closed {
		return nil
	}

	defer errRecover(&err)

	w := st.cn.writeMessageType(m_close)
	w.byte('S') // this is not a sync message, it's a parameter to the close command (to close a statement)
	w.string(st.name)
	st.cn.send(w)

	st.cn.send(st.cn.writeMessageType(m_sync))

	t, r := st.cn.recv()
	if t != m_closeComplete {
		errorf("unexpected close response: %q", t)
	}
	st.closed = true

	t, r = st.cn.recv()
	if t != m_readyForQuery {
		errorf("expected ready for query, but got: %q", t)
	}
	st.cn.processReadyForQuery(r)

	return nil
}

func (st *stmt) Query(v []driver.Value) (_ driver.Rows, err error) {
	defer errRecover(&err)

	log.Printf("st.Query(%v)", v)
	st.exec(v)
	return &rows{st: st}, nil
}

func (st *stmt) Exec(v []driver.Value) (res driver.Result, err error) {
	defer errRecover(&err)

	log.Printf("st.Exec(%v)", v)
	if len(v) == 0 {
		// ignore commandTag, our caller doesn't care
		r, _, err := st.cn.simpleExec(st.query)
		return r, err
	}
	st.exec(v)

	for {
		t, r := st.cn.recv1()
		switch t {
		case m_error:
			err = parseError(r)
		case m_commandComplete:
			rowsAffected, _ := parseComplete(r.string())

			if st.rowData != nil {
				res = createResult(rowsAffected, st.rowData)
			} else {
				res = driver.RowsAffected(rowsAffected)
			}
		case m_readyForQuery:
			// done
			return
		case m_notice, m_parameterStatus:
			// ignore
		case m_rowDescription:
			st.parseRowDesciption(r)
		case m_dataRow:
			if st.cols != nil {
				st.rowData = make([]driver.Value, len(st.cols), len(st.cols))
				// we received a m_rowDescription at some point
				// so parse this now
				st.parseDataRow(r, st.rowData)
			}
		default:
			errorf("unknown exec response: %q", t)
		}
	}

	panic("not reached")
}

func (st *stmt) exec(v []driver.Value) {
	if len(v) != len(st.paramTyps) {
		errorf("got %d parameters but the statement requires %d", len(v), len(st.paramTyps))
	}
	w := st.cn.writeBuf('B')
	w.string("")
	w.string(st.name)
	w.int16(0)
	w.int16(len(v))
	for i, x := range v {
		if x == nil {
			w.int32(-1)
		} else {
			b := encode(&st.cn.parameterStatus, x, st.paramTyps[i])
			w.int32(len(b))
			w.bytes(b)
		}
	}
	w.int16(0)
	st.cn.send(w)

	w = st.cn.writeMessageType(m_execute)
	w.string("")
	w.int32(0)
	st.cn.send(w)

	st.cn.send(st.cn.writeMessageType(m_sync))

	var err error
	for {
		t, r := st.cn.recv1()
		switch t {
		case m_error:
			err = parseError(r)
		case m_bindComplete:
			if err != nil {
				panic(err)
			}
			goto workaround
		case m_readyForQuery:
			st.cn.processReadyForQuery(r)
			if err != nil {
				panic(err)
			}
			return
		case m_notice:
			// ignore
		default:
			errorf("unexpected bind response: %q", t)
		}
	}

	// Work around a bug in sql.DB.QueryRow: in Go 1.2 and earlier it ignores
	// any errors from rows.Next, which masks errors that happened during the
	// execution of the query.  To avoid the problem in common cases, we wait
	// here for one more message from the database.  If it's not an error the
	// query will likely succeed (or perhaps has already, if it's a
	// CommandComplete), so we push the message into the conn struct; recv1
	// will return it as the next message for rows.Next or rows.Close.
	// However, if it's an error, we wait until ReadyForQuery and then return
	// the error to our caller.
workaround:
	for {
		t, r := st.cn.recv1()
		switch t {
		case 'E':
			err = parseError(r)
		case 'C', 'D':
			// the query didn't fail, but we can't process this message
			st.cn.saveMessageType = t
			st.cn.saveMessageBuffer = r
			return
		case 'Z':
			if err == nil {
				errorf("unexpected ReadyForQuery during extended query execution")
			}
			panic(err)
		default:
			errorf("unexpected message during query execution: %q", t)
		}
	}
}

func (st *stmt) NumInput() int {
	return len(st.paramTyps)
}

// parseComplete parses the "command tag" from a CommandComplete message, and
// returns the number of rows affected (if applicable) and a string
// identifying only the command that was executed, e.g. "ALTER TABLE".  If the
// command tag could not be parsed, parseComplete panics.
func parseComplete(commandTag string) (int64, string) {

	log.Printf("parseComplete(%s)", commandTag)
	commandsWithAffectedRows := []string{
		"SELECT ",
		// INSERT is handled below
		"UPDATE ",
		"DELETE ",
		"FETCH ",
		"MOVE ",
		"COPY ",
	}

	var affectedRows *string
	for _, tag := range commandsWithAffectedRows {
		if strings.HasPrefix(commandTag, tag) {
			t := commandTag[len(tag):]
			affectedRows = &t
			commandTag = tag[:len(tag)-1]
			break
		}
	}
	// INSERT also includes the oid of the inserted row in its command tag.
	// Oids in user tables are deprecated, and the oid is only returned when
	// exactly one row is inserted, so it's unlikely to be of value to any
	// real-world application and we can ignore it.
	if affectedRows == nil && strings.HasPrefix(commandTag, "INSERT ") {
		parts := strings.Split(commandTag, " ")
		if len(parts) != 3 {
			errorf("unexpected INSERT command tag %s", commandTag)
		}
		affectedRows = &parts[len(parts)-1]
		commandTag = "INSERT"
	}
	// There should be no affected rows attached to the tag, just return it
	if affectedRows == nil {
		return 0, commandTag
	}
	n, err := strconv.ParseInt(*affectedRows, 10, 64)
	if err != nil {
		errorf("could not parse commandTag: %s", err)
	}
	return n, commandTag
}

func (st *stmt) parseRowDesciption(r *readBuf) {
	n := r.int16()
	st.cols = make([]string, n)
	st.rowTyps = make([]oid.Oid, n)

	for i := range st.cols {
		st.cols[i] = r.string()
		r.next(6)
		st.rowTyps[i] = r.oid()
		r.next(8)
	}
}

// Parses an m_dataRow message into a slice of driver values.
// A decode is run on each column value, based on column types set on the
// statement from a previous m_rowDescription message.
// Dest is an output parameter; it will mostly be st.rowData, but is
// provided as a parameter for reuse in Rows.Next()
func (st *stmt) parseDataRow(r *readBuf, dest []driver.Value) {
	n := r.int16()
	if n < len(dest) {
		dest = dest[:n]
	}
	for i := range dest {
		l := r.int32()
		if l == -1 {
			dest[i] = nil
			continue
		}
		dest[i] = decode(&st.cn.parameterStatus, r.next(l), st.rowTyps[i])
	}
}

type result struct {
	rowsAffected int64 // number of rows affected by the statement
	lastInsertId int64 // id of provided by last RETURNING clause
	idReturned   bool  // true if lastInserted id is valid on zero
}

func (r *result) LastInsertId() (int64, error) {

	if r.idReturned {
		return r.lastInsertId, nil
	}

	return 0, errors.New("no LastInsertId available")
}

func (r *result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

func createResult(rowsAffected int64, rowData []driver.Value) driver.Result {

	res := new(result)
	res.idReturned = false
	res.rowsAffected = rowsAffected

	// take the first int64 as the id
	for _, v := range rowData {
		n, ok := v.(int64)

		if ok {
			res.idReturned = true
			res.lastInsertId = n
			break
		}
	}

	return res
}

type rows struct {
	st   *stmt
	done bool
}

func (rs *rows) Close() error {
	for {
		err := rs.Next(nil)
		switch err {
		case nil:
		case io.EOF:
			return nil
		default:
			return err
		}
	}
	panic("not reached")
}

func (rs *rows) Columns() []string {
	return rs.st.cols
}

func (rs *rows) Next(dest []driver.Value) (err error) {
	if rs.done {
		return io.EOF
	}

	if rs.st.lasterr != nil {
		return rs.st.lasterr
	}
	defer errRecover(&err)

	conn := rs.st.cn
	for {
		t, r := conn.recv1()
		switch t {
		case m_error:
			err = parseError(r)
		case m_commandComplete, m_parameterStatus, m_notice:
			continue
		case m_readyForQuery:
			conn.processReadyForQuery(r)
			rs.done = true
			if err != nil {
				return err
			}
			return io.EOF
		case m_dataRow:
			rs.st.parseDataRow(r, dest)
			return
		default:
			errorf("unexpected message after execute: %q", t)
		}
	}

	panic("not reached")
}
