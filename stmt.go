package pq

import (
	"database/sql/driver"
	"errors"
	"github.com/gregb/pq/oid"
	"io"
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
	execData  []driver.Value
}

// ColumnConverter returns a ValueConverter for the provided
// column index.  If the type of a specific column isn't known
// or shouldn't be handled specially, DefaultValueConverter
// can be returned.
func (st *stmt) ColumnConverter(idx int) driver.ValueConverter {
	paramTyp := st.paramTyps[idx]

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

	t, _ := st.cn.recv()
	if t != m_closeComplete {
		errorf("unexpected close response: %q", t)
	}
	st.closed = true

	t, _ = st.cn.recv()
	if t != m_readyForQuery {
		errorf("expected ready for query, but got: %q", t)
	}

	return nil
}

func (st *stmt) Query(v []driver.Value) (_ driver.Rows, err error) {
	defer errRecover(&err)
	st.exec(v)
	return &rows{st: st}, nil
}

func (st *stmt) Exec(v []driver.Value) (res driver.Result, err error) {
	defer errRecover(&err)

	if len(v) == 0 {
		return st.cn.simpleQuery(st.query)
	}
	st.exec(v)

	for {
		t, r := st.cn.recv1()
		switch t {
		case m_error:
			err = parseError(r)
		case m_commandComplete:
			rowsAffected := parseComplete(r.string())

			if st.execData != nil {
				res = createResult(rowsAffected, st.execData)
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
				st.execData = make([]driver.Value, len(st.cols), len(st.cols))
				// we received a m_rowDescription at some point
				// so parse this now
				st.parseDataRow(r, st.execData)
			}
		default:
			errorf("unknown exec response: %q", t)
		}
	}

	panic("not reached")
}

func (st *stmt) exec(v []driver.Value) {
	w := st.cn.writeBuf('B')
	w.string("")
	w.string(st.name)
	w.int16(0)
	w.int16(len(v))
	for i, x := range v {
		if x == nil {
			w.int32(-1)
		} else {
			b := encode(x, st.paramTyps[i])
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
			return
		case m_readyForQuery:
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
}

func (st *stmt) NumInput() int {
	return len(st.paramTyps)
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
		dest[i] = decode(r.next(l), st.rowTyps[i])
	}
}

func parseComplete(s string) int64 {
	parts := strings.Split(s, " ")
	n, _ := strconv.ParseInt(parts[len(parts)-1], 10, 64)
	return n
}

type result struct {
	rowsAffected int64
	lastInsertId int64
	idReturned   bool
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

func createResult(rowsAffected int64, execData []driver.Value) driver.Result {

	res := new(result)
	res.idReturned = false
	res.rowsAffected = rowsAffected

	// take the first int64 as the id
	for _, v := range execData {
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

	defer errRecover(&err)

	for {
		t, r := rs.st.cn.recv1()
		switch t {
		case m_error:
			err = parseError(r)
		case m_commandComplete, m_parameterStatus, m_notice:
			continue
		case m_readyForQuery:
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
