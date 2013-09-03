package pq

import "testing"

func TestStatment(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	st, err := db.Prepare("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}

	st1, err := db.Prepare("SELECT 2")
	if err != nil {
		t.Fatal(err)
	}

	r, err := st.Query()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if !r.Next() {
		t.Fatal("expected row")
	}

	var i int
	err = r.Scan(&i)
	if err != nil {
		t.Fatal(err)
	}

	if i != 1 {
		t.Fatalf("expected 1, got %d", i)
	}

	// st1

	r1, err := st1.Query()
	if err != nil {
		t.Fatal(err)
	}
	defer r1.Close()

	if !r1.Next() {
		if r.Err() != nil {
			t.Fatal(r1.Err())
		}
		t.Fatal("expected row")
	}

	err = r1.Scan(&i)
	if err != nil {
		t.Fatal(err)
	}

	if i != 2 {
		t.Fatalf("expected 2, got %d", i)
	}
}

func Test_StmtReturnId(t *testing.T) {
	db := openTestConn(t)
	defer db.Close()

	_, err := db.Exec(`create temp table a (id bigserial, s varchar)`)

	if err != nil {
		t.Fatal(err)
	}

	// not testing the ability to insert a row...
	res1, _ := db.Exec(`insert into a(s) values ('test1') returning id`)
	res2, _ := db.Exec(`insert into a(s) values ('test2') returning id`)
	res3, _ := db.Exec(`insert into a(s) values ('test3')`)
	res4, _ := db.Exec(`insert into a(id, s) values (42, 'test3') returning id`)

	id1, err := res1.LastInsertId()
	id2, err := res2.LastInsertId()
	id3, err := res3.LastInsertId()
	id4, err := res4.LastInsertId()

	if id1 != 1 {
		t.Errorf("Wrong value returned from from LastInsertId(): %d", id1)
	}

	if id2 != 2 {
		t.Errorf("Wrong value returned from from LastInsertId(): %d", id2)
	}

	// this shouldn't work
	if id3 != 0 {
		t.Errorf("Wrong value returned from from LastInsertId(): %d", id3)
	}

	if id4 != 42 {
		t.Errorf("Wrong value returned from from LastInsertId(): %d", id4)
	}
}
