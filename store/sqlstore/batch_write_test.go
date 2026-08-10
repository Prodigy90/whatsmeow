// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

// recordingDB is a fake database/sql driver that records executed queries, their
// args, and transaction begins, so tests can assert on statement shape (UNNEST vs
// placeholders), statement count, and the absence of explicit transactions.
type recordingDB struct {
	execs  []recordedStatement
	begins int
	rows   [][]driver.Value // rows returned by the next QueryContext
	cols   []string
}

type recordedStatement struct {
	query string
	args  []driver.NamedValue
}

type recordingConnector struct{ state *recordingDB }

func (c *recordingConnector) Connect(context.Context) (driver.Conn, error) {
	return &recordingConn{state: c.state}, nil
}

func (*recordingConnector) Driver() driver.Driver { return recordingDriver{} }

type recordingDriver struct{}

func (recordingDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("use connector")
}

type recordingConn struct{ state *recordingDB }

func (*recordingConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}

func (*recordingConn) Close() error { return nil }

func (c *recordingConn) Begin() (driver.Tx, error) {
	c.state.begins++
	return recordingTx{}, nil
}

type recordingTx struct{}

func (recordingTx) Commit() error   { return nil }
func (recordingTx) Rollback() error { return nil }

func (c *recordingConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.state.execs = append(c.state.execs, recordedStatement{query: query, args: append([]driver.NamedValue(nil), args...)})
	return driver.RowsAffected(int64(len(args))), nil
}

func (c *recordingConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.state.execs = append(c.state.execs, recordedStatement{query: query, args: append([]driver.NamedValue(nil), args...)})
	return &recordingRows{cols: c.state.cols, rows: c.state.rows}, nil
}

type recordingRows struct {
	cols []string
	rows [][]driver.Value
	idx  int
}

func (r *recordingRows) Columns() []string { return r.cols }
func (r *recordingRows) Close() error      { return nil }

func (r *recordingRows) Next(dest []driver.Value) error {
	if r.idx >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.idx])
	r.idx++
	return nil
}

// testArrayValuer stands in for pq.Array in tests: it renders the wrapped slice
// with %v so assertions can check content and ordering from the recorded arg.
type testArrayValuer struct{ wrapped any }

func (v testArrayValuer) Value() (driver.Value, error) { return fmt.Sprintf("%v", v.wrapped), nil }
func (v testArrayValuer) Scan(any) error               { return errors.New("not implemented") }

func withTestArrayWrapper(t *testing.T) {
	t.Helper()
	prev := PostgresArrayWrapper
	PostgresArrayWrapper = func(val any) interface {
		driver.Valuer
		sql.Scanner
	} {
		return testArrayValuer{wrapped: val}
	}
	t.Cleanup(func() { PostgresArrayWrapper = prev })
}

func newRecordingStore(t *testing.T, dialect string, state *recordingDB) *SQLStore {
	t.Helper()
	db := sql.OpenDB(&recordingConnector{state: state})
	t.Cleanup(func() { _ = db.Close() })
	return NewSQLStore(NewWithDB(db, dialect, nil), types.NewJID("15550000000", types.DefaultUserServer))
}

func TestPutManySessionsPostgresUsesSingleUnnestStatementWithoutTxn(t *testing.T) {
	withTestArrayWrapper(t)
	state := &recordingDB{}
	s := newRecordingStore(t, "postgres", state)

	err := s.PutManySessions(context.Background(), map[string][]byte{
		"222:1": []byte("s2"),
		"111:1": []byte("s1"),
		"333:1": []byte("s3"),
	})
	if err != nil {
		t.Fatalf("PutManySessions: %v", err)
	}
	if state.begins != 0 {
		t.Errorf("expected no transaction, got %d begins", state.begins)
	}
	if len(state.execs) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(state.execs))
	}
	stmt := state.execs[0]
	if !strings.Contains(stmt.query, "UNNEST") {
		t.Errorf("expected UNNEST query, got: %s", stmt.query)
	}
	if len(stmt.args) != 3 {
		t.Fatalf("expected 3 binds (jid, ids, sessions), got %d", len(stmt.args))
	}
	if ids, ok := stmt.args[1].Value.(string); !ok || ids != "[111:1 222:1 333:1]" {
		t.Errorf("expected sorted address array, got %v", stmt.args[1].Value)
	}
	// Blob order must match the sorted address order.
	if blobs, ok := stmt.args[2].Value.(string); !ok || blobs != "[[115 49] [115 50] [115 51]]" {
		t.Errorf("expected blobs in sorted-address order, got %v", stmt.args[2].Value)
	}
}

func TestPutManySessionsSingleEntryUsesPlainPut(t *testing.T) {
	withTestArrayWrapper(t)
	state := &recordingDB{}
	s := newRecordingStore(t, "postgres", state)

	err := s.PutManySessions(context.Background(), map[string][]byte{"111:1": []byte("s1")})
	if err != nil {
		t.Fatalf("PutManySessions: %v", err)
	}
	if state.begins != 0 || len(state.execs) != 1 {
		t.Fatalf("expected 1 plain statement without txn, got %d stmts / %d begins", len(state.execs), state.begins)
	}
	if strings.Contains(state.execs[0].query, "UNNEST") {
		t.Errorf("single-entry put should not use UNNEST: %s", state.execs[0].query)
	}
}

func TestPutManySessionsGenericSingleChunkSkipsTxn(t *testing.T) {
	state := &recordingDB{}
	s := newRecordingStore(t, "sqlite3", state)

	err := s.PutManySessions(context.Background(), map[string][]byte{
		"222:1": []byte("s2"),
		"111:1": []byte("s1"),
	})
	if err != nil {
		t.Fatalf("PutManySessions: %v", err)
	}
	if state.begins != 0 {
		t.Errorf("expected no transaction for a single chunk, got %d begins", state.begins)
	}
	if len(state.execs) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(state.execs))
	}
	// jid + 2 args per row
	if len(state.execs[0].args) != 5 {
		t.Errorf("expected 5 binds, got %d", len(state.execs[0].args))
	}
}

func TestPutManyIdentitiesPostgresUsesSingleUnnestStatementWithoutTxn(t *testing.T) {
	withTestArrayWrapper(t)
	state := &recordingDB{}
	s := newRecordingStore(t, "postgres", state)

	err := s.PutManyIdentities(context.Background(), map[string][32]byte{
		"222:1": {2},
		"111:1": {1},
	})
	if err != nil {
		t.Fatalf("PutManyIdentities: %v", err)
	}
	if state.begins != 0 {
		t.Errorf("expected no transaction, got %d begins", state.begins)
	}
	if len(state.execs) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(state.execs))
	}
	stmt := state.execs[0]
	if !strings.Contains(stmt.query, "UNNEST") {
		t.Errorf("expected UNNEST query, got: %s", stmt.query)
	}
	if ids, ok := stmt.args[1].Value.(string); !ok || ids != "[111:1 222:1]" {
		t.Errorf("expected sorted address array, got %v", stmt.args[1].Value)
	}
}

func TestPutMessageSecretsSingleChunkSkipsTxn(t *testing.T) {
	state := &recordingDB{}
	s := newRecordingStore(t, "postgres", state)

	chat := types.NewJID("120363000000000000", types.GroupServer)
	sender := types.NewJID("15550000001", types.DefaultUserServer)
	err := s.PutMessageSecrets(context.Background(), []store.MessageSecretInsert{
		{Chat: chat, Sender: sender, ID: "MSG1", Secret: []byte("k1")},
		{Chat: chat, Sender: sender, ID: "MSG2", Secret: []byte("k2")},
	})
	if err != nil {
		t.Fatalf("PutMessageSecrets: %v", err)
	}
	if state.begins != 0 {
		t.Errorf("expected no transaction for a single chunk, got %d begins", state.begins)
	}
	if len(state.execs) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(state.execs))
	}
}

func TestIterateSessionsStreamsRows(t *testing.T) {
	withTestArrayWrapper(t)
	state := &recordingDB{
		cols: []string{"their_id", "session"},
		rows: [][]driver.Value{
			{"111:1", []byte("s1")},
			{"222:1", nil},
		},
	}
	s := newRecordingStore(t, "postgres", state)

	var got []string
	err := s.IterateSessions(context.Background(), []string{"111:1", "222:1", "333:1"}, func(addr string, blob []byte) error {
		got = append(got, fmt.Sprintf("%s=%s", addr, blob))
		return nil
	})
	if err != nil {
		t.Fatalf("IterateSessions: %v", err)
	}
	if len(got) != 2 || got[0] != "111:1=s1" || got[1] != "222:1=" {
		t.Errorf("unexpected rows: %v", got)
	}
}

func TestIterateSessionCallbackErrorPropagates(t *testing.T) {
	state := &recordingDB{
		cols: []string{"session"},
		rows: [][]driver.Value{{[]byte("bad")}},
	}
	s := newRecordingStore(t, "postgres", state)

	wantErr := errors.New("deserialize failed")
	found, err := s.IterateSession(context.Background(), "111:1", func([]byte) error {
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Errorf("expected callback error to propagate, got %v", err)
	}
	if found {
		t.Errorf("expected found=false on callback error")
	}
}
