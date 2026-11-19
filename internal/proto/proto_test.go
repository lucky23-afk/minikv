package proto

import (
	"bytes"
	"strings"
	"testing"
)

func TestWriter_WriteOK(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.WriteOK(); err != nil {
		t.Fatal(err)
	}
	if got := buf.String(); got != "+OK\r\n" {
		t.Fatalf("got %q", got)
	}
}

func TestWriter_WriteError(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.WriteError("not found")
	if got := buf.String(); got != "-ERR not found\r\n" {
		t.Fatalf("got %q", got)
	}
}

func TestWriter_WriteInt(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.WriteInt(42)
	if got := buf.String(); got != ":42\r\n" {
		t.Fatalf("got %q", got)
	}
}

func TestWriter_WriteBulk(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.WriteBulk([]byte("hello"))
	want := "$5\r\nhello\r\n"
	if got := buf.String(); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestWriter_WriteBulk_Nil(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.WriteBulk(nil)
	if got := buf.String(); got != "$-1\r\n" {
		t.Fatalf("got %q", got)
	}
}

func TestWriter_WriteArray(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.WriteArray([][]byte{[]byte("a"), []byte("bb")})
	want := "*2\r\n$1\r\na\r\n$2\r\nbb\r\n"
	if got := buf.String(); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestReader_ReadRequest_GET(t *testing.T) {
	r := NewReader(strings.NewReader("GET mykey\r\n"))
	req, err := r.ReadRequest()
	if err != nil {
		t.Fatal(err)
	}
	if req.Command != "GET" {
		t.Fatalf("command: got %q, want GET", req.Command)
	}
	if len(req.Args) != 1 || req.Args[0] != "mykey" {
		t.Fatalf("args: %v", req.Args)
	}
}

func TestReader_ReadRequest_PUT(t *testing.T) {
	raw := "PUT mykey 1000\r\n5\r\nhello"
	r := NewReader(strings.NewReader(raw))
	req, err := r.ReadRequest()
	if err != nil {
		t.Fatal(err)
	}
	if req.Command != "PUT" {
		t.Fatalf("command: %q", req.Command)
	}
	if string(req.Value) != "hello" {
		t.Fatalf("value: %q", req.Value)
	}
}

func TestReader_ReadRequest_DEL(t *testing.T) {
	r := NewReader(strings.NewReader("DEL mykey\r\n"))
	req, err := r.ReadRequest()
	if err != nil {
		t.Fatal(err)
	}
	if req.Command != "DEL" {
		t.Fatalf("command: %q", req.Command)
	}
}

func TestReader_ReadRequest_Empty(t *testing.T) {
	r := NewReader(strings.NewReader("\r\n"))
	_, err := r.ReadRequest()
	if err == nil {
		t.Fatal("expected error on empty line")
	}
}
