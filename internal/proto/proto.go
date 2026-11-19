// Package proto defines the line-based TCP protocol used between the
// coordinator and storage nodes (RESP-like but simpler).
//
// Request wire format:
//
//	<COMMAND> [<arg1> [<arg2>]]\r\n
//	[<value_length>\r\n]
//	[<value_bytes>]
//
// Response wire format:
//
//	+OK\r\n                      simple success
//	$<len>\r\n<data>\r\n         bulk string
//	:<integer>\r\n               integer
//	-ERR <message>\r\n           error
package proto

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const crlf = "\r\n"

// Writer writes protocol responses to an io.Writer.
type Writer struct {
	w io.Writer
}

// NewWriter returns a Writer wrapping w.
func NewWriter(w io.Writer) *Writer { return &Writer{w: w} }

// WriteOK writes "+OK\r\n".
func (w *Writer) WriteOK() error {
	_, err := fmt.Fprintf(w.w, "+OK%s", crlf)
	return err
}

// WriteError writes "-ERR <msg>\r\n".
func (w *Writer) WriteError(msg string) error {
	_, err := fmt.Fprintf(w.w, "-ERR %s%s", msg, crlf)
	return err
}

// WriteInt writes ":<n>\r\n".
func (w *Writer) WriteInt(n int64) error {
	_, err := fmt.Fprintf(w.w, ":%d%s", n, crlf)
	return err
}

// WriteBulk writes "$<len>\r\n<data>\r\n". A nil data slice writes
// "$-1\r\n" (null bulk string).
func (w *Writer) WriteBulk(data []byte) error {
	if data == nil {
		_, err := fmt.Fprintf(w.w, "$-1%s", crlf)
		return err
	}
	if _, err := fmt.Fprintf(w.w, "$%d%s", len(data), crlf); err != nil {
		return err
	}
	if _, err := w.w.Write(data); err != nil {
		return err
	}
	_, err := fmt.Fprint(w.w, crlf)
	return err
}

// WriteArray writes "*<n>\r\n" followed by n bulk strings.
func (w *Writer) WriteArray(items [][]byte) error {
	if _, err := fmt.Fprintf(w.w, "*%d%s", len(items), crlf); err != nil {
		return err
	}
	for _, item := range items {
		if err := w.WriteBulk(item); err != nil {
			return err
		}
	}
	return nil
}

// Request is a parsed client request.
type Request struct {
	Command string   // upper-cased command name
	Args    []string // positional string arguments (key, ttl, etc.)
	Value   []byte   // binary payload (for PUT)
}

// Reader parses requests from an io.Reader.
type Reader struct {
	r *bufio.Reader
}

// NewReader returns a Reader wrapping r.
func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReader(r)}
}

// ReadRequest reads one request from the stream.
//
// Line format:
//
//	<CMD> [<arg1>] [<arg2>]\r\n
//	[<value_length>\r\n<value_bytes>]   -- only when CMD==PUT
func (r *Reader) ReadRequest() (*Request, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}

	parts := strings.Fields(line)
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty request line")
	}

	req := &Request{
		Command: strings.ToUpper(parts[0]),
		Args:    parts[1:],
	}

	// PUT sends a value payload prefixed by its byte length.
	if req.Command == "PUT" {
		lenLine, err := r.readLine()
		if err != nil {
			return nil, fmt.Errorf("proto: reading value length: %w", err)
		}
		n, err := strconv.Atoi(lenLine)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("proto: invalid value length %q", lenLine)
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(r.r, buf); err != nil {
			return nil, fmt.Errorf("proto: reading value: %w", err)
		}
		req.Value = buf
	}

	return req, nil
}

// readLine reads a \r\n terminated line and returns it without the
// trailing \r\n.
func (r *Reader) readLine() (string, error) {
	line, err := r.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimRight(line, "\r\n")
	return line, nil
}

// FormatPUT formats a PUT request line + value payload.
func FormatPUT(key string, ttlMs int64, value []byte) string {
	return fmt.Sprintf("PUT %s %d%s%d%s%s", key, ttlMs, crlf, len(value), crlf, value)
}
