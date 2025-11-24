package main

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestTeeWriter(t *testing.T) {
	t.Run("writes to all writers", func(t *testing.T) {
		var buf1, buf2, buf3 bytes.Buffer
		tee := NewTeeWriter(&buf1, &buf2, &buf3)

		data := []byte("hello world")
		n, err := tee.Write(data)
		if err != nil {
			t.Fatalf("Write returned error: %v", err)
		}
		if n != len(data) {
			t.Errorf("Write returned %d, expected %d", n, len(data))
		}

		if buf1.String() != "hello world" {
			t.Errorf("buf1 = %q, expected %q", buf1.String(), "hello world")
		}
		if buf2.String() != "hello world" {
			t.Errorf("buf2 = %q, expected %q", buf2.String(), "hello world")
		}
		if buf3.String() != "hello world" {
			t.Errorf("buf3 = %q, expected %q", buf3.String(), "hello world")
		}
	})

	t.Run("handles nil writers", func(t *testing.T) {
		var buf bytes.Buffer
		tee := NewTeeWriter(&buf, nil)

		data := []byte("test")
		n, err := tee.Write(data)
		if err != nil {
			t.Fatalf("Write returned error: %v", err)
		}
		if n != len(data) {
			t.Errorf("Write returned %d, expected %d", n, len(data))
		}
		if buf.String() != "test" {
			t.Errorf("buf = %q, expected %q", buf.String(), "test")
		}
	})

	t.Run("handles empty writers list", func(t *testing.T) {
		tee := NewTeeWriter()
		data := []byte("test")
		n, err := tee.Write(data)
		if err != nil {
			t.Fatalf("Write returned error: %v", err)
		}
		if n != len(data) {
			t.Errorf("Write returned %d, expected %d", n, len(data))
		}
	})

	t.Run("propagates write errors", func(t *testing.T) {
		errWriter := &errorWriter{err: io.ErrClosedPipe}
		var buf bytes.Buffer
		tee := NewTeeWriter(&buf, errWriter)

		data := []byte("test")
		_, err := tee.Write(data)
		if err != io.ErrClosedPipe {
			t.Errorf("Write returned error %v, expected %v", err, io.ErrClosedPipe)
		}
		// First writer should still receive the data
		if buf.String() != "test" {
			t.Errorf("buf = %q, expected %q", buf.String(), "test")
		}
	})
}

func TestSplitByContentLengthWriter(t *testing.T) {
	t.Run("writes line without marker as-is", func(t *testing.T) {
		var buf bytes.Buffer
		writer := NewSplitByContentLengthWriter(&buf)

		data := []byte("hello world\n")
		n, err := writer.Write(data)
		if err != nil {
			t.Fatalf("Write returned error: %v", err)
		}
		if n != len(data) {
			t.Errorf("Write returned %d, expected %d", n, len(data))
		}

		if buf.String() != "hello world\n" {
			t.Errorf("buf = %q, expected %q", buf.String(), "hello world\n")
		}
	})

	t.Run("inserts newline before Content-Length marker", func(t *testing.T) {
		var buf bytes.Buffer
		writer := NewSplitByContentLengthWriter(&buf)

		data := []byte("prefix Content-Length: 123\n")
		n, err := writer.Write(data)
		if err != nil {
			t.Fatalf("Write returned error: %v", err)
		}
		if n != len(data) {
			t.Errorf("Write returned %d, expected %d", n, len(data))
		}

		expected := "prefix \nContent-Length: 123\n"
		if buf.String() != expected {
			t.Errorf("buf = %q, expected %q", buf.String(), expected)
		}
	})

	t.Run("handles Content-Length at start of line", func(t *testing.T) {
		var buf bytes.Buffer
		writer := NewSplitByContentLengthWriter(&buf)

		data := []byte("Content-Length: 456\n")
		_, err := writer.Write(data)
		if err != nil {
			t.Fatalf("Write returned error: %v", err)
		}

		expected := "\nContent-Length: 456\n"
		if buf.String() != expected {
			t.Errorf("buf = %q, expected %q", buf.String(), expected)
		}
	})

	t.Run("handles multiple lines", func(t *testing.T) {
		var buf bytes.Buffer
		writer := NewSplitByContentLengthWriter(&buf)

		data := []byte("line1\nline2 Content-Length: 789\nline3\n")
		_, err := writer.Write(data)
		if err != nil {
			t.Fatalf("Write returned error: %v", err)
		}

		expected := "line1\nline2 \nContent-Length: 789\nline3\n"
		if buf.String() != expected {
			t.Errorf("buf = %q, expected %q", buf.String(), expected)
		}
	})

	t.Run("buffers incomplete lines", func(t *testing.T) {
		var buf bytes.Buffer
		writer := NewSplitByContentLengthWriter(&buf)

		data := []byte("incomplete")
		n, err := writer.Write(data)
		if err != nil {
			t.Fatalf("Write returned error: %v", err)
		}
		if n != len(data) {
			t.Errorf("Write returned %d, expected %d", n, len(data))
		}

		// Should be buffered, nothing written yet
		if buf.Len() != 0 {
			t.Errorf("buf should be empty, got %q", buf.String())
		}

		// Complete the line
		data2 := []byte(" line\n")
		n2, err := writer.Write(data2)
		if err != nil {
			t.Fatalf("Write returned error: %v", err)
		}

		expected := "incomplete line\n"
		if buf.String() != expected {
			t.Errorf("buf = %q, expected %q", buf.String(), expected)
		}
		if n2 != len(data2) {
			t.Errorf("Write returned %d, expected %d", n2, len(data2))
		}
	})

	t.Run("Flush writes incomplete buffer with Content-Length header", func(t *testing.T) {
		var buf bytes.Buffer
		writer := NewSplitByContentLengthWriter(&buf)

		data := []byte("incomplete buffer")
		_, err := writer.Write(data)
		if err != nil {
			t.Fatalf("Write returned error: %v", err)
		}

		err = writer.Flush()
		if err != nil {
			t.Fatalf("Flush returned error: %v", err)
		}

		// Should write Content-Length header and the buffer
		output := buf.String()
		if !strings.Contains(output, "Content-Length:") {
			t.Errorf("output should contain Content-Length header, got %q", output)
		}
		if !strings.Contains(output, "incomplete buffer") {
			t.Errorf("output should contain buffered data, got %q", output)
		}
	})

	t.Run("Flush on empty buffer is no-op", func(t *testing.T) {
		var buf bytes.Buffer
		writer := NewSplitByContentLengthWriter(&buf)

		err := writer.Flush()
		if err != nil {
			t.Fatalf("Flush returned error: %v", err)
		}

		if buf.Len() != 0 {
			t.Errorf("buf should be empty, got %q", buf.String())
		}
	})

	t.Run("handles Content-Length marker in incomplete line", func(t *testing.T) {
		var buf bytes.Buffer
		writer := NewSplitByContentLengthWriter(&buf)

		data := []byte("prefix Content-Length: 999")
		_, err := writer.Write(data)
		if err != nil {
			t.Fatalf("Write returned error: %v", err)
		}

		// Complete the line
		data2 := []byte(" suffix\n")
		_, err = writer.Write(data2)
		if err != nil {
			t.Fatalf("Write returned error: %v", err)
		}

		expected := "prefix \nContent-Length: 999 suffix\n"
		if buf.String() != expected {
			t.Errorf("buf = %q, expected %q", buf.String(), expected)
		}
	})
}

func TestComposedWriters(t *testing.T) {
	t.Run("TeeWriter with SplitByContentLengthWriter", func(t *testing.T) {
		var buf1, buf2 bytes.Buffer
		splitWriter := NewSplitByContentLengthWriter(&buf1)
		tee := NewTeeWriter(splitWriter, &buf2)

		data := []byte("test Content-Length: 123\n")
		_, err := tee.Write(data)
		if err != nil {
			t.Fatalf("Write returned error: %v", err)
		}

		// buf1 should have the split applied
		expected1 := "test \nContent-Length: 123\n"
		if buf1.String() != expected1 {
			t.Errorf("buf1 = %q, expected %q", buf1.String(), expected1)
		}

		// buf2 should have raw data
		if buf2.String() != string(data) {
			t.Errorf("buf2 = %q, expected %q", buf2.String(), string(data))
		}
	})

	t.Run("TimestampedWriter with SplitByContentLengthWriter", func(t *testing.T) {
		var buf bytes.Buffer
		timestamped := NewTimestampedWriter(&buf)
		splitWriter := NewSplitByContentLengthWriter(timestamped)

		data := []byte("prefix Content-Length: 456\n")
		_, err := splitWriter.Write(data)
		if err != nil {
			t.Fatalf("Write returned error: %v", err)
		}

		output := buf.String()
		// Should have timestamp
		if !strings.Contains(output, "prefix") {
			t.Errorf("output should contain prefix, got %q", output)
		}
		// Should have newline before Content-Length
		if !strings.Contains(output, "\nContent-Length:") {
			t.Errorf("output should have newline before Content-Length, got %q", output)
		}
	})

	t.Run("all three writers composed", func(t *testing.T) {
		var buf1, buf2 bytes.Buffer
		timestamped1 := NewTimestampedWriter(&buf1)
		splitWriter := NewSplitByContentLengthWriter(timestamped1)
		tee := NewTeeWriter(splitWriter, &buf2)

		data := []byte("hello Content-Length: 789\n")
		_, err := tee.Write(data)
		if err != nil {
			t.Fatalf("Write returned error: %v", err)
		}

		// buf1 should have timestamp and split
		output1 := buf1.String()
		if !strings.Contains(output1, "hello") {
			t.Errorf("buf1 should contain hello, got %q", output1)
		}
		if !strings.Contains(output1, "\nContent-Length:") {
			t.Errorf("buf1 should have newline before Content-Length, got %q", output1)
		}

		// buf2 should have raw data
		if buf2.String() != string(data) {
			t.Errorf("buf2 = %q, expected %q", buf2.String(), string(data))
		}
	})
}

// errorWriter is a test helper that always returns an error on Write
type errorWriter struct {
	err error
}

func (e *errorWriter) Write(p []byte) (int, error) {
	return 0, e.err
}

