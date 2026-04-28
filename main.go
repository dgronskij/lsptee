package main

import (
    "bytes"
    "flag"
    "fmt"
    "io"
    "os"
    "os/exec"
    "os/signal"
    "sync"
    "syscall"
    "time"
)

var contentLengthMarker = []byte("Content-Length:")

// TeeWriter implements io.Writer and writes the same data to all provided writers.
type TeeWriter struct {
    writers []io.Writer
}

// NewTeeWriter creates a new TeeWriter that writes to all provided writers.
func NewTeeWriter(writers ...io.Writer) *TeeWriter {
    return &TeeWriter{writers: writers}
}

// Write implements io.Writer. It writes p to all writers.
func (t *TeeWriter) Write(p []byte) (int, error) {
    for _, w := range t.writers {
        if w != nil {
            if _, err := w.Write(p); err != nil {
                return len(p), err
            }
        }
    }
    return len(p), nil
}

// tee reads byte-by-byte from src and writes each byte to the provided writer.
// Returns when src reaches EOF or encounters an error.
func tee(src io.Reader, w io.Writer) error {
    tmp := make([]byte, 1)
    for {
        n, err := src.Read(tmp)
        if n > 0 {
            if _, writeErr := w.Write(tmp[:n]); writeErr != nil {
                return writeErr
            }
        }
        if err != nil {
            if err == io.EOF {
                return nil
            }
            return err
        }
    }
}

// fileSyncer is implemented by *os.File.
type fileSyncer interface {
	Sync() error
}

// SplitByContentLengthWriter wraps an io.Writer and buffers bytes until a newline is encountered.
// For each complete line, it inserts a newline before any "Content-Length:" marker.
// On Flush, it writes incomplete buffers and syncs to disk.
type SplitByContentLengthWriter struct {
	w      io.Writer
	buf    []byte
	syncer fileSyncer // non-nil when underlying file supports Sync
}

// NewSplitByContentLengthWriter creates a new SplitByContentLengthWriter wrapping the given writer.
// It discovers the underlying *os.File (through TimestampedWriter if needed) so it can
// sync completed LSP messages to disk, preventing data loss from in-memory buffering.
func NewSplitByContentLengthWriter(w io.Writer, syncer fileSyncer) *SplitByContentLengthWriter {
	return &SplitByContentLengthWriter{
		w:      w,
		buf:    make([]byte, 0),
		syncer: syncer,
	}
}

// sync flushes the underlying file to disk if a syncer is available.
func (l *SplitByContentLengthWriter) sync() error {
	if l.syncer != nil {
		return l.syncer.Sync()
	}
	return nil
}

// flushAsLine processes a complete line from the buffer and writes it, inserting a newline before any Content-Length marker.
// When a Content-Length marker is found, it syncs to disk first — this ensures the previous LSP message body
// (which had no trailing newline and was thus only flushed when this new header line arrived) is durable.
func (l *SplitByContentLengthWriter) flushAsLine() error {
	// Process complete line
	line := l.buf
	l.buf = l.buf[:0]

	// Find Content-Length marker in the line
	idx := bytes.Index(line, contentLengthMarker)

	if idx == -1 {
		// No marker found, write line as-is
		if _, err := l.w.Write(line); err != nil {
			return err
		}
		return nil
	}

	// A Content-Length marker means a new LSP message is starting.
	// Sync the previous message body to disk before continuing.
	if err := l.sync(); err != nil {
		return err
	}

	// Write everything before the marker (if any)
	if idx > 0 {
		if _, err := l.w.Write(line[:idx]); err != nil {
			return err
		}
	}
	// Insert newline before marker
	if _, err := l.w.Write([]byte{'\n'}); err != nil {
		return err
	}
	// Write marker and rest of line
	if _, err := l.w.Write(line[idx:]); err != nil {
		return err
	}
	return nil
}

// Write implements io.Writer. It splits input by newlines, and for each line,
// inserts a newline before any "Content-Length:" marker.
func (l *SplitByContentLengthWriter) Write(p []byte) (int, error) {
    written := 0

    for _, b := range p {
        l.buf = append(l.buf, b)
        written++

        if b == '\n' {
            if err := l.flushAsLine(); err != nil {
                return written, err
            }
        }
    }

    return written, nil
}

// Flush writes any remaining buffered data and syncs to disk.
func (l *SplitByContentLengthWriter) Flush() error {
	if len(l.buf) > 0 {
		if err := l.flushAsLine(); err != nil {
			return err
		}
	}
	return l.sync()
}

// TimestampedWriter wraps an io.Writer and prepends RFC3339 timestamps to each write.
type TimestampedWriter struct {
    w io.Writer
}

// NewTimestampedWriter creates a new TimestampedWriter wrapping the given writer.
func NewTimestampedWriter(w io.Writer) *TimestampedWriter {
    return &TimestampedWriter{w: w}
}

// Write implements io.Writer. It prepends a timestamp to the data before writing.
func (t *TimestampedWriter) Write(p []byte) (int, error) {
    ts := time.Now().Format(time.RFC3339)
    data := append([]byte(ts+" "), p...)
    n, err := t.w.Write(data)
    // Return the original length, not the timestamped length
    if n > len(ts)+1 {
        return n - len(ts) - 1, err
    }
    return 0, err
}

func main() {
    // Logging flag parsing
    stdinLog := flag.String("stdin-log", "", "Path to stdin log file")
    stdoutLog := flag.String("stdout-log", "", "Path to stdout log file")
    stderrLog := flag.String("stderr-log", "", "Path to stderr log file")
    flag.Parse()

    // Find "--" separator for child process and args
    args := os.Args
    sep := -1
    for i, v := range args {
        if v == "--" {
            sep = i
            break
        }
    }
    if sep == -1 || sep+1 >= len(args) {
        fmt.Fprintln(os.Stderr, "Usage: [flags] -- <binary> [args...]")
        os.Exit(1)
    }
    childArgs := args[sep+1:]

    // Prepare log files
    var stdinLogFile, stdoutLogFile, stderrLogFile *os.File
    var err error
    if *stdinLog != "" {
        stdinLogFile, err = os.OpenFile(*stdinLog, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
        if err != nil {
            fmt.Fprintf(os.Stderr, "Failed to open stdin log: %v\n", err)
            os.Exit(1)
        }
        defer stdinLogFile.Close()
    }
    if *stdoutLog != "" {
        stdoutLogFile, err = os.OpenFile(*stdoutLog, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
        if err != nil {
            fmt.Fprintf(os.Stderr, "Failed to open stdout log: %v\n", err)
            os.Exit(1)
        }
        defer stdoutLogFile.Close()
    }
    if *stderrLog != "" {
        stderrLogFile, err = os.OpenFile(*stderrLog, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
        if err != nil {
            fmt.Fprintf(os.Stderr, "Failed to open stderr log: %v\n", err)
            os.Exit(1)
        }
        defer stderrLogFile.Close()
    }

    cmd := exec.Command(childArgs[0], childArgs[1:]...)
    cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

    // Set up I/O
    childStdin, err := cmd.StdinPipe()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Failed to get stdin pipe: %v\n", err)
        os.Exit(1)
    }
    childStdout, err := cmd.StdoutPipe()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Failed to get child stdout: %v\n", err)
        os.Exit(1)
    }
    childStderr, err := cmd.StderrPipe()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Failed to get child stderr: %v\n", err)
        os.Exit(1)
    }

    // Signal forwarding
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT)
    go func() {
        for sig := range sigChan {
            if cmd.Process != nil {
                syscall.Kill(-cmd.Process.Pid, sig.(syscall.Signal))
            }
        }
    }()

    if err := cmd.Start(); err != nil {
        fmt.Fprintf(os.Stderr, "Failed to start child process: %v\n", err)
        os.Exit(1)
    }

    var wg sync.WaitGroup

    // Build composed writers for logging
    var stdinLogger, stdoutLogger, stderrLogger *SplitByContentLengthWriter
	if stdinLogFile != nil {
		stdinLogger = NewSplitByContentLengthWriter(NewTimestampedWriter(stdinLogFile), stdinLogFile)
	}
	if stdoutLogFile != nil {
		stdoutLogger = NewSplitByContentLengthWriter(NewTimestampedWriter(stdoutLogFile), stdoutLogFile)
	}
	if stderrLogFile != nil {
		stderrLogger = NewSplitByContentLengthWriter(NewTimestampedWriter(stderrLogFile), stderrLogFile)
	}

    // stdin: parent -> childStdin (bytewise), log line/Content-Length as appropriate
    wg.Add(1)
    go func() {
        defer wg.Done()
        defer childStdin.Close()
        writers := []io.Writer{childStdin}
        if stdinLogger != nil {
            writers = append(writers, stdinLogger)
        }
        teeWriter := NewTeeWriter(writers...)
        tee(os.Stdin, teeWriter)
        if stdinLogger != nil {
            stdinLogger.Flush()
        }
    }()

    // stdout: childStdout -> parent (bytewise), log line/Content-Length as appropriate
    wg.Add(1)
    go func() {
        defer wg.Done()
        writers := []io.Writer{os.Stdout}
        if stdoutLogger != nil {
            writers = append(writers, stdoutLogger)
        }
        teeWriter := NewTeeWriter(writers...)
        tee(childStdout, teeWriter)
        if stdoutLogger != nil {
            stdoutLogger.Flush()
        }
    }()

    // stderr: childStderr -> parent (bytewise), log line/Content-Length as appropriate
    wg.Add(1)
    go func() {
        defer wg.Done()
        writers := []io.Writer{os.Stderr}
        if stderrLogger != nil {
            writers = append(writers, stderrLogger)
        }
        teeWriter := NewTeeWriter(writers...)
        tee(childStderr, teeWriter)
        if stderrLogger != nil {
            stderrLogger.Flush()
        }
    }()

    wg.Wait()

    // Retrieve and propagate child exit code
    err = cmd.Wait()
    if err != nil {
        if exiterr, ok := err.(*exec.ExitError); ok {
            if ws, ok := exiterr.Sys().(syscall.WaitStatus); ok {
                os.Exit(ws.ExitStatus())
            }
        }
        os.Exit(1)
    }
    os.Exit(0)
}

