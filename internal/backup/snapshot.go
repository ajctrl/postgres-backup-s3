package backup

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/netip"
	"os/exec"
	"strings"
	"unicode/utf8"
)

// Release the exporter as soon as pg_dump has emitted its TOC and the catalog checks finish.
// psql emits one compact JSON value per query. ON_ERROR_STOP closes stdout on
// errors, so an incomplete result can never authorize publication.
type catalogSnapshot struct {
	cmd      *exec.Cmd
	in       io.WriteCloser
	out      *bufio.Reader
	id       string
	endpoint snapshotEndpoint
	role     string
}

func openCatalogSnapshot(ctx context.Context, r commandRunner, conn []string, opts *dumpOptions) (*catalogSnapshot, error) {
	args := append([]string{"-XqAt", "-v", "ON_ERROR_STOP=1", "-v", "requested_snapshot=" + opts.snapshot, "-v", "dump_role=" + opts.role}, conn...)
	cmd := r.command(ctx, "psql", append(args, "-f", "-")...)
	cmd.Env = append(cmd.Env, "LC_ALL=C", "LANGUAGE=C", "PGCLIENTENCODING=UTF8")
	in, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		in.Close()
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		in.Close()
		out.Close()
		return nil, err
	}
	s := &catalogSnapshot{cmd: cmd, in: in, out: bufio.NewReader(out), role: opts.role}
	isolation := "REPEATABLE READ READ ONLY"
	if opts.serializable {
		isolation = "SERIALIZABLE READ ONLY DEFERRABLE"
	}
	sql := "SET search_path = pg_catalog, pg_temp; SET client_encoding='UTF8'; BEGIN ISOLATION LEVEL " + isolation + ";\n"
	if opts.snapshot != "" {
		sql += "SET TRANSACTION SNAPSHOT :'requested_snapshot';\n"
	}
	if err := s.query(sql+"SELECT json_build_object('snapshot',pg_export_snapshot(),'host',:'HOST','port',:'PORT','db',:'DBNAME','user',:'USER','client',:VERSION_NUM);", &s.endpoint); err != nil {
		s.close()
		return nil, fmt.Errorf("could not export catalog snapshot: %w", err)
	}
	s.id = s.endpoint.Snapshot
	if s.id == "" || s.endpoint.Host == "" || s.endpoint.Port == "" {
		s.close()
		return nil, fmt.Errorf("incomplete snapshot connection information")
	}
	if err := s.readPeerAddress(); err != nil {
		s.close()
		return nil, fmt.Errorf("identify PostgreSQL snapshot endpoint: %w", err)
	}
	return s, nil
}

func (s *catalogSnapshot) query(sql string, result any) error {
	if _, err := io.WriteString(s.in, strings.TrimSpace(sql)+"\n"); err != nil {
		return err
	}
	line, err := s.out.ReadBytes('\n')
	if err != nil {
		return fmt.Errorf("catalog snapshot query failed: %w", err)
	}
	if !utf8.Valid(line) || json.Unmarshal(line, result) != nil {
		return fmt.Errorf("invalid catalog snapshot response")
	}
	return nil
}

func (s *catalogSnapshot) close() {
	if s.in == nil {
		return
	}
	s.in.Close() // EOF closes psql and rolls back the read-only transaction.
	s.cmd.Wait()
	s.in = nil
}

type snapshotEndpoint struct {
	Snapshot, Host, Port, DB, User string
	Client                         int
	address                        string
}

// HOST and PORT are libpq's selected values. psql exposes PQhostaddr only
// through conninfo. Read the client peer so TLS names and proxy ports survive.
func (s *catalogSnapshot) readPeerAddress() error {
	end := rand.Text()
	if _, err := io.WriteString(s.in, "\\pset format csv\n\\conninfo\n\\pset format unaligned\n\\echo "+end+"\n"); err != nil {
		return err
	}
	var report strings.Builder
	for {
		line, err := s.out.ReadString('\n')
		if err != nil {
			return err
		}
		if strings.TrimSuffix(line, "\n") == end {
			break
		}
		report.WriteString(line)
	}
	addr, err := s.endpoint.peerAddress(report.String())
	s.endpoint.address = addr
	return err
}

func (e snapshotEndpoint) peerAddress(report string) (string, error) {
	var addr string
	socket := strings.HasPrefix(e.Host, "/") || strings.HasPrefix(e.Host, "@")
	if e.Client >= 180000 {
		rows, err := csv.NewReader(strings.NewReader(report)).ReadAll()
		if err != nil {
			return "", fmt.Errorf("invalid psql connection information")
		}
		for _, row := range rows {
			if len(row) == 2 && row[0] == "Host Address" {
				addr = row[1]
				break
			}
		}
	} else {
		// psql prints the names verbatim, even when they contain quotes.
		prefix := "You are connected to database \"" + e.DB + "\" as user \"" + e.User + "\" "
		info, ok := strings.CutPrefix(report, prefix)
		if !ok {
			return "", fmt.Errorf("unexpected psql connection information")
		}
		host := "on host \"" + e.Host + "\""
		switch {
		case strings.HasPrefix(info, host+" (address \""):
			addr, _, _ = strings.Cut(strings.TrimPrefix(info, host+" (address \""), "\"")
		case strings.HasPrefix(info, "on address \""):
			addr, _, _ = strings.Cut(strings.TrimPrefix(info, "on address \""), "\"")
		}
	}
	if addr == "" {
		if socket {
			return "", nil
		}
		addr = e.Host // libpq omits the separate address when host is already numeric.
	}
	if _, err := netip.ParseAddr(addr); err != nil {
		return "", fmt.Errorf("missing numeric address for PostgreSQL connection")
	}
	return addr, nil
}

func (e snapshotEndpoint) connectionURI(uri string) string {
	return uri + "&host=" + encodeComponent(e.Host) + "&port=" + encodeComponent(e.Port) + "&hostaddr=" + encodeComponent(e.address)
}
