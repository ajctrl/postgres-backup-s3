package backup

import (
	"context"
	"fmt"
)

// The preflight and dump use separate connections. Bind the dump to the
// verified role in its URI, overriding environment/service-file preferences.
func (a *App) dumpConnectionArgs(database string, standby bool) []string {
	role := "primary"
	if standby {
		role = "standby"
	}
	args := a.connectionArgs(database)
	args[1] += "?target_session_attrs=" + role
	return args
}

// Check every selected database, since UNLOGGED relations are database-local.
// Requiring this query to succeed avoids treating an unknown source as complete.
func (a *App) checkBackupSource(ctx context.Context, runner commandRunner, database string) (bool, error) {
	args := append([]string{"-X", "-A", "-t", "-v", "ON_ERROR_STOP=1"}, a.connectionArgs(database)...)
	args = append(args, "-c", `SELECT pg_catalog.json_build_object('standby', pg_catalog.pg_is_in_recovery(), 'unlogged', EXISTS (
SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relpersistence = 'u' AND n.nspname NOT IN ('pg_catalog', 'information_schema')
AND n.nspname !~ '^pg_(toast|temp)_'));`)
	var state struct {
		Standby  *bool `json:"standby"`
		Unlogged *bool `json:"unlogged"`
	}
	if err := runner.queryJSON(ctx, &state, args...); err != nil {
		return false, fmt.Errorf("cannot verify backup source: %w", err)
	}
	if state.Standby == nil || state.Unlogged == nil {
		return false, fmt.Errorf("incomplete backup source metadata")
	}
	if *state.Standby {
		if *state.Unlogged && !a.Config.AllowPartial {
			return true, fmt.Errorf("standby omits UNLOGGED contents; use the primary or explicitly set BACKUP_ALLOW_PARTIAL=true")
		}
		if a.Config.AllowPartial {
			fmt.Fprintln(a.Err, "WARNING: standby backup explicitly allowed; UNLOGGED contents are omitted, including relations created after the source check.")
		}
	}
	return *state.Standby, nil
}
