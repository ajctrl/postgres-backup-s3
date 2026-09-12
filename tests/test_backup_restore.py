"""Exercise the shell entrypoints without accessing PostgreSQL or S3."""
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[1]

FAKE_COMMAND = r'''
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import time
from urllib.parse import unquote, urlparse

command = Path(sys.argv[0]).name
args = sys.argv[1:]
if args[:1] == ["--endpoint-url"]:
    args = args[2:]
config = json.loads(os.environ["FAKE_CONFIG"])
if command == "date":
    now = datetime(2026, 9, 12, 12, 0, 0, tzinfo=timezone.utc)
    if args == ["+%s"]:
        print(int(now.timestamp()))
    else:
        if "-d" in args:
            now = datetime.fromtimestamp(int(args[args.index("-d") + 1][1:]), timezone.utc)
        print(now.strftime(args[-1][1:]))
    sys.exit(0)
database = None
if command in ("pg_dump", "pg_restore", "psql"):
    database = unquote(urlparse(args[args.index("-d") + 1]).path[1:])
with open(os.environ["FAKE_LOG"], "a") as log:
    log.write(json.dumps({"command": command, "args": args, "database": database,
                          "original_args": sys.argv[1:]}) + "\n")
if command == config.get("pause_command") and (
        not config.get("pause_action") or args[:2] == config["pause_action"]):
    Path(config["pause_entered"]).touch()
    deadline = time.monotonic() + 15
    while not Path(config["pause_release"]).exists():
        if time.monotonic() > deadline:
            raise TimeoutError("test did not release paused command")
        time.sleep(0.01)
if command == "psql":
    if config.get("discovery_failure"):
        sys.exit(1)
    print(json.dumps(config.get("databases", ["app", "postgres"])))
elif command == "pg_dump":
    if database == config.get("dump_failure"):
        print("partial dump")
        sys.exit(1)
    print("PGDMP " + database)
elif command == "pg_restore":
    assert Path(args[-1]).read_text().startswith("PGDMP")
    if config.get("restore_failure"):
        sys.exit(1)
elif command == "gpg":
    source = Path(args[-1])
    data = source.read_text()
    if config.get("gpg_failure") and config["gpg_failure"] in data:
        sys.exit(1)
    if "--decrypt" in args:
        print(data)
    else:
        Path(str(source) + ".gpg").write_text(data)
elif command == "aws":
    action = args[:2]
    if action == ["s3api", "get-bucket-versioning"]:
        if config.get("versioning_failure"):
            sys.exit(1)
        print(json.dumps(config.get("versioning", {"Status": "Enabled"})))
    elif action == ["s3api", "list-objects-v2"]:
        if config.get("list_failure"):
            sys.exit(1)
        print(json.dumps({"Contents": config.get("objects", [])}))
    elif action == ["s3", "cp"]:
        source, destination = args[2:4]
        if source.startswith("s3://"):
            if config.get("download_failure"):
                sys.exit(1)
            Path(destination).write_text("PGDMP restored")
        else:
            assert Path(source).read_text().startswith("PGDMP")
            if config.get("upload_failure") and config["upload_failure"] in destination:
                sys.exit(1)
    elif action == ["s3", "rm"]:
        if config.get("delete_failure"):
            sys.exit(1)
    elif action == ["s3api", "get-object"]:
        if config.get("download_failure"):
            sys.exit(1)
        Path(args[-1]).write_text("PGDMP restored")
        print("{}")
    else:
        raise AssertionError(args)
else:
    raise AssertionError(command)
'''


@unittest.skipUnless(shutil.which("jq") and shutil.which("flock"), "jq and flock are required")
class BackupRestoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.runtime = self.root / "runtime"
        self.runtime.mkdir()
        self.log = self.root / "calls.jsonl"
        fake = self.bin / "fake"
        fake.write_text("#!" + sys.executable + "\n" + FAKE_COMMAND)
        fake.chmod(0o755)
        for name in ("aws", "psql", "pg_dump", "pg_restore", "gpg", "date"):
            (self.bin / name).symlink_to(fake)

    def script_env(self, config=None, **settings):
        env = {key: value for key, value in os.environ.items()
               if not key.startswith(("POSTGRES_", "S3_", "BACKUP_", "PG", "AWS_"))
               and key != "PASSPHRASE"}
        env.update(PATH=str(self.bin) + os.pathsep + os.environ["PATH"],
                   TMPDIR=str(self.runtime), FAKE_CONFIG=json.dumps(config or {}),
                   FAKE_LOG=str(self.log), S3_BUCKET="test-bucket", S3_PREFIX="backup",
                   POSTGRES_HOST="postgres", POSTGRES_USER="backup", POSTGRES_PASSWORD="test")
        env.update(settings)
        return env

    def run_script(self, script="backup", args=(), config=None, **settings):
        self.log.write_text("")
        env = self.script_env(config, **settings)
        result = subprocess.run([os.environ.get("TEST_SHELL", "sh"), str(ROOT / "src" / (script + ".sh")), *args],
                                env=env, cwd=self.root, text=True, capture_output=True)
        self.calls = [json.loads(line) for line in self.log.read_text().splitlines()]
        self.assertEqual(list(self.runtime.iterdir()), [], "temporary files leaked")
        return result

    def calls_for(self, command, action=None):
        return [call for call in self.calls if call["command"] == command
                and (action is None or call["args"][:2] == action)]

    def assert_success(self, result):
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_legacy_single_database(self):
        self.assert_success(self.run_script(POSTGRES_DATABASE="app"))
        self.assertEqual(self.calls_for("pg_dump")[0]["database"], "app")
        self.assertRegex(self.calls_for("aws", ["s3", "cp"])[0]["args"][3],
                         r"^s3://test-bucket/backup/app_\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.dump$")

    def test_list_order_duplicates_literal_all_and_precedence(self):
        self.assert_success(self.run_script(POSTGRES_DATABASE="ignored",
                            POSTGRES_DATABASES=" billing, ALL,app,billing "))
        self.assertEqual([c["database"] for c in self.calls_for("pg_dump")], ["billing", "ALL", "app"])
        self.assertFalse(self.calls_for("psql"))
        self.assertEqual([c["command"] for c in self.calls],
                         ["pg_dump", "aws", "pg_dump", "aws", "pg_dump", "aws"])

    def test_names_are_not_connection_strings_or_shell_words(self):
        for name in ["with space", "ALL", "with,comma", "a'b", "a/b", "host=other", "$(oops)*"]:
            with self.subTest(name=name):
                self.assert_success(self.run_script(POSTGRES_DATABASE=name))
                self.assertEqual(self.calls_for("pg_dump")[0]["database"], name)

    def test_all_discovery_and_exact_exclusions(self):
        self.assert_success(self.run_script(POSTGRES_BACKUP_ALL="true",
                            POSTGRES_MAINTENANCE_DB="maintenance db", POSTGRES_DATABASES_EXCLUDE="scratch",
                            config={"databases": ["ALL", "app", "postgres", "scratch", "scratchpad"]}))
        self.assertEqual([c["database"] for c in self.calls_for("pg_dump")],
                         ["ALL", "app", "postgres", "scratchpad"])
        discovery = self.calls_for("psql")[0]
        self.assertEqual(discovery["database"], "maintenance db")
        self.assertIn("NOT datistemplate AND datallowconn", discovery["args"][-1])
        self.assertIn("ORDER BY datname", discovery["args"][-1])
        self.assertIn("ON_ERROR_STOP=1", discovery["args"])

    def test_invalid_selections_never_dump(self):
        cases = [{}, {"POSTGRES_BACKUP_ALL": "ALL"},
                 {"POSTGRES_BACKUP_ALL": "true", "POSTGRES_DATABASE": "app"},
                 {"POSTGRES_BACKUP_ALL": "true", "POSTGRES_DATABASES": "app"},
                 {"POSTGRES_DATABASES": "app,,billing"}, {"POSTGRES_DATABASES": "app,"},
                 {"POSTGRES_DATABASE": "app\nother"},
                 {"POSTGRES_DATABASE": "app", "POSTGRES_DATABASES_EXCLUDE": "other"}]
        for settings in cases:
            with self.subTest(settings=settings):
                self.assertNotEqual(self.run_script(**settings).returncode, 0)
                self.assertFalse(self.calls_for("pg_dump"))

    def test_empty_failed_or_invalid_discovery_never_dumps(self):
        for config in [{"databases": []}, {"databases": [""]}, {"databases": ["a\nb"]},
                       {"databases": {}}, {"discovery_failure": True}]:
            with self.subTest(config=config):
                self.assertNotEqual(self.run_script(POSTGRES_BACKUP_ALL="true", config=config).returncode, 0)
                self.assertFalse(self.calls_for("pg_dump"))
        self.assertNotEqual(self.run_script(POSTGRES_BACKUP_ALL="true",
                            POSTGRES_DATABASES_EXCLUDE="app,postgres").returncode, 0)
        self.assertFalse(self.calls_for("pg_dump"))

    def test_fixed_keys_and_encryption(self):
        for passphrase, suffix in [("", ".dump"), ("test", ".dump.gpg")]:
            with self.subTest(suffix=suffix):
                self.assert_success(self.run_script(POSTGRES_DATABASES="app,ALL",
                                    BACKUP_FILENAME_MODE="fixed", PASSPHRASE=passphrase))
                self.assertEqual(self.calls[0]["args"][:2], ["s3api", "get-bucket-versioning"])
                self.assertEqual([c["args"][3] for c in self.calls_for("aws", ["s3", "cp"])],
                                 ["s3://test-bucket/backup/" + name + "/latest" + suffix for name in ["app", "ALL"]])

    def test_fixed_names_use_distinct_directories_for_special_database_names(self):
        keys = []
        for name in ["app_2000-01-01T00:00:00", "with space", "a/b", "a%2Fb", ".", ".."]:
            with self.subTest(name=name):
                directory = quote(name, safe="") if name not in [".", ".."] else name.replace(".", "%2E")
                key = "backup/" + directory + "/latest.dump"
                self.assert_success(self.run_script(POSTGRES_DATABASE=name, BACKUP_FILENAME_MODE="fixed"))
                self.assertEqual(self.calls_for("aws", ["s3", "cp"])[0]["args"][3], "s3://test-bucket/" + key)
                self.assert_success(self.run_script("restore", POSTGRES_DATABASE=name, BACKUP_FILENAME_MODE="fixed"))
                self.assertEqual(self.calls_for("aws", ["s3", "cp"])[0]["args"][2], "s3://test-bucket/" + key)
                self.assertEqual(self.calls_for("pg_restore")[0]["database"], name)
                keys.append(key)
        self.assertEqual(len(keys), len(set(keys)))

    def test_other_databases_fixed_backup_is_not_deleted_or_restored_as_timestamped(self):
        name = "app_2000-01-01T00:00:00"
        self.assert_success(self.run_script(POSTGRES_DATABASE=name, BACKUP_FILENAME_MODE="fixed"))
        fixed_key = self.calls_for("aws", ["s3", "cp"])[0]["args"][3].removeprefix("s3://test-bucket/")
        fixed_object = {"Key": fixed_key, "LastModified": "2000-01-01T00:00:00Z"}
        old_key = "backup/app_2000-01-01T00:00:00.dump"
        config = {"objects": [fixed_object, {"Key": old_key, "LastModified": "2000-01-01T00:00:00Z"}]}
        for mode in ["timestamp", "fixed"]:
            with self.subTest(mode=mode):
                self.assert_success(self.run_script(POSTGRES_DATABASE="app", BACKUP_FILENAME_MODE=mode,
                                    BACKUP_KEEP_DAYS="7", config=config))
                self.assertEqual([c["args"][2] for c in self.calls_for("aws", ["s3", "rm"])],
                                 ["s3://test-bucket/" + old_key])
        result = self.run_script("restore", POSTGRES_DATABASE="app", config={"objects": [fixed_object]})
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.calls_for("pg_restore"))

    def test_fixed_requires_enabled_versioning(self):
        for config in [{"versioning": {}}, {"versioning": {"Status": "Suspended"}},
                       {"versioning_failure": True}]:
            with self.subTest(config=config):
                self.assertNotEqual(self.run_script(POSTGRES_DATABASE="app",
                                    BACKUP_FILENAME_MODE="fixed", config=config).returncode, 0)
                self.assertFalse(self.calls_for("pg_dump"))
                self.assertFalse(self.calls_for("aws", ["s3", "cp"]))

    def test_invalid_retention_setting_stops_before_dump_or_s3(self):
        for mode in ["timestamp", "fixed"]:
            for value in ["0", "-1", "abc", "08", "1.5", "999999999999"]:
                with self.subTest(mode=mode, value=value):
                    result = self.run_script(POSTGRES_DATABASE="app", BACKUP_FILENAME_MODE=mode,
                                             BACKUP_KEEP_DAYS=value)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("BACKUP_KEEP_DAYS must be", result.stderr)
                    self.assertFalse(self.calls)

    def test_invalid_filename_mode_never_uploads(self):
        self.assertNotEqual(self.run_script(POSTGRES_DATABASE="app",
                            BACKUP_FILENAME_MODE="wrong").returncode, 0)
        self.assertFalse(self.calls)

    def test_failed_database_does_not_prevent_later_backups(self):
        for failure in ["dump_failure", "gpg_failure", "upload_failure"]:
            with self.subTest(failure=failure):
                result = self.run_script(POSTGRES_DATABASES="app,billing", PASSPHRASE="test",
                                         config={failure: "app"})
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("1 succeeded, 1 failed", result.stdout)
                self.assertEqual([c["database"] for c in self.calls_for("pg_dump")], ["app", "billing"])
                uploads = self.calls_for("aws", ["s3", "cp"])
                self.assertIn("billing_", uploads[-1]["args"][3])
                self.assertEqual(len(uploads), 2 if failure == "upload_failure" else 1)
                self.assertFalse(self.calls_for("aws", ["s3api", "list-objects-v2"]))

    def test_unset_retention_never_lists_or_deletes_existing_s3_backups(self):
        config = {"list_failure": True, "objects": [
            {"Key": "backup/app_2000-01-01T00:00:00.dump", "LastModified": "2000-01-01T00:00:00Z"}]}
        for mode in ["timestamp", "fixed"]:
            for legacy_setting in [{}, {"BACKUP_KEEP_DAYS": ""}]:
                with self.subTest(mode=mode, legacy_setting=legacy_setting):
                    self.assert_success(self.run_script(POSTGRES_DATABASES="app,billing",
                                        BACKUP_FILENAME_MODE=mode, config=config, **legacy_setting))
                    actions = [call["args"][:2] for call in self.calls_for("aws")]
                    expected = ([["s3api", "get-bucket-versioning"]] if mode == "fixed" else [])
                    expected += [["s3", "cp"], ["s3", "cp"]]
                    self.assertEqual(actions, expected)

    def test_retention_keeps_unexpired_and_fixed_backups_in_both_modes(self):
        # The fake clock is 2026-09-12 12:00:00 UTC: a seven-day cutoff is
        # 2026-09-05 12:00:00 UTC, including the time of day.
        expired = [
            {"Key": "backup/app_2026-09-05T11:59:59.dump", "LastModified": "2026-09-05T11:59:59+00:00"},
            {"Key": "backup/app_2026-09-05T11:59:58.dump.gpg", "LastModified": "2026-09-05T11:59:59.999000+00:00"},
        ]
        retained = [
            {"Key": "backup/app_2026-09-05T12:00:00.dump", "LastModified": "2026-09-05T12:00:00+00:00"},
            {"Key": "backup/app_2026-09-05T12:00:01.dump", "LastModified": "2026-09-05T12:00:00.001000+00:00"},
            {"Key": "backup/app_2026-09-12T11:00:00.dump.gpg", "LastModified": "2026-09-12T11:00:00Z"},
        ]
        retained += [{"Key": key, "LastModified": "2000-01-01T00:00:00Z"} for key in [
            "backup/app/latest.dump", "backup/app/latest.dump.gpg", "backup/app_other_2000-01-01T00:00:00.dump",
            "backup/app_2000-01-01T00%3A00%3A00/latest.dump",
            "backup/other_2000-01-01T00:00:00.dump", "backup/app_notes.dump",
            "backup/app_2000-01-01T00:00:00.dump.extra", "backup/app_2000-01-01T00:00:00.dump\n",
        ]]
        for mode in ["timestamp", "fixed"]:
            with self.subTest(mode=mode):
                self.assert_success(self.run_script(POSTGRES_DATABASE="app", BACKUP_FILENAME_MODE=mode,
                                    BACKUP_KEEP_DAYS="7", config={"objects": expired + retained}))
                self.assertEqual([c["args"][2] for c in self.calls_for("aws", ["s3", "rm"])],
                                 ["s3://test-bucket/" + item["Key"] for item in expired])

    def test_retention_only_runs_after_successful_upload_in_both_modes(self):
        for mode in ["timestamp", "fixed"]:
            for failure in ["dump_failure", "gpg_failure", "upload_failure"]:
                with self.subTest(mode=mode, failure=failure):
                    result = self.run_script(POSTGRES_DATABASES="app,billing", BACKUP_FILENAME_MODE=mode,
                                             PASSPHRASE="test", BACKUP_KEEP_DAYS="7", config={failure: "app"})
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("1 succeeded, 1 failed", result.stdout)
                    lists = self.calls_for("aws", ["s3api", "list-objects-v2"])
                    self.assertEqual(len(lists), 1)
                    self.assertIn("backup/billing_", lists[0]["args"])

    def test_retention_failure_reports_failure_and_continues(self):
        for mode in ["timestamp", "fixed"]:
            for config in [{"list_failure": True}, {"delete_failure": True, "objects": [
                    {"Key": "backup/app_2000-01-01T00:00:00.dump", "LastModified": "2000-01-01T00:00:00Z"}]}]:
                with self.subTest(mode=mode, config=config):
                    result = self.run_script(POSTGRES_DATABASES="app,billing", BACKUP_FILENAME_MODE=mode,
                                             BACKUP_KEEP_DAYS="7", config=config)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("2 succeeded, 0 failed", result.stdout)
                    self.assertEqual(len(self.calls_for("pg_dump")), 2)

    def test_backup_lock_covers_dump_upload_and_retention(self):
        command = [os.environ.get("TEST_SHELL", "sh"), str(ROOT / "src" / "backup.sh")]
        for stage, (tool, action) in enumerate([
                ("pg_dump", None), ("aws", ["s3", "cp"]), ("aws", ["s3", "rm"])]):
            with self.subTest(stage=stage):
                entered = self.root / f"entered-{stage}"
                release = self.root / f"release-{stage}"
                config = {"pause_command": tool, "pause_action": action,
                          "pause_entered": str(entered), "pause_release": str(release),
                          "objects": [{"Key": "backup/app_2000-01-01T00:00:00.dump",
                                       "LastModified": "2000-01-01T00:00:00Z"}]}
                env = self.script_env(config, POSTGRES_DATABASE="app", BACKUP_FILENAME_MODE="fixed",
                                      BACKUP_KEEP_DAYS="7")
                first = subprocess.Popen(command, env=env, cwd=self.root, text=True,
                                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
                try:
                    deadline = time.monotonic() + 5
                    while not entered.exists() and first.poll() is None and time.monotonic() < deadline:
                        time.sleep(0.01)
                    self.assertTrue(entered.exists(), "first backup did not reach the paused command")
                    self.assertIsNone(first.poll())

                    # A manual run from a different directory/TMPDIR must use the same lock.
                    other_dir = self.root / f"other-{stage}"
                    other_dir.mkdir()
                    second_log = self.root / f"second-{stage}.jsonl"
                    second_log.write_text("")
                    second_env = self.script_env(POSTGRES_DATABASE="app", BACKUP_FILENAME_MODE="fixed",
                                                 TMPDIR=str(other_dir), FAKE_LOG=str(second_log))
                    second = subprocess.run(command, env=second_env, cwd=other_dir, text=True,
                                            capture_output=True, timeout=5)
                    self.assertNotEqual(second.returncode, 0)
                    self.assertIn("backup lock", second.stderr)
                    self.assertEqual(second_log.read_text(), "", "overlapping backup accessed DB or S3")
                    self.assertEqual(list(other_dir.iterdir()), [])

                    release.touch()
                    stdout, stderr = first.communicate(timeout=5)
                    self.assertEqual(first.returncode, 0, stdout + stderr)
                    self.assertEqual(list(self.runtime.iterdir()), [])
                finally:
                    release.touch()
                    if first.poll() is None:
                        os.killpg(first.pid, signal.SIGKILL)
                    first.communicate(timeout=5)
                # The persistent lock file must not prevent the next scheduled run.
                self.assert_success(self.run_script(POSTGRES_DATABASE="app", BACKUP_FILENAME_MODE="fixed"))

    def test_backup_lock_is_released_after_failure(self):
        result = self.run_script(POSTGRES_DATABASE="app", BACKUP_FILENAME_MODE="fixed",
                                 config={"dump_failure": "app"})
        self.assertNotEqual(result.returncode, 0)
        self.assert_success(self.run_script(POSTGRES_DATABASE="app", BACKUP_FILENAME_MODE="fixed"))

    def test_endpoint_prefix_and_dump_options(self):
        self.assert_success(self.run_script(POSTGRES_DATABASE="app", S3_PREFIX="",
                            S3_ENDPOINT="https://storage.example.test", PGDUMP_EXTRA_OPTS="--no-owner --no-acl"))
        call = self.calls_for("aws", ["s3", "cp"])[0]
        self.assertEqual(call["original_args"][:2], ["--endpoint-url", "https://storage.example.test"])
        self.assertTrue(call["args"][3].startswith("s3://test-bucket//app_"))
        self.assertEqual(self.calls_for("pg_dump")[0]["args"][-2:], ["--no-owner", "--no-acl"])

    def test_timestamp_prefix_compatibility_for_backup_and_restore(self):
        timestamp = "2026-09-12T12:00:00"
        for prefix, stored_prefix in [("backup", "backup/"), ("backup/", "backup//"),
                                      ("backup//", "backup///"), ("", "/"), ("/", "//")]:
            for passphrase, suffix in [("", ".dump"), ("test", ".dump.gpg")]:
                with self.subTest(prefix=prefix, suffix=suffix):
                    key = stored_prefix + "app_" + timestamp + suffix
                    settings = {"POSTGRES_DATABASE": "app", "S3_PREFIX": prefix, "PASSPHRASE": passphrase}
                    self.assert_success(self.run_script(**settings))
                    self.assertEqual(self.calls_for("aws", ["s3", "cp"])[0]["args"][3],
                                     "s3://test-bucket/" + key)
                    self.assert_success(self.run_script("restore", config={"objects": [{"Key": key}]}, **settings))
                    self.assertIn(stored_prefix + "app_",
                                  self.calls_for("aws", ["s3api", "list-objects-v2"])[0]["args"])
                    self.assertEqual(self.calls_for("aws", ["s3", "cp"])[0]["args"][2],
                                     "s3://test-bucket/" + key)
                    self.assert_success(self.run_script("restore", args=(timestamp,), **settings))
                    self.assertEqual(self.calls_for("aws", ["s3", "cp"])[0]["args"][2],
                                     "s3://test-bucket/" + key)

    def test_retention_uses_original_timestamp_prefix_in_both_modes(self):
        for prefix, stored_prefix, fixed_prefix in [("backup/", "backup//", "backup/"),
                                                   ("backup//", "backup///", "backup//"),
                                                   ("", "/", ""), ("/", "//", "")]:
            for mode in ["timestamp", "fixed"]:
                with self.subTest(prefix=prefix, mode=mode):
                    expired_key = stored_prefix + "app_2000-01-01T00:00:00.dump"
                    objects = [
                        {"Key": expired_key, "LastModified": "2000-01-01T00:00:00Z"},
                        {"Key": stored_prefix + "app_2026-09-12T00:00:00.dump", "LastModified": "2026-09-12T00:00:00Z"},
                        {"Key": fixed_prefix + "app_2000-01-01T00:00:00.dump", "LastModified": "2000-01-01T00:00:00Z"},
                        {"Key": fixed_prefix + "app/latest.dump", "LastModified": "2000-01-01T00:00:00Z"},
                    ]
                    self.assert_success(self.run_script(POSTGRES_DATABASE="app", S3_PREFIX=prefix,
                                        BACKUP_FILENAME_MODE=mode, BACKUP_KEEP_DAYS="7", config={"objects": objects}))
                    self.assertIn(stored_prefix + "app_",
                                  self.calls_for("aws", ["s3api", "list-objects-v2"])[0]["args"])
                    self.assertEqual([c["args"][2] for c in self.calls_for("aws", ["s3", "rm"])],
                                     ["s3://test-bucket/" + expired_key])

    def test_fixed_prefix_layout_for_backup_and_restore(self):
        for prefix, stored_prefix in [("backup/", "backup/"), ("backup//", "backup//"), ("", ""), ("/", "")]:
            with self.subTest(prefix=prefix):
                key = stored_prefix + "app/latest.dump"
                settings = {"POSTGRES_DATABASE": "app", "S3_PREFIX": prefix, "BACKUP_FILENAME_MODE": "fixed"}
                self.assert_success(self.run_script(**settings))
                self.assertEqual(self.calls_for("aws", ["s3", "cp"])[0]["args"][3], "s3://test-bucket/" + key)
                self.assert_success(self.run_script("restore", **settings))
                self.assertEqual(self.calls_for("aws", ["s3", "cp"])[0]["args"][2], "s3://test-bucket/" + key)
                self.assert_success(self.run_script("restore", args=("--version-id", "version1"), **settings))
                self.assertIn(key, self.calls_for("aws", ["s3api", "get-object"])[0]["args"])

    def test_restore_fixed_latest_and_version(self):
        for args in [(), ("--version-id", "opaque+/version==")]:
            with self.subTest(args=args):
                self.assert_success(self.run_script("restore", args=args, POSTGRES_DATABASE="ALL",
                                    POSTGRES_BACKUP_ALL="true", BACKUP_FILENAME_MODE="fixed"))
                restore = self.calls_for("pg_restore")[0]
                self.assertEqual(restore["database"], "ALL")
                self.assertIn("--exit-on-error", restore["args"])
                self.assertFalse(self.calls_for("psql"))
                self.assertFalse(self.calls_for("aws", ["s3api", "get-bucket-versioning"]))
                if args:
                    call = self.calls_for("aws", ["s3api", "get-object"])[0]
                    self.assertIn(args[1], call["args"])
                    self.assertIn("backup/ALL/latest.dump", call["args"])
                else:
                    self.assertEqual(self.calls_for("aws", ["s3", "cp"])[0]["args"][2],
                                     "s3://test-bucket/backup/ALL/latest.dump")

    def test_restore_latest_matches_exact_database_and_format(self):
        keys = ["backup/app_2020-01-01T00:00:00.dump", "backup/app_2021-01-01T00:00:00.dump",
                "backup/app_extra_2030-01-01T00:00:00.dump", "backup/app_2025-01-01T00:00:00.dump.gpg"]
        self.assert_success(self.run_script("restore", POSTGRES_DATABASE="app",
                            config={"objects": [{"Key": key} for key in keys]}))
        self.assertEqual(self.calls_for("aws", ["s3", "cp"])[0]["args"][2], "s3://test-bucket/" + keys[1])

    def test_restore_timestamp_and_encrypted_version(self):
        self.assert_success(self.run_script("restore", args=("2020-01-01T00:00:00",), POSTGRES_DATABASE="app"))
        self.assertEqual(self.calls_for("aws", ["s3", "cp"])[0]["args"][2],
                         "s3://test-bucket/backup/app_2020-01-01T00:00:00.dump")
        self.assert_success(self.run_script("restore", args=("--version-id", "version1"),
                            POSTGRES_DATABASE="app", BACKUP_FILENAME_MODE="fixed", PASSPHRASE="test"))
        self.assertIn("backup/app/latest.dump.gpg", self.calls_for("aws", ["s3api", "get-object"])[0]["args"])
        self.assertEqual(len(self.calls_for("gpg")), 1)
        self.assertEqual(len(self.calls_for("pg_restore")), 1)

    def test_restore_stops_before_database_on_download_or_decryption_error(self):
        for config in [{"download_failure": True}, {"gpg_failure": "restored"}]:
            with self.subTest(config=config):
                result = self.run_script("restore", POSTGRES_DATABASE="app", BACKUP_FILENAME_MODE="fixed",
                                         PASSPHRASE="test", config=config)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(self.calls_for("pg_restore"))
                self.assertNotIn("Restore complete", result.stdout)

    def test_restore_errors_and_empty_lookup_are_not_success(self):
        result = self.run_script("restore", POSTGRES_DATABASE="app", BACKUP_FILENAME_MODE="fixed",
                                 config={"restore_failure": True})
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("Restore complete", result.stdout)
        for config in [{}, {"list_failure": True}]:
            self.assertNotEqual(self.run_script("restore", POSTGRES_DATABASE="app", config=config).returncode, 0)
            self.assertFalse(self.calls_for("pg_restore"))

    def test_restore_invalid_arguments(self):
        cases = [((), {}), (("--version-id", "id"), {"POSTGRES_DATABASE": "app"}),
                 (("bad-timestamp",), {"POSTGRES_DATABASE": "app"}),
                 (("2020-01-01T00:00:00\n",), {"POSTGRES_DATABASE": "app"}),
                 (("2020-01-01T00:00:00",), {"POSTGRES_DATABASE": "app", "BACKUP_FILENAME_MODE": "fixed"}),
                 (("--version-id", ""), {"POSTGRES_DATABASE": "app", "BACKUP_FILENAME_MODE": "fixed"}),
                 (("a", "b", "c"), {"POSTGRES_DATABASE": "app"})]
        for args, settings in cases:
            with self.subTest(args=args, settings=settings):
                self.assertNotEqual(self.run_script("restore", args=args, **settings).returncode, 0)
                self.assertFalse(self.calls)


if __name__ == "__main__":
    unittest.main()
