"""Release tests use only temporary data and mocked GitHub CLI calls."""
from __future__ import annotations

import importlib.util
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tarfile
import tempfile
import unittest
from unittest import mock

SPEC = importlib.util.spec_from_file_location("release_artifacts", Path(__file__).resolve().parents[1] / "scripts" / "release_artifacts.py")
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        silence = mock.patch("sys.stdout", new_callable=io.StringIO)
        silence.start()
        self.addCleanup(silence.stop)
        self.root = Path(self.temp.name)
        self.repo = self.root / "repo"
        self.transcripts = self.repo / "data" / "transcripts"
        self.group = self.transcripts / "lectures"
        self.group.mkdir(parents=True)
        self.db = self.repo / "data" / "pipeline.db"
        self.out = self.root / "release"
        self.dest = self.root / "destination"
        for name, value in (("a.txt", "new a"), ("b.srt", "new b"), ("a.json", "{}"), (".a.complete.json", "private marker")):
            (self.group / name).write_text(value, encoding="utf-8")
        for name, value in (("REPO_ROOT", self.repo), ("TRANSCRIPTS_DIR", self.transcripts), ("DB_PATH", self.db)):
            patch = mock.patch.object(release, name, value)
            patch.start()
            self.addCleanup(patch.stop)

    def package(self, **kwargs):
        return release.package_artifacts(self.out, ["lectures"], include_db=False, **kwargs)

    def rewrite_manifest(self, manifest):
        path = self.out / "MANIFEST.json"
        path.write_text(json.dumps(manifest), encoding="utf-8")
        sums = [f"{a['sha256']}  {a['name']}" for a in manifest["assets"]]
        sums.append(f"{release.sha256_file(path)}  MANIFEST.json")
        (self.out / "SHA256SUMS.txt").write_text("\n".join(sums) + "\n", encoding="utf-8")

    def assert_unchanged(self):
        self.assertEqual((self.dest / "data/transcripts/lectures/a.txt").read_text(), "old a")
        self.assertFalse((self.dest / "data/transcripts/lectures/b.srt").exists())

    def old_destination(self):
        target = self.dest / "data/transcripts/lectures/a.txt"
        target.parent.mkdir(parents=True)
        target.write_text("old a", encoding="utf-8")

    def test_round_trip_and_manifest_fields(self):
        manifest = self.package()
        self.assertEqual(manifest["schema_version"], 2)
        self.assertEqual(manifest["db_profile"], "public")
        self.assertEqual(manifest["sanitization_version"], 1)
        self.assertTrue(manifest["release_id"])
        release.restore_artifacts(self.out, self.dest)
        self.assertEqual((self.dest / "data/transcripts/lectures/a.txt").read_text(), "new a")
        self.assertFalse((self.dest / "data/transcripts/lectures/.a.complete.json").exists())
        self.assertFalse((self.dest / ".release-restore").exists())

    def test_missing_manifest_or_checksum_is_always_error(self):
        for name in ("MANIFEST.json", "SHA256SUMS.txt"):
            for skip in (False, True):
                with self.subTest(name=name, skip=skip):
                    self.package()
                    (self.out / name).unlink()
                    with self.assertRaises(FileNotFoundError):
                        release.restore_artifacts(self.out, self.dest, skip)
                    self.assertFalse(self.dest.exists())

    def test_missing_declared_part_never_writes_destination(self):
        manifest = self.package()
        (self.out / manifest["assets"][-1]["name"]).unlink()
        self.old_destination()
        for skip in (False, True):
            with self.assertRaises(FileNotFoundError):
                release.restore_artifacts(self.out, self.dest, skip)
        self.assert_unchanged()

    def test_digest_failure_validates_all_before_any_extraction(self):
        manifest = self.package()
        archive = self.out / manifest["assets"][-1]["name"]
        raw = archive.read_bytes()
        archive.write_bytes(bytes([raw[0] ^ 1]) + raw[1:])
        self.old_destination()
        with self.assertRaisesRegex(RuntimeError, "SHA-256"):
            release.restore_artifacts(self.out, self.dest)
        self.assert_unchanged()

    def test_skip_verify_skips_only_digests(self):
        manifest = self.package()
        manifest["assets"][0]["sha256"] = "0" * 64
        self.rewrite_manifest(manifest)
        with self.assertRaisesRegex(RuntimeError, "SHA-256"):
            release.validate_release(self.out)
        release.restore_artifacts(self.out, self.dest, skip_verify=True)
        manifest["assets"][0]["archive_bytes"] += 1
        self.rewrite_manifest(manifest)
        with self.assertRaisesRegex(RuntimeError, "Size mismatch"):
            release.validate_release(self.out, skip_verify=True)

    def test_crash_before_initial_journal_can_restart(self):
        self.package()
        work = self.dest / ".release-restore"
        (work / "stage").mkdir(parents=True)
        release.restore_artifacts(self.out, self.dest)
        self.assertFalse(work.exists())
        self.assertEqual((self.dest / "data/transcripts/lectures/a.txt").read_text(), "new a")

    def test_package_directory_swap_failure_restores_previous(self):
        old = self.package()
        original = Path.replace
        def replace(path, target):
            if path.name == "batch" and Path(target).resolve() == self.out.resolve():
                raise OSError("injected directory swap failure")
            return original(path, target)
        with mock.patch.object(Path, "replace", replace):
            with self.assertRaises(OSError):
                self.package()
        self.assertEqual(release.validate_release(self.out), old)

    def test_package_recovers_interrupted_directory_swap_before_build(self):
        old = self.package()
        self.out.replace(self.out.with_name("release.previous"))
        with mock.patch.object(release, "build_tar_gz", side_effect=OSError("injected build failure")):
            with self.assertRaises(OSError):
                self.package()
        self.assertEqual(release.validate_release(self.out), old)

    def test_public_export_replaces_existing_private_file(self):
        self.create_database()
        exported = self.root / "export.db"
        release.export_clean_db(exported, db_profile="private")
        release.export_clean_db(exported, db_profile="public")
        conn = sqlite3.connect(exported)
        self.addCleanup(conn.close)
        self.assertIsNone(conn.execute("SELECT asr_task_id FROM media_items").fetchone()[0])
        self.assertEqual(conn.execute("SELECT count(*) FROM sqlite_master WHERE name='private_extra'").fetchone()[0], 0)
        with self.assertRaises(ValueError):
            release.export_clean_db(self.db)

    def test_manifest_digest_checked_first(self):
        self.package()
        path = self.out / "MANIFEST.json"
        path.write_text(path.read_text() + " ", encoding="utf-8")
        with self.assertRaisesRegex(RuntimeError, "MANIFEST"):
            release.validate_release(self.out)
        release.validate_release(self.out, skip_verify=True)

    def test_extra_archive_is_ignored(self):
        self.package()
        (self.out / "old-part99.tar.gz").write_bytes(b"not a tar")
        release.restore_artifacts(self.out, self.dest)
        self.assertTrue((self.dest / "data/transcripts/lectures/a.txt").is_file())

    def test_legacy_manifest_assets_supported(self):
        manifest = self.package()
        for key in ("schema_version", "release_id", "db_profile", "sanitization_version"):
            manifest.pop(key)
        self.rewrite_manifest(manifest)
        release.restore_artifacts(self.out, self.dest)

    def test_duplicate_manifest_and_checksum_names_rejected(self):
        manifest = self.package()
        manifest["assets"].append(manifest["assets"][0])
        self.rewrite_manifest(manifest)
        with self.assertRaises(ValueError):
            release.validate_release(self.out, True)
        self.package()
        sums = self.out / "SHA256SUMS.txt"
        sums.write_text(sums.read_text() * 2, encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            release.validate_release(self.out, True)

    def test_asset_path_rejected_even_skip_verify(self):
        for name in ("../escape.tar.gz", "C:\\escape.tar.gz", "nested/part.tar.gz"):
            manifest = self.package()
            manifest["assets"][0]["name"] = name
            self.rewrite_manifest(manifest)
            with self.assertRaises(ValueError):
                release.restore_artifacts(self.out, self.dest, True)

    def malicious_archive(self, member_name, kind=None):
        manifest = self.package()
        archive = self.out / manifest["assets"][0]["name"]
        with tarfile.open(archive, "w:gz") as tf:
            info = tarfile.TarInfo(member_name)
            if kind:
                info.type = kind
                info.linkname = "data/transcripts/lectures/a.txt"
                tf.addfile(info)
            else:
                info.size = 4
                tf.addfile(info, io.BytesIO(b"evil"))
        manifest["assets"][0]["sha256"] = release.sha256_file(archive)
        manifest["assets"][0]["archive_bytes"] = archive.stat().st_size
        self.rewrite_manifest(manifest)

    def test_both_path_syntaxes_and_prefix_sibling_rejected(self):
        self.old_destination()
        for path in ("../destination-evil/file", "/absolute/file", "C:/evil", "..\\evil", "data/../../evil", ".release-restore/journal.json", "./.release-restore/journal.json", ".RELEASE-RESTORE/journal.json", "data/CON", "data/trailing."):
            with self.subTest(path=path):
                self.malicious_archive(path)
                with self.assertRaises(RuntimeError):
                    release.restore_artifacts(self.out, self.dest, True)
                self.assert_unchanged()

    def test_tar_links_rejected_and_data_filter_used(self):
        for kind in (tarfile.SYMTYPE, tarfile.LNKTYPE, tarfile.FIFOTYPE):
            self.malicious_archive("data/link", kind)
            with self.assertRaises(RuntimeError):
                release.restore_artifacts(self.out, self.dest, True)
        self.package()
        original = tarfile.TarFile.extractall
        calls = []
        def extract(tf, *args, **kwargs):
            calls.append(kwargs.get("filter"))
            return original(tf, *args, **kwargs)
        with mock.patch.object(tarfile.TarFile, "extractall", extract):
            release.restore_artifacts(self.out, self.dest)
        self.assertTrue(calls)
        self.assertEqual(set(calls), {"data"})

    def test_missing_data_filter_errors_before_destination(self):
        self.package()
        with mock.patch.object(tarfile, "data_filter", create=True):
            # A missing capability is simulated by deleting the attribute briefly.
            del tarfile.data_filter
            with self.assertRaisesRegex(RuntimeError, "data filter"):
                release.restore_artifacts(self.out, self.dest)
        self.assertFalse(self.dest.exists())

    def test_staging_failure_does_not_replace_target(self):
        self.package()
        self.old_destination()
        original = tarfile.TarFile.extractall
        calls = 0
        def extract(tf, *args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise OSError("injected second archive failure")
            return original(tf, *args, **kwargs)
        with mock.patch.object(tarfile.TarFile, "extractall", extract):
            with self.assertRaises(OSError):
                release.restore_artifacts(self.out, self.dest)
        self.assert_unchanged()

    def test_commit_failure_rolls_back_replaced_and_new_files(self):
        self.package()
        self.old_destination()
        original = os.replace
        count = 0
        def replace(src, dst):
            nonlocal count
            if "stage" in Path(src).parts:
                count += 1
                if count == 3:
                    raise OSError("injected commit failure")
            return original(src, dst)
        with mock.patch.object(release.os, "replace", replace):
            with self.assertRaises(OSError):
                release.restore_artifacts(self.out, self.dest)
        self.assert_unchanged()
        self.assertFalse((self.dest / "data/transcripts/lectures/a.json").exists())
        self.assertFalse((self.dest / ".release-restore").exists())

    def test_crash_log_recovery_is_idempotent(self):
        self.old_destination()
        work = self.dest / ".release-restore"
        backup = work / "backup/data/transcripts/lectures/a.txt"
        backup.parent.mkdir(parents=True)
        backup.write_text("old a", encoding="utf-8")
        (self.dest / "data/transcripts/lectures/a.txt").write_text("interrupted")
        (self.dest / "data/transcripts/lectures/b.srt").write_text("partial new")
        release._write_journal(work / "journal.json", {"state": "committing", "entries": [
            {"path": "data/transcripts/lectures/a.txt", "existed": True},
            {"path": "data/transcripts/lectures/b.srt", "existed": False},
        ]})
        release._recover_restore(self.dest)
        release._recover_restore(self.dest)
        self.assert_unchanged()

    def test_recovery_missing_backup_retains_journal(self):
        self.old_destination()
        work = self.dest / ".release-restore"
        work.mkdir()
        release._write_journal(work / "journal.json", {"state": "committing", "entries": [
            {"path": "data/transcripts/lectures/a.txt", "existed": True},
        ]})
        with self.assertRaisesRegex(RuntimeError, "backup missing"):
            release._recover_restore(self.dest)
        self.assertTrue((work / "journal.json").is_file())
        self.assert_unchanged()

    def test_recovery_checks_db_sidecars_before_rollback(self):
        self.old_destination()
        work = self.dest / ".release-restore"
        backup = work / "backup/data/pipeline.db"
        backup.parent.mkdir(parents=True)
        backup.write_bytes(b"old")
        (self.dest / "data/pipeline.db").write_bytes(b"new")
        sidecar = self.dest / "data/pipeline.db-wal"
        sidecar.write_bytes(b"wal")
        release._write_journal(work / "journal.json", {"state": "committing", "entries": [
            {"path": "data/pipeline.db", "existed": True},
        ]})
        with self.assertRaisesRegex(RuntimeError, "WAL"):
            release._recover_restore(self.dest)
        self.assertEqual((self.dest / "data/pipeline.db").read_bytes(), b"new")
        sidecar.unlink()
        release._recover_restore(self.dest)
        self.assertEqual((self.dest / "data/pipeline.db").read_bytes(), b"old")

    def test_active_wal_sidecar_prevents_commit(self):
        self.package()
        # Add a declared DB archive to a synthetic release.
        manifest = json.loads((self.out / "MANIFEST.json").read_text())
        sample = self.root / "sample.db"
        sample.write_bytes(b"db")
        archive = self.out / "db.tar.gz"
        release.build_tar_gz(archive, [(sample, "data/pipeline.db")])
        manifest["assets"].append({"name": archive.name, "sha256": release.sha256_file(archive), "archive_bytes": archive.stat().st_size})
        self.rewrite_manifest(manifest)
        self.old_destination()
        (self.dest / "data/pipeline.db-wal").write_bytes(b"active")
        with self.assertRaisesRegex(RuntimeError, "WAL"):
            release.restore_artifacts(self.out, self.dest)
        self.assert_unchanged()

    def test_failed_package_preserves_old_batch(self):
        old = self.package()
        with mock.patch.object(release, "build_tar_gz", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                self.package()
        self.assertEqual(release.validate_release(self.out), old)
        new = self.package()
        self.assertNotEqual(new["release_id"], old["release_id"])
        self.assertEqual(release.validate_release(self.out.with_name("release.previous")), old)

    def create_database(self):
        # Representative legacy schema plus future columns, private table and trigger.
        conn = sqlite3.connect(self.db)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript("""
            CREATE TABLE media_items (
                id INTEGER PRIMARY KEY, source_group TEXT NOT NULL, video_id TEXT NOT NULL,
                title TEXT NOT NULL, video_path TEXT NOT NULL UNIQUE, video_rel_path TEXT NOT NULL,
                video_size_bytes INTEGER, duration_sec REAL, audio_status TEXT, asr_status TEXT,
                audio_path TEXT, txt_path TEXT, srt_path TEXT, json_path TEXT,
                asr_server_url TEXT, asr_task_id TEXT, audio_error TEXT, asr_error TEXT, asr_message TEXT,
                source_fingerprint TEXT, audio_fingerprint TEXT, asr_fingerprint TEXT, asr_phase TEXT,
                future_secret TEXT, future_required TEXT NOT NULL DEFAULT 'default');
            CREATE TABLE pipeline_runs (id INTEGER PRIMARY KEY, run_type TEXT, source_group TEXT,
                total_count INTEGER, done_count INTEGER, status TEXT, config_json TEXT, params_json TEXT,
                future_secret TEXT);
            CREATE TABLE private_extra (secret TEXT);
            INSERT INTO private_extra VALUES ('private extra secret');
            CREATE TRIGGER private_trigger AFTER UPDATE ON private_extra BEGIN SELECT 1; END;
        """)
        columns = [r[1] for r in conn.execute("PRAGMA table_info(media_items)")]
        values = {name: "private secret" for name in columns}
        values.update(id=1, source_group="lectures", video_id="media01", title="Lecture", video_path="C:\\private\\lectures\\clip.mp4",
                      video_rel_path="lectures/clip.mp4", video_size_bytes=100, duration_sec=2.5, audio_status="done", asr_status="done",
                      audio_path="C:\\private\\data\\audio\\lectures\\clip.mp3", txt_path="/private/data/transcripts/lectures/clip.txt",
                      srt_path="/private/data/transcripts/lectures/clip.srt", json_path="/private/data/transcripts/lectures/clip.json")
        conn.execute(f"INSERT INTO media_items VALUES ({','.join('?' for _ in columns)})", [values[c] for c in columns])
        conn.execute("INSERT INTO pipeline_runs VALUES (1, 'transcribe', 'lectures', 1, 1, 'done', 'private config', 'private params', 'private secret')")
        conn.execute("PRAGMA user_version=1")
        conn.commit()
        self.addCleanup(conn.close)
        return conn

    def test_public_db_explicit_allowlist_schema_and_metadata(self):
        source = self.create_database()
        exported = self.root / "public.db"
        release.export_clean_db(exported)
        conn = sqlite3.connect(exported)
        self.addCleanup(conn.close)
        conn.row_factory = sqlite3.Row
        self.assertEqual([tuple(r) for r in conn.execute("PRAGMA table_info(media_items)")], source.execute("PRAGMA table_info(media_items)").fetchall())
        row = conn.execute("SELECT * FROM media_items").fetchone()
        for name in row.keys():
            if name in release.PUBLIC_MEDIA_FIELDS:
                self.assertEqual(row[name], source.execute(f'SELECT "{name}" FROM media_items').fetchone()[0])
            elif name in release.PUBLIC_PATH_FIELDS:
                self.assertFalse(str(row[name]).startswith(("/", "C:", "\\")))
                self.assertNotIn("private", row[name])
            elif name == "future_required":
                self.assertEqual(row[name], "")
            else:
                self.assertIsNone(row[name], name)
        run = conn.execute("SELECT * FROM pipeline_runs").fetchone()
        for name in ("config_json", "params_json", "future_secret"):
            self.assertIsNone(run[name])
        self.assertEqual(dict(conn.execute("SELECT key,value FROM snapshot_metadata")), {"profile": "public", "sanitization_version": "1"})
        self.assertEqual(conn.execute("PRAGMA journal_mode").fetchone()[0], "delete")
        self.assertEqual({r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}, {"media_items", "pipeline_runs", "snapshot_metadata"})
        self.assertEqual(conn.execute("SELECT count(*) FROM sqlite_master WHERE type='trigger'").fetchone()[0], 0)
        self.assertNotIn(b"private secret", exported.read_bytes())

    def test_private_online_backup_preserves_wal_and_schema_version(self):
        self.create_database()
        exported = self.root / "private.db"
        release.export_clean_db(exported, sanitize=False)
        conn = sqlite3.connect(exported)
        self.addCleanup(conn.close)
        with conn:
            self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT asr_task_id FROM media_items").fetchone()[0], "private secret")
            self.assertEqual(conn.execute("SELECT secret FROM private_extra").fetchone()[0], "private extra secret")
            self.assertEqual(conn.execute("SELECT value FROM snapshot_metadata WHERE key='profile'").fetchone()[0], "private")

    def test_profile_alias_conflict(self):
        self.assertEqual(release._db_profile(False, None), "private")
        self.assertEqual(release._db_profile(None, None), "public")
        with self.assertRaises(ValueError):
            release.export_clean_db(self.root / "unused.db", False, db_profile="public")

    def publish_mock(self, existing=False, draft=False, fail_upload=False):
        manifest = self.package()
        calls = []
        uploaded = []
        def run(cmd, **kwargs):
            calls.append(cmd)
            if cmd[1:3] == ["release", "view"]:
                if not uploaded:
                    return subprocess.CompletedProcess(cmd, 0 if existing else 1, '{"assets": []}')
                assets = [{"name": p.name, "size": p.stat().st_size, "digest": "sha256:" + release.sha256_file(p)} for p in uploaded]
                return subprocess.CompletedProcess(cmd, 0, json.dumps({"assets": assets}))
            if cmd[1:3] == ["release", "upload"]:
                if fail_upload:
                    return subprocess.CompletedProcess(cmd, 1)
                uploaded.append(Path(cmd[4]))
            return subprocess.CompletedProcess(cmd, 0)
        with mock.patch.object(release.shutil, "which", return_value="gh"), mock.patch.object(release.subprocess, "run", side_effect=run):
            if fail_upload:
                with self.assertRaises(RuntimeError):
                    release.publish_release(self.out, "vtest", None, manifest, draft=draft, max_retries=1)
            else:
                release.publish_release(self.out, "vtest", None, manifest, draft=draft)
        return calls, uploaded

    def test_verified_resume_skips_uploads_and_finishes_existing_draft(self):
        manifest = self.package()
        paths = [self.out / a["name"] for a in manifest["assets"]] + [self.out / "MANIFEST.json", self.out / "SHA256SUMS.txt"]
        payload = json.dumps({"isDraft": True, "assets": [{"name": p.name, "size": p.stat().st_size,
                              "digest": "sha256:" + release.sha256_file(p)} for p in paths]})
        calls = []
        def run(cmd, **kwargs):
            calls.append(cmd)
            return subprocess.CompletedProcess(cmd, 0, payload)
        with mock.patch.object(release.shutil, "which", return_value="gh"), mock.patch.object(release.subprocess, "run", side_effect=run):
            release.publish_release(self.out, "vtest", None, manifest)
        self.assertFalse(any(c[2] == "upload" for c in calls))
        self.assertEqual(calls[-1], ["gh", "release", "edit", "vtest", "--draft=false"])

    def test_remote_collection_validation_blocks_publish(self):
        manifest = self.package()
        calls = []
        def run(cmd, **kwargs):
            calls.append(cmd)
            if cmd[2] == "view":
                return subprocess.CompletedProcess(cmd, 1 if len(calls) == 1 else 0, '{"assets": []}')
            return subprocess.CompletedProcess(cmd, 0)
        with mock.patch.object(release.shutil, "which", return_value="gh"), mock.patch.object(release.subprocess, "run", side_effect=run):
            with self.assertRaisesRegex(RuntimeError, "Remote asset"):
                release.publish_release(self.out, "vtest", None, manifest)
        self.assertFalse(any("--draft=false" in c for c in calls))

    def test_split_parts_extract_standalone_and_all_are_required(self):
        (self.group / "a.txt").write_bytes(b"a" * (700 * 1024))
        (self.group / "b.srt").write_bytes(b"b" * (700 * 1024))
        manifest = self.package(include_json=False, max_part_raw_mb=1)
        self.assertEqual([a["part"] for a in manifest["assets"]], ["1/2", "2/2"])
        for asset in manifest["assets"]:
            with tarfile.open(self.out / asset["name"], "r:gz") as tf:
                self.assertEqual(len(tf.getmembers()), 1)
        release.restore_artifacts(self.out, self.dest)
        self.assertEqual((self.dest / "data/transcripts/lectures/b.srt").stat().st_size, 700 * 1024)

    def test_new_release_draft_then_upload_verify_publish(self):
        calls, uploaded = self.publish_mock()
        create = next(c for c in calls if c[1:3] == ["release", "create"])
        self.assertIn("--draft", create)
        self.assertEqual([p.name for p in uploaded][-2:], ["SHA256SUMS.txt", "MANIFEST.json"])
        self.assertEqual(calls[-1], ["gh", "release", "edit", "vtest", "--draft=false"])

    def test_failed_upload_leaves_new_release_draft(self):
        calls, _ = self.publish_mock(fail_upload=True)
        self.assertFalse(any("--draft=false" in c for c in calls))

    def test_existing_release_manifest_is_last_and_old_assets_not_deleted(self):
        calls, uploaded = self.publish_mock(existing=True)
        self.assertEqual(uploaded[-1].name, "MANIFEST.json")
        self.assertFalse(any(c[2] in ("delete-asset", "delete") for c in calls))
        self.assertFalse(any("--draft=false" in c for c in calls))

    def test_explicit_draft_stays_draft(self):
        calls, _ = self.publish_mock(draft=True)
        self.assertFalse(any("--draft=false" in c for c in calls))


if __name__ == "__main__":
    unittest.main()
