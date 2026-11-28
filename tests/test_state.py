from pathlib import Path
import sqlite3
import tempfile
import unittest

import asr_pipeline as p


class StateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.path = self.root / 'pipeline.db'
        self.db = p.StateDB(self.path)
        self.video = self.root / 'clip.mp4'
        self.video.write_bytes(b'video')
        self.audio = self.root / 'clip.mp3'
        self.audio.write_bytes(b'audio')
        self.scan()
        self.item_id = int(self.db.get_all_group_items('group')[0]['id'])

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    def scan(self):
        st = self.video.stat()
        self.db.upsert_videos('group', [('clip', 'clip', str(self.video), 'clip.mp4', st.st_size, st.st_mtime)])

    def mark_done(self):
        self.db.mark_audio_done(self.item_id, preset='asr-mp3', audio_path=self.audio,
            audio_format='mp3', audio_size_bytes=self.audio.stat().st_size, duration_sec=200)
        self.db.mark_asr_done(self.item_id, txt_path=self.root/'clip.txt', srt_path=self.root/'clip.srt')

    def test_changed_video_invalidates_both_stages(self):
        self.mark_done()
        self.video.write_bytes(b'changed video')
        self.scan()
        row = self.db.get_all_group_items('group')[0]
        self.assertEqual(row['audio_status'], 'pending')
        self.assertEqual(row['asr_status'], 'pending')
        self.assertIsNone(row['asr_task_id'])
        self.assertIsNone(row['txt_path'])

    def test_reextraction_invalidates_transcript(self):
        self.mark_done()
        self.db.mark_audio_done(self.item_id, preset='wav', audio_path=self.audio,
            audio_format='wav', audio_size_bytes=5, duration_sec=200)
        self.assertEqual(self.db.get_all_group_items('group')[0]['asr_status'], 'pending')

    def test_new_attempt_clears_old_task_id(self):
        self.db.mark_asr_started(self.item_id, mode='async', server_url='http://fixture', task_id='old')
        self.db.mark_asr_failed(self.item_id, 'failed')
        self.db.mark_asr_started(self.item_id, mode='async', server_url='http://fixture', status='queued')
        row = self.db.get_all_group_items('group')[0]
        self.assertIsNone(row['asr_task_id'])
        self.assertEqual(row['asr_phase'], 'submitting')

    def test_reset_audio_invalidates_completed_transcription_immediately(self):
        self.mark_done()
        self.assertEqual(self.db.reset_items('audio', ['done']), 1)
        row = self.db.get_all_group_items('group')[0]
        self.assertEqual(row['audio_status'], 'pending')
        self.assertEqual(row['asr_status'], 'pending')
        self.assertIsNone(row['audio_fingerprint'])
        self.assertIsNone(row['asr_fingerprint'])
        self.assertIsNone(row['txt_path'])

    def test_schema_is_versioned_and_reopening_is_idempotent(self):
        self.assertEqual(self.db.conn.execute('PRAGMA user_version').fetchone()[0], 1)
        self.db.close()
        self.db = p.StateDB(self.path)
        self.assertEqual(len(self.db.get_all_group_items('group')), 1)

    def test_concurrent_legacy_migration_rechecks_schema_under_lock(self):
        import concurrent.futures
        import re
        import threading
        from unittest.mock import patch
        old = self.root / 'concurrent.db'
        connect = sqlite3.connect
        con = connect(old)
        legacy = p.SCHEMA_SQL
        for name in ('source_fingerprint', 'audio_fingerprint', 'asr_fingerprint', 'asr_phase'):
            legacy = re.sub(r'^.*\b' + name + r'\b.*\n', '', legacy, flags=re.M)
        con.executescript(legacy)
        self.assertNotIn('asr_phase', {r[1] for r in con.execute('PRAGMA table_info(media_items)')})
        con.close()
        barrier = threading.Barrier(2)
        class SynchronizedBackup(sqlite3.Connection):
            def backup(self, target, **kwargs):
                super().backup(target, **kwargs)
                barrier.wait(timeout=10)
        def wrapped(database, *args, **kwargs):
            if str(database) == str(old.resolve()):
                kwargs['factory'] = SynchronizedBackup
            return connect(database, *args, **kwargs)
        def open_and_close():
            db = p.StateDB(old)
            try:
                return db.conn.execute('PRAGMA user_version').fetchone()[0]
            finally:
                db.close()
        with patch.object(p.sqlite3, 'connect', side_effect=wrapped):
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(lambda _: open_and_close(), range(2)))
        self.assertEqual(results, [1, 1])

    def test_old_database_is_backed_up_before_migration(self):
        old = self.root / 'old.db'
        con = sqlite3.connect(old)
        legacy = p.SCHEMA_SQL
        for name in ('source_fingerprint', 'audio_fingerprint', 'asr_fingerprint', 'asr_phase'):
            import re
            legacy = re.sub(r'^.*\b' + name + r'\b.*\n', '', legacy, flags=re.M)
        con.executescript(legacy)
        con.close()
        db = p.StateDB(old)
        try:
            self.assertEqual(db.conn.execute('PRAGMA user_version').fetchone()[0], 1)
            backups = list((old.parent / '.migration-backups').glob('old.pre-v1-*.db'))
            self.assertEqual(len(backups), 1)
            saved = sqlite3.connect(backups[0])
            try:
                cols = {r[1] for r in saved.execute('PRAGMA table_info(media_items)')}
                self.assertNotIn('asr_phase', cols)
            finally:
                saved.close()
        finally:
            db.close()


if __name__ == '__main__':
    unittest.main()
