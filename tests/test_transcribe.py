import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import asr_pipeline as p


class TranscriptionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db_path = self.root / 'pipeline.db'
        self.audio = self.root / 'clip.mp3'
        self.audio.write_bytes(b'audio')
        self.db = p.StateDB(self.db_path)
        self.db.upsert_videos('group', [('clip', 'clip', str(self.root/'clip.mp4'), 'clip.mp4', 5, 1)])
        self.item_id = int(self.db.get_all_group_items('group')[0]['id'])
        self.db.mark_audio_done(self.item_id, preset='asr-mp3', audio_path=self.audio,
            audio_format='mp3', audio_size_bytes=5, duration_sec=200)
        self.db.close()
        self.calls = []

    def tearDown(self):
        self.temp.cleanup()

    def row(self):
        db = p.StateDB(self.db_path)
        try:
            return dict(db.get_all_group_items('group')[0])
        finally:
            db.close()

    def run_cli(self, http, *extra):
        args = p.build_parser().parse_args(['--db', str(self.db_path), 'transcribe',
            '--url', 'http://fixture.local', '--group', 'group', '--prefetch', '0',
            '--concurrency', '1', '--poll-retries', '0', '--output-dir', str(self.root/'out'), *extra])
        with patch.object(p, '_http_request', side_effect=http), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            return p.cmd_transcribe(args)

    def success(self, method, url, **kwargs):
        self.calls.append((method, url))
        if method == 'POST':
            payload = {'task_id': 'task-1'}
        elif '/subtitle?' in url:
            return 200, b'1\n00:00:00,000 --> 00:00:01,000\nhello\n', 'text/plain'
        else:
            payload = {'status': 'completed', 'text': 'hello', 'segments': [{'start': 0, 'end': 1, 'text': 'hello'}]}
        return 200, json.dumps(payload).encode(), 'application/json'

    def test_http_error_retains_status_and_both_retry_after_formats(self):
        for value in ('120', 'Thu, 01 Jan 1970 00:18:40 GMT'):
            error = p.urllib.error.HTTPError('http://fixture.local', 429, 'limited', {'Retry-After': value}, io.BytesIO(b'wait'))
            with self.subTest(header=value), patch.object(p.urllib.request, 'urlopen', side_effect=error), patch.object(p.time, 'time', return_value=1000):
                with self.assertRaises(p.AsrHttpError) as raised:
                    p._http_request('GET', 'http://fixture.local')
                self.assertEqual(raised.exception.status, 429)
                self.assertEqual(raised.exception.retry_after, 120)

    def test_poll_respects_retry_after_without_reposting(self):
        client_type = p.AsrClient
        waits = []
        polls = 0
        def http(method, url, **kwargs):
            nonlocal polls
            if method == 'GET' and '/subtitle?' not in url:
                polls += 1
                if polls == 1:
                    self.calls.append((method, url))
                    raise p.AsrHttpError(429, 'limited', 120)
            return self.success(method, url, **kwargs)
        with patch.object(p, 'AsrClient', side_effect=lambda ep, args, stop: client_type(ep, args, stop, wait=waits.append)):
            self.assertEqual(self.run_cli(http, '--poll-retries', '1'), 0)
        self.assertEqual(waits, [120])
        self.assertEqual(sum(method=='POST' for method, _ in self.calls), 1)

    def test_transient_poll_failure_resumes_without_post(self):
        def failing(method, url, **kwargs):
            if method == 'POST':
                return self.success(method, url, **kwargs)
            self.calls.append((method, url))
            raise TimeoutError('temporary poll failure')
        self.assertEqual(self.run_cli(failing), 2)
        row = self.row()
        self.assertEqual(row['asr_task_id'], 'task-1')
        self.assertIn(row['asr_status'], ('queued', 'running'))
        self.calls.clear()
        self.assertEqual(self.run_cli(self.success), 0)
        self.assertNotIn('POST', [method for method, _ in self.calls])

    def test_unknown_submission_is_not_repeated(self):
        def timeout(method, url, **kwargs):
            self.calls.append((method, url))
            raise TimeoutError('upload result unknown')
        self.assertEqual(self.run_cli(timeout), 2)
        self.assertEqual(self.row()['asr_phase'], 'submission_unknown')
        self.calls.clear()
        self.assertEqual(self.run_cli(self.success), 2)
        self.assertEqual(self.calls, [])

    def test_done_result_is_not_resubmitted_and_changed_options_invalidate(self):
        self.assertEqual(self.run_cli(self.success), 0)
        self.calls.clear()
        self.assertEqual(self.run_cli(self.success), 0)
        self.assertEqual(self.calls, [])
        self.assertEqual(self.run_cli(self.success, '--language', 'zh'), 0)
        self.assertIn('POST', [method for method, _ in self.calls])

    def test_local_write_failure_preserves_remote_task(self):
        with patch.object(p.TranscriptStore, 'commit', side_effect=OSError('disk full')):
            self.assertEqual(self.run_cli(self.success), 2)
        self.assertNotIn('DELETE', [method for method, _ in self.calls])
        self.assertEqual(self.row()['asr_task_id'], 'task-1')
        self.calls.clear()
        self.assertEqual(self.run_cli(self.success), 0)
        self.assertNotIn('POST', [method for method, _ in self.calls])

    def test_conflicting_output_names_never_upload(self):
        db = p.StateDB(self.db_path)
        db.upsert_videos('group', [('clip2', 'clip', str(self.root/'clip.mkv'), 'clip.mkv', 5, 1)])
        item_id = int(db.get_all_group_items('group')[0]['id'])
        # Locate the second record without relying on sort order.
        item_id = next(int(r['id']) for r in db.get_all_group_items('group') if r['video_rel_path']=='clip.mkv')
        db.mark_audio_done(item_id, preset='asr-mp3', audio_path=self.audio,
            audio_format='mp3', audio_size_bytes=5, duration_sec=200)
        db.close()
        self.assertEqual(self.run_cli(self.success), 2)
        self.assertEqual(self.calls, [])

    def test_active_task_option_change_requires_explicit_overwrite(self):
        def fail_poll(method, url, **kwargs):
            if method == 'POST':
                return self.success(method, url, **kwargs)
            raise TimeoutError('temporarily offline')
        self.assertEqual(self.run_cli(fail_poll), 2)
        self.calls.clear()
        self.assertEqual(self.run_cli(self.success, '--language', 'zh'), 2)
        self.assertEqual(self.calls, [])
        self.assertEqual(self.row()['asr_task_id'], 'task-1')
        self.assertEqual(self.run_cli(self.success, '--language', 'zh', '--overwrite'), 0)
        self.assertIn('POST', [method for method, _ in self.calls])

    def test_known_async_task_resumes_even_if_sync_routing_selected(self):
        def fail_poll(method, url, **kwargs):
            if method == 'POST':
                return self.success(method, url, **kwargs)
            raise TimeoutError('temporarily offline')
        self.assertEqual(self.run_cli(fail_poll), 2)
        self.calls.clear()
        self.assertEqual(self.run_cli(self.success, '--mode', 'sync'), 0)
        self.assertNotIn('POST', [method for method, _ in self.calls])

    def test_json_gap_does_not_convert_done_into_failed_and_reupload(self):
        self.assertEqual(self.run_cli(self.success), 0)
        self.calls.clear()
        self.assertEqual(self.run_cli(self.success, '--save-json'), 2)
        self.assertEqual(self.row()['asr_status'], 'done')
        self.assertEqual(self.calls, [])
        self.assertEqual(self.run_cli(self.success, '--save-json'), 2)
        self.assertEqual(self.calls, [])

    def test_json_output_cannot_overwrite_another_completion_marker(self):
        db = p.StateDB(self.db_path)
        db.upsert_videos('group', [('other', 'other', str(self.root/'.clip.complete.mp4'), '.clip.complete.mp4', 5, 1)])
        item_id = next(int(r['id']) for r in db.get_all_group_items('group') if r['video_rel_path']=='.clip.complete.mp4')
        db.mark_audio_done(item_id, preset='asr-mp3', audio_path=self.audio,
            audio_format='mp3', audio_size_bytes=5, duration_sec=200)
        db.close()
        self.assertEqual(self.run_cli(self.success, '--save-json'), 2)
        self.assertEqual(self.calls, [])

    def test_prefetch_uses_same_task_and_attempt_rules(self):
        self.assertEqual(self.run_cli(self.success, '--prefetch', '2'), 0)
        self.assertEqual(sum(method=='POST' for method, _ in self.calls), 1)
        self.assertEqual(self.row()['asr_attempts'], 1)
        self.assertEqual(self.row()['asr_phase'], None)

    def test_reconcile_defaults_to_dry_run_then_repairs(self):
        self.assertEqual(self.run_cli(self.success), 0)
        row = self.row()
        Path(row['srt_path']).unlink()
        args = p.build_parser().parse_args(['--db', str(self.db_path), 'reconcile'])
        with contextlib.redirect_stdout(io.StringIO()):
            p.cmd_reconcile(args)
        self.assertEqual(self.row()['asr_status'], 'done')
        args.apply = True
        with contextlib.redirect_stdout(io.StringIO()):
            p.cmd_reconcile(args)
        self.assertEqual(self.row()['asr_status'], 'pending')


if __name__ == '__main__':
    unittest.main()
