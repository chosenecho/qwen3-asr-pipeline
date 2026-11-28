import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import asr_pipeline as p


class OutputTests(unittest.TestCase):
    def test_dotted_names_keep_full_stem(self):
        paths = p.build_output_paths(Path('out'), 'group', 'lecture.part1.mp4', True)
        self.assertEqual(paths['txt'].name, 'lecture.part1.txt')
        self.assertEqual(paths['marker'].name, '.lecture.part1.complete.json')

    def test_result_marker_checks_all_files_and_request(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = p.build_output_paths(root, 'group', 'clip.mp4', True)
            store = p.TranscriptStore(paths)
            store.commit('hello', 'subtitle', {'text': 'hello'}, 'source', 'request', '/fixture/clip.mp3')
            self.assertTrue(store.valid('source', 'request'))
            self.assertFalse(store.valid('source', 'other-request'))
            paths['txt'].write_text('changed', encoding='utf-8')
            self.assertFalse(store.valid('source', 'request'))

    def test_empty_speech_is_valid_when_explicit(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = p.build_output_paths(Path(tmp), 'group', 'clip.mp4', False)
            store = p.TranscriptStore(paths)
            store.commit('', '', {'text': ''}, 'source', 'request', '/fixture/audio')
            self.assertTrue(store.valid('source', 'request'))

    def test_partial_replace_never_validates_old_marker(self):
        with tempfile.TemporaryDirectory() as tmp:
            paths = p.build_output_paths(Path(tmp), 'group', 'clip.mp4', False)
            store = p.TranscriptStore(paths)
            store.commit('old', 'old-subtitle', {}, 'source', 'old-request', '/fixture/audio')
            original = p.os.replace
            def broken(src, dst):
                if Path(dst) == paths['srt']:
                    raise OSError('disk failure')
                return original(src, dst)
            with patch.object(p.os, 'replace', side_effect=broken):
                with self.assertRaises(OSError):
                    store.commit('new', 'new-subtitle', {}, 'source', 'new-request', '/fixture/audio')
            self.assertFalse(store.valid('source', 'new-request'))
            self.assertFalse(store.valid('source', 'old-request'))

    def test_contract_fixtures(self):
        cases = json.loads((Path(__file__).parent / 'fixtures' / 'asr_cases.json').read_text(encoding='utf-8'))
        for case in cases:
            with self.subTest(case=case['name']):
                if case.get('error'):
                    with self.assertRaises(p.AsrProtocolError):
                        p._extract_segments_and_text(case['payload'])
                else:
                    text, segments = p._extract_segments_and_text(case['payload'])
                    self.assertEqual(text, case['text'])
                    self.assertEqual(segments, case['segments'])


if __name__ == '__main__':
    unittest.main()
