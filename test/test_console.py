import sys
import unittest
from unittest.mock import patch
from io import StringIO
import json

from pyshard.console import pyshard as console


class TestConsole(unittest.TestCase):
    TOWRITE = [
        ('2', '{"test": "test"}', json.loads),
        ('3', '42', int),
        ('1', 'test', str),
        ('4', '0.9', float)
    ]

    def _prepare_mock_stdin(self):
        for line in self.TOWRITE:
            sys.stdin.write(f'{line[0]}|{line[1]}\n')
        sys.stdin.flush()
        sys.stdin.seek(0)

    def _read_mock_stdout(self):
        result = []
        sys.stdout.seek(0)
        for line in sys.stdout:
            key, raw_doc = line.rstrip('\n').split('|')
            doc = json.loads(raw_doc)
            result.append((key, doc['record']))

        result.sort(key=lambda x: x[0])

        return result

    @patch('sys.stdin', StringIO())
    @patch('sys.stdout', StringIO())
    def test_write_and_cat(self):
        index = 'test_index'
        self._prepare_mock_stdin()

        console.write(index, force=True)
        console.cat(index)

        self.TOWRITE.sort(key=lambda x: x[0])
        target = [(key, serializer(doc)) for key, doc, serializer in self.TOWRITE]

        self.assertEqual(target, self._read_mock_stdout())
