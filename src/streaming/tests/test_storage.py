"""Testes de DryRunClient e contrato StorageClient."""
import unittest

from streaming.storage.base_storage import StorageClient
from streaming.storage.dryrun_client import DryRunClient


class TestDryRunClient(unittest.TestCase):

    def setUp(self):
        self.client = DryRunClient()

    def test_implements_storage_protocol(self):
        # Protocol runtime_checkable → isinstance verifica métodos do contrato
        self.assertIsInstance(self.client, StorageClient)

    def test_put_json_returns_byte_count(self):
        events = [
            {"sensor_id": "s1", "value": 1.0},
            {"sensor_id": "s2", "value": 2.0},
        ]
        size = self.client.put_json("foo/bar.ndjson", events)
        self.assertGreater(size, 0)
        self.assertGreater(size, len('{"sensor_id":"s1","value":1.0}'))

    def test_put_json_empty_list(self):
        size = self.client.put_json("foo/empty.ndjson", [])
        self.assertEqual(size, 0)

    def test_put_json_handles_unicode(self):
        events = [{"sensor_id": "máquina-π", "value": 1.0}]
        size = self.client.put_json("foo/uni.ndjson", events)
        self.assertGreater(size, 0)


if __name__ == "__main__":
    unittest.main()