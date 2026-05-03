"""Testes de PartitionedBuffer."""
import time
import unittest

from streaming.buffer.partitioned_buffer import PartitionedBuffer


def _evt(factory="F1", mtype="temperature", ts="2025-01-01T10:00:00"):
    return {
        "factory_id": factory,
        "measurement_type": mtype,
        "timestamp": ts,
        "value": 1.0,
    }


class TestPartitionedBuffer(unittest.TestCase):

    def test_partition_key_format(self):
        buf = PartitionedBuffer(flush_size=10, flush_interval=30)
        key = buf.partition_key(_evt())
        self.assertEqual(
            key,
            "factory_id=F1/measurement_type=temperature/dt=2025-01-01"
        )

    def test_partition_key_with_invalid_timestamp(self):
        """Deve cair no fallback (data UTC atual) sem levantar exceção."""
        buf = PartitionedBuffer(flush_size=10, flush_interval=30)
        key = buf.partition_key({"timestamp": "not-a-date", "factory_id": "F1"})
        self.assertIn("factory_id=F1", key)
        self.assertIn("dt=", key)

    def test_partition_key_unknown_fields(self):
        buf = PartitionedBuffer(flush_size=10, flush_interval=30)
        key = buf.partition_key({})
        self.assertIn("UNKNOWN", key)

    def test_add_groups_by_partition(self):
        buf = PartitionedBuffer(flush_size=10, flush_interval=30)
        buf.add(_evt("F1", "temperature"))
        buf.add(_evt("F1", "temperature"))
        buf.add(_evt("F2", "humidity"))
        self.assertEqual(buf.total(), 3)
        snapshot = buf.drain()
        self.assertEqual(len(snapshot), 2)

    def test_should_flush_by_size(self):
        buf = PartitionedBuffer(flush_size=2, flush_interval=999)
        self.assertFalse(buf.should_flush())
        buf.add(_evt())
        self.assertFalse(buf.should_flush())
        buf.add(_evt())
        self.assertTrue(buf.should_flush())

    def test_should_flush_by_interval(self):
        buf = PartitionedBuffer(flush_size=999, flush_interval=0.1)
        buf.add(_evt())
        self.assertFalse(buf.should_flush())
        time.sleep(0.15)
        self.assertTrue(buf.should_flush())

    def test_drain_resets_buffer(self):
        buf = PartitionedBuffer(flush_size=10, flush_interval=30)
        buf.add(_evt())
        buf.add(_evt())
        snapshot = buf.drain()
        self.assertEqual(sum(len(v) for v in snapshot.values()), 2)
        self.assertEqual(buf.total(), 0)

    def test_drain_resets_flush_clock(self):
        buf = PartitionedBuffer(flush_size=999, flush_interval=0.1)
        time.sleep(0.15)
        buf.drain()
        self.assertFalse(buf.should_flush())


if __name__ == "__main__":
    unittest.main()