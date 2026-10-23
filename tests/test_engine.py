import sys
import tempfile
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from engine import seed_synthetic, migrate, metrics, connect

class MigrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.path=Path(self.tmp.name)/'test.db'
        seed_synthetic(self.path, count=100, inject_errors=True)
    def tearDown(self): self.tmp.cleanup()
    def test_full_load_and_reconciliation(self):
        res=migrate(self.path)
        self.assertEqual(res['scanned'],100)
        self.assertEqual(res['rejected'],3)
        self.assertEqual(res['accepted'],97)
        self.assertTrue(metrics(self.path)['source_target_match'])
    def test_repeat_full_load_is_idempotent(self):
        migrate(self.path)
        migrate(self.path)
        self.assertEqual(metrics(self.path)['silver_transactions'],97)
    def test_incremental_upsert(self):
        migrate(self.path)
        with connect(self.path) as db:
            db.execute("UPDATE legacy_transactions SET amount=999,updated_at='2099-01-01T00:00:00+00:00' WHERE source_id=1")
        r=migrate(self.path,'incremental')
        self.assertEqual(r['scanned'],1)
        self.assertTrue(metrics(self.path)['source_target_match'])

if __name__=='__main__': unittest.main()
