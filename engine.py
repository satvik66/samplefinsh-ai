"""FinShift AI demo migration engine. All records are synthetic."""
from __future__ import annotations
import hashlib
import json
import random
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

DB_PATH = Path(__file__).parent / 'finshift_demo.db'


def connect(path=None):
    db = sqlite3.connect(str(path or DB_PATH))
    db.row_factory = sqlite3.Row
    return db


def initialize(path=None, reset=False):
    db = connect(path)
    if reset:
        db.executescript('DROP TABLE IF EXISTS legacy_transactions; DROP TABLE IF EXISTS bronze_transactions; DROP TABLE IF EXISTS silver_transactions; DROP TABLE IF EXISTS gold_daily_summary; DROP TABLE IF EXISTS migration_runs; DROP TABLE IF EXISTS rejections; DROP TABLE IF EXISTS pipeline_state;')
    db.executescript('''
    CREATE TABLE IF NOT EXISTS legacy_transactions (source_id INTEGER PRIMARY KEY, account_id TEXT, amount REAL, currency TEXT, merchant TEXT, event_at TEXT, updated_at TEXT);
    CREATE TABLE IF NOT EXISTS bronze_transactions (source_id INTEGER PRIMARY KEY, account_id TEXT, amount REAL, currency TEXT, merchant TEXT, event_at TEXT, updated_at TEXT, ingested_at TEXT);
    CREATE TABLE IF NOT EXISTS silver_transactions (source_id INTEGER PRIMARY KEY, account_id TEXT NOT NULL, amount REAL NOT NULL, currency TEXT NOT NULL, merchant TEXT, event_at TEXT, updated_at TEXT);
    CREATE TABLE IF NOT EXISTS gold_daily_summary (event_day TEXT PRIMARY KEY, transaction_count INTEGER, amount_usd REAL);
    CREATE TABLE IF NOT EXISTS migration_runs (run_id INTEGER PRIMARY KEY AUTOINCREMENT, started_at TEXT, finished_at TEXT, status TEXT, mode TEXT, scanned INTEGER, accepted INTEGER, rejected INTEGER, diagnostics TEXT);
    CREATE TABLE IF NOT EXISTS rejections (run_id INTEGER, source_id INTEGER, reason TEXT, payload TEXT);
    CREATE TABLE IF NOT EXISTS pipeline_state (name TEXT PRIMARY KEY, last_updated_at TEXT);
    ''')
    db.commit()
    db.close()


def seed_synthetic(path=None, count=750, inject_errors=True):
    initialize(path)
    db = connect(path)
    rnd = random.Random(42)
    start = datetime(2026, 9, 1, tzinfo=timezone.utc)
    merchants = ['Grocery', 'Fuel', 'Travel', 'Online', 'Healthcare']
    records = []
    for i in range(1, count+1):
        event = start + timedelta(days=rnd.randint(0, 29), hours=rnd.randint(0,23))
        records.append((i, f'ACCT-{rnd.randint(100,169)}', round(rnd.uniform(5,1500),2), 'USD', rnd.choice(merchants), event.isoformat(), (event+timedelta(days=1)).isoformat()))
    if inject_errors and count >= 20:
        records[6] = (7, None, 45.10, 'USD', 'Online', records[6][5], records[6][6])
        records[15] = (16, 'ACCT-101', -80.0, 'USD', 'Travel', records[15][5], records[15][6])
        records[18] = (19, 'ACCT-102', 23.15, 'XYZ', 'Online', records[18][5], records[18][6])
    db.executemany('INSERT OR REPLACE INTO legacy_transactions VALUES (?,?,?,?,?,?,?)', records)
    db.commit()
    db.close()
    return len(records)


def _now():
    return datetime.now(timezone.utc).isoformat()


def migrate(path=None, mode='full'):
    if mode not in ('full','incremental'):
        raise ValueError('mode must be full or incremental')
    initialize(path)
    db = connect(path)
    started = _now()
    state = db.execute("SELECT last_updated_at FROM pipeline_state WHERE name='legacy_transactions'").fetchone()
    cutoff = state['last_updated_at'] if state and mode=='incremental' else None
    sql = 'SELECT * FROM legacy_transactions'
    args = []
    if cutoff:
        sql += ' WHERE updated_at > ?'
        args.append(cutoff)
    sql += ' ORDER BY updated_at, source_id'
    rows = db.execute(sql, args).fetchall()
    accepted, rejected = 0, 0
    errors = []
    try:
        with db:
            cur = db.execute('INSERT INTO migration_runs (started_at,status,mode,scanned,accepted,rejected) VALUES (?,?,?,?,?,?)', (started,'RUNNING',mode,len(rows),0,0))
            run_id = cur.lastrowid
            if mode == 'full':
                db.execute('DELETE FROM bronze_transactions')
                db.execute('DELETE FROM silver_transactions')
            for r in rows:
                d = dict(r)
                db.execute('INSERT OR REPLACE INTO bronze_transactions VALUES (?,?,?,?,?,?,?,?)', (*tuple(d.values()), _now()))
                why = []
                if not d['account_id']: why.append('null account key')
                if d['amount'] is None or d['amount'] < 0: why.append('invalid amount')
                if d['currency'] != 'USD': why.append('unsupported currency')
                if why:
                    rejected += 1
                    error = {'source_id': d['source_id'], 'reason': ', '.join(why)}
                    errors.append(error)
                    db.execute('DELETE FROM silver_transactions WHERE source_id=?', (d['source_id'],))
                    db.execute('INSERT INTO rejections VALUES (?,?,?,?)', (run_id,d['source_id'], error['reason'], json.dumps(d)))
                    continue
                db.execute('INSERT OR REPLACE INTO silver_transactions VALUES (?,?,?,?,?,?,?)', tuple(d.values()))
                accepted += 1
            db.execute('DELETE FROM gold_daily_summary')
            db.execute('''INSERT INTO gold_daily_summary SELECT substr(event_at,1,10), COUNT(*), ROUND(SUM(amount),2)
                          FROM silver_transactions GROUP BY substr(event_at,1,10)''')
            if rows:
                max_updated = max(r['updated_at'] for r in rows)
                db.execute("INSERT INTO pipeline_state VALUES ('legacy_transactions',?) ON CONFLICT(name) DO UPDATE SET last_updated_at=excluded.last_updated_at", (max_updated,))
            db.execute('UPDATE migration_runs SET finished_at=?, status=?, accepted=?, rejected=?, diagnostics=? WHERE run_id=?', (_now(),'COMPLETED',accepted,rejected,json.dumps(errors[:25]),run_id))
    except Exception:
        db.rollback()
        db.close()
        raise
    db.close()
    return {'run_id':run_id,'mode':mode,'scanned':len(rows),'accepted':accepted,'rejected':rejected,'status':'COMPLETED'}


def metrics(path=None):
    initialize(path)
    db=connect(path)
    tables=['legacy_transactions','bronze_transactions','silver_transactions','gold_daily_summary']
    data={table:db.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0] for table in tables}
    data['source_amount']=round(db.execute('SELECT COALESCE(SUM(amount),0) FROM legacy_transactions WHERE account_id IS NOT NULL AND amount >= 0 AND currency="USD"').fetchone()[0],2)
    data['target_amount']=round(db.execute('SELECT COALESCE(SUM(amount),0) FROM silver_transactions').fetchone()[0],2)
    data['runs']=[dict(r) for r in db.execute('SELECT * FROM migration_runs ORDER BY run_id DESC LIMIT 20')]
    data['rejections']=[dict(r) for r in db.execute('SELECT run_id,source_id,reason FROM rejections ORDER BY run_id DESC, source_id LIMIT 100')]
    data['daily']=[dict(r) for r in db.execute('SELECT * FROM gold_daily_summary ORDER BY event_day')]
    data['source_target_match']=(data['source_amount']==data['target_amount'] and data['legacy_transactions']==data['silver_transactions']+len({r['source_id'] for r in db.execute('SELECT DISTINCT source_id FROM rejections WHERE run_id=(SELECT MAX(run_id) FROM migration_runs)')})) if data['runs'] and data['runs'][0]['mode']=='full' else (data['source_amount']==data['target_amount'])
    db.close()
    return data


def inspect_issue(path=None):
    m=metrics(path)
    last=m['runs'][0] if m['runs'] else None
    if not last:
        return 'No migration run exists. Seed synthetic legacy data and run a migration first.'
    reasons={}
    for r in m['rejections']:
        if r['run_id']==last['run_id']:
            reasons[r['reason']]=reasons.get(r['reason'],0)+1
    details='; '.join(f'{reason}: {count}' for reason,count in reasons.items()) or 'No rejected records'
    return (f"Latest run #{last['run_id']} ({last['mode']}) scanned {last['scanned']} records, "
            f"accepted {last['accepted']}, rejected {last['rejected']}. Issues: {details}. "
            f"Accepted source total ${m['source_amount']:,.2f}; target total ${m['target_amount']:,.2f}. "
            f"Totals reconciled: {m['source_amount']==m['target_amount']}.")


def assistant_answer(question, path=None, api_key=''):
    context=inspect_issue(path)
    if not api_key:
        return ('**Evidence-based offline diagnosis**\n\n' + context +
          '\n\n**Recommended actions:** Inspect rejected rows, correct source quality issues, and rerun the full migration. '
          'Incremental mode processes only updated source timestamps. Verify row counts and accepted-amount totals after every run. '
          '\n\n*Connect an OpenAI API key for natural-language explanations. The offline response is deterministic, not an LLM.*')
    try:
        from openai import OpenAI
        response=OpenAI(api_key=api_key).responses.create(
          model='gpt-4.1-mini',
          instructions='You are a data migration reliability copilot. Respond using only provided synthetic migration evidence. Never claim access to other systems, invent metrics, expose secrets, or suggest executing unsafe destructive changes automatically. Clearly separate verified facts from recommendations. Keep responses concise.',
          input=f'Migration evidence: {context}\n\nQuestion: {question}')
        return response.output_text
    except Exception as exc:
        return f'AI request unavailable ({type(exc).__name__}). Offline evidence:\n\n{context}'

if __name__=='__main__':
    seed_synthetic()
    print(migrate())
    print(inspect_issue())
