import time
from extract.mbta_client import MBTAClient
from extract.snowflake_loader import load_records, get_connection

t0 = time.time()
print("connecting to Snowflake...", flush=True)
with get_connection() as conn:
    print(f"  connected in {time.time()-t0:.1f}s", flush=True)
    cur = conn.cursor()
    print("querying referenced stop_ids...", flush=True)
    cur.execute('''
        select distinct stop_id from RAW.MBTA.PREDICTIONS where stop_id is not null
        union
        select distinct stop_id from RAW.MBTA.SCHEDULES   where stop_id is not null
    ''')
    stop_ids = [r[0] for r in cur.fetchall()]
print(f"{len(stop_ids)} distinct stops referenced (t={time.time()-t0:.1f}s)", flush=True)

client = MBTAClient()
stops = []
BATCH = 100
for i in range(0, len(stop_ids), BATCH):
    batch = stop_ids[i:i+BATCH]
    print(f"  pulling stops {i}..{i+len(batch)} (t={time.time()-t0:.1f}s)", flush=True)
    stops.extend(client.get_all('stops', {'filter[id]': ','.join(batch)}))
print(f"pulled {len(stops)} stop records (t={time.time()-t0:.1f}s)", flush=True)

load_records(stops, table='stops', mode='overwrite')
print(f"stops reloaded (t={time.time()-t0:.1f}s)", flush=True)
