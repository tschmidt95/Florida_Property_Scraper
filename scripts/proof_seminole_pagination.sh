#!/usr/bin/env bash
set -euo pipefail

python - <<'PY'
import json, urllib.request
BASE='http://127.0.0.1:8000'
county='seminole'
stats=json.load(urllib.request.urlopen(f"{BASE}/api/debug/db_stats?county={county}"))
poly=stats['suggested_polygon']
limit=500
cursor=None
all_ids=set()
page=0
total=None
while True:
    page += 1
    payload={'county':county,'polygon_geojson':poly,'limit':limit,'include_geometry':False,'explain':True}
    if cursor is not None:
        payload['cursor']=cursor
    req=urllib.request.Request(
        f"{BASE}/api/parcels/search?county={county}&explain=1",
        data=json.dumps(payload).encode('utf-8'),
        headers={'Content-Type':'application/json'},
        method='POST',
    )
    resp=json.load(urllib.request.urlopen(req))
    recs=resp.get('records',[])
    for r in recs:
        pid=str(r.get('parcel_id') or '')
        ckey=str(r.get('county') or '')
        if pid:
            all_ids.add(f"{ckey}:{pid}")
    ex=resp.get('explain') or {}
    total = resp.get('total_count', ex.get('total_count', total))
    has_more = resp.get('has_more', ex.get('has_more', False))
    cursor = resp.get('next_cursor')
    print({'page':page,'returned':len(recs),'total':total,'has_more':has_more,'next_cursor':bool(cursor)})
    if not has_more:
        break
print({'unique_ids':len(all_ids),'total_count':total})

def search(extra=None):
    payload={'county':county,'polygon_geojson':poly,'limit':500,'include_geometry':False,'explain':True}
    if extra:
        payload.update(extra)
    req=urllib.request.Request(
        f"{BASE}/api/parcels/search?county={county}&explain=1",
        data=json.dumps(payload).encode('utf-8'),
        headers={'Content-Type':'application/json'},
        method='POST',
    )
    return json.load(urllib.request.urlopen(req))

base=search()
print({'base_total':base.get('total_count'),'base_returned':base.get('returned_count')})
first_pid=None
try:
    if base.get('records'):
        r0=base['records'][0]
        first_pid=str(r0.get('parcel_id') or '')
        print({'first_record':{'parcel_id':first_pid,'county':r0.get('county'), 'signal_keys': r0.get('signal_keys')}})
except Exception:
    pass

if first_pid:
    try:
        import sqlite3
        con = sqlite3.connect('/workspaces/Florida_Property_Scraper/leads.sqlite')
        cur = con.cursor()
        row = cur.execute("SELECT details_json FROM parcel_trigger_rollups WHERE county='seminole' AND parcel_id=?", (first_pid,)).fetchone()
        con.close()
        print({'rollup_details_json': row[0] if row else None})
    except Exception as e:
        print({'rollup_query_error': str(e)})
flt_note='min_sqft'
flt_value=None
vals=[]
try:
    for r in (base.get('records') or []):
        v = r.get('living_area_sqft')
        if isinstance(v, (int, float)) and v > 0:
            vals.append(float(v))
except Exception:
    vals=[]
if vals:
    vals.sort()
    flt_value = vals[len(vals)//2]
else:
    flt_value = 1500
flt=search({'filters':{'min_sqft':flt_value,'missing_policy':'strict'}})
if (flt.get('total_count') in (0, None)) and base.get('records'):
    pid = str(base['records'][0].get('parcel_id') or '').strip()
    if pid:
        flt=search({'filters':[{'field':'parcel_id','op':'contains','value':pid}]})
        flt_note='parcel_id_contains'
print({'filter_total':flt.get('total_count'),'filter_returned':flt.get('returned_count'),'filter_note':flt_note,'filter_value':flt_value})

sig_key=None
try:
    for r in (base.get('records') or []):
        keys = r.get('signal_keys') or []
        if keys:
            sig_key = keys[0]
            break
    if not sig_key:
        for r in (base.get('records') or []):
            sigs = r.get('signals') or {}
            for k, v in sigs.items():
                if v:
                    sig_key = k
                    break
            if sig_key:
                break
except Exception:
    sig_key=None

sig_key = sig_key or 'permit_hvac'
sig=search({'trigger_keys':[sig_key]})
print({'signal_key_used':sig_key,'signal_total':sig.get('total_count'),'signal_returned':sig.get('returned_count')})
PY
