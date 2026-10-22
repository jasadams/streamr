#!/usr/bin/env python3
"""Prepare native grouped-selector CDC SQL and an independent sorting oracle.

No Streamr operators or captures participate in expected-value calculation.
Run generated manifest with test-native-result-manifest.py (8 default cases).
"""
import argparse
from functools import cmp_to_key
import json
from pathlib import Path
from statistics import median, stdev

CHECKPOINT = 19


def events():
    live, result = {}, []

    def write(row_id, group=None, second=None, value=None, priority=None, payload=None):
        before=live.get(row_id)
        after=None if group is None else dict(row_id=row_id,canonical_id=group,
            event_time=f'2026-10-18T00:00:{second:02d}Z',
            progress_time=f'1970-01-01T00:00:{len(result)+11:02d}Z',value=value,
            priority=priority,payload=json.dumps(payload or {},separators=(',',':')))
        result.append(dict(before=before,after=after,
            op='d' if after is None else ('c' if before is None else 'u'),
            ts_ms=1000*(len(result)+1)))
        if after is None:
            del live[row_id]
        else:
            live[row_id]=after

    # Independent keys keep every scenario observable at mandatory endpoints.
    write(1,'raw',20,'A',0)
    write(2,'raw',10,'B',999)
    write(3,'ties',20,'A',0)
    write(4,'ties',20,'B',1)
    write(5,'ordinal',20,'A',0)
    write(6,'ordinal',20,'B',0)
    write(7,'null-order',20,'A',0)
    write(8,'null-order',20,'N',None)
    write(9,'nullable',20,'A',0,dict(assignment='set',locale='en',flag=True,number=5,
        identifiers=[dict(identity_type='email',value='a@example.test')],
        touch=dict(source='one',medium='paid')))
    write(10,'nullable',21,None,0,dict(assignment=None,locale='',language='fr',flag=False,number=0,
        identifiers=[dict(identity_type='email',value=None)],identity_type='email',
        user_id='fallback@example.test',touch=dict(source='two',medium='')))
    write(11,'blank',20,'A',0,dict(assignment='set'))
    write(12,'blank',21,'',0,dict(assignment='',identifiers=[dict(identity_type='email',value='')],
        identity_type='email',user_id='blank-fallback@example.test'))
    write(20,'identity',20,'X',0)
    write(21,'identity',20,'Y',0)
    write(22,'identity',20,'X',0)
    write(30,'replacement',20,'X',0)
    write(31,'replacement',20,'Y',0)
    write(40,'email-priority',20,'A',0,dict(identifiers=[dict(identity_type='email',value=None)]))
    write(41,'email-priority',20,'B',0,dict(identity_type='email',user_id='fallback@example.test'))
    # Checkpoint preserves all original identities and ordinals before suffix.
    write(13,'ties',20,'C',1)
    write(14,'ordinal',20,'C',0)
    write(15,'nullable',22,None,0)  # Missing fields cannot clear assignments.
    write(22)  # Interleaved X/Y/X: removing later X must expose Y.
    write(30,'replacement',20,'Z',0)
    write(30)  # Removing replacement exposes the surviving original Y ordinal.
    return result


def select(rows, expression, *, last=True, secondary=False, descending=False,
           nulls_first=False, ignore_nulls=False, eligible=lambda row: True):
    candidates = [(row, ordinal, expression(row)) for row, ordinal in rows if eligible(row)]
    if ignore_nulls:
        candidates = [item for item in candidates if item[2] is not None]

    def compare(left, right):
        a, b = left[0]['event_time'], right[0]['event_time']
        if a != b:
            return (a > b) - (a < b)
        if secondary:
            a, b = secondary(left[0]), secondary(right[0])
            if a != b:
                if a is None or b is None:
                    return (-1 if a is None else 1) * (1 if nulls_first else -1)
                return ((a > b) - (a < b)) * (-1 if descending else 1)
        return (left[1] > right[1]) - (left[1] < right[1])

    candidates.sort(key=cmp_to_key(compare))
    return candidates[-1 if last else 0][2] if candidates else None


def fixed_outputs(rows):
    """Select one typed two-element value from one candidate, including nulls."""
    def pair(row):
        priority=row['priority']
        return [priority,priority+1 if priority is not None else None]
    def nullable_pair(row):
        return None if row['value'] is None else pair(row)
    return dict(fixed_first=select(rows,pair,last=False),fixed_last=select(rows,pair),
        fixed_nullable_last=select(rows,nullable_pair),
        fixed_nonnull_last=select(rows,nullable_pair,ignore_nulls=True))


def preserved_aggregates(rows):
    """Ordinary SQL sample deviation and median over the actual multiset."""
    constants=[1.0 for _ in rows]
    filtered=[1.0 for row,_ in rows if row['value'] is not None]
    priorities=[float(row['priority']) for row,_ in rows if row['priority'] is not None]
    return dict(spread=stdev(constants) if len(constants)>=2 else None,
        filtered_spread=stdev(filtered) if len(filtered)>=2 else None,
        midpoint=float(median(priorities)) if priorities else None)


def output(rows):
    def payload(row):
        return json.loads(row['payload'])

    def email_parts(row):
        p = payload(row)
        first = next((item.get('value') for item in p.get('identifiers', [])
                      if item.get('identity_type') == 'email'), None)
        first = first if first != '' else None
        second = p.get('user_id') if p.get('identity_type') == 'email' else None
        return first, second if second != '' else None

    def catalog_parts(row):
        p = payload(row)
        matching = [item for item in p.get('identifiers', [])
                    if item.get('identity_type') == 'email' and 'value' in item]
        first = matching[0]['value'] if matching else None
        preferred = bool(matching) and first != ''
        fallback = p.get('identity_type') == 'email' and 'user_id' in p
        eligible = preferred or (fallback and p['user_id'] != '')
        value = first if preferred else p.get('user_id') if fallback else None
        return value, int(preferred), eligible

    value = lambda row: row['value']
    priority = lambda row: row['priority']
    pick = lambda expr, **kwargs: select(rows, expr, **kwargs)
    touch = pick(lambda row: payload(row)['touch'],
                 eligible=lambda row: 'touch' in payload(row))
    return dict(**fixed_outputs(rows),**preserved_aggregates(rows),canonical_id=rows[0][0]['canonical_id'],
        earliest_value=pick(value,last=False), latest_value=pick(value,secondary=priority),
        descending_value=pick(value,secondary=priority,descending=True,nulls_first=True),
        ordinal_first=pick(value,last=False,secondary=priority), ordinal_last=pick(value),
        tie_first=pick(value,last=False,secondary=priority,eligible=lambda row: row['priority']==1),
        interleaved_last=pick(value,eligible=lambda row: row['row_id']>=20),
        nulls_first=pick(value,secondary=priority,nulls_first=True),
        nulls_last=pick(value,secondary=priority),
        latest_nonblank=pick(lambda row: row['value'] if row['value'] != '' else None,
                             ignore_nulls=True),
        assignment=pick(lambda row: payload(row)['assignment'],
                        eligible=lambda row: 'assignment' in payload(row)),
        email=pick(lambda row: next((v for v in email_parts(row) if v is not None),None),
                   secondary=lambda row: int(email_parts(row)[0] is not None),ignore_nulls=True),
        catalog_email=pick(lambda row: catalog_parts(row)[0],
            secondary=lambda row: catalog_parts(row)[1],
            eligible=lambda row: catalog_parts(row)[2]),
        device_locale=pick(lambda row: next((payload(row).get(k) for k in ('locale','language')
                         if payload(row).get(k) not in (None,'')),None),ignore_nulls=True),
        flag=pick(lambda row: payload(row)['flag'],eligible=lambda row:'flag' in payload(row)),
        number=pick(lambda row: payload(row)['number'],eligible=lambda row:'number' in payload(row)),
        touch_source=touch.get('source') if touch else None,
        touch_medium=touch.get('medium') if touch else None)


def prefix_states(source):
    live, states, ordinal = {}, [], 0
    for event in source:
        before, after = event['before'], event['after']
        row_id = (after if after is not None else before)['row_id']
        if before != (live[row_id][0] if row_id in live else None):
            raise ValueError('CDC before image does not match original row identity')
        if after is None:
            del live[row_id]
        else:
            ordinal += 1
            live[row_id] = (after, ordinal)
        grouped={}
        for member in live.values():
            grouped.setdefault(member[0]['canonical_id'],[]).append(member)
        states.append([output(grouped[key]) for key in sorted(grouped)])
    return states


def analytic_fixture(last=False):
    # The row-time order deliberately differs from SQL priority order. At the
    # checkpoint the first window is finalized and the second remains buffered.
    # Watermark progress is the MIN AS value per source batch. Two complete
    # eight-row batches separate low data from advancing filtered controls.
    real = [dict(row_id=i,event_time=f'2026-10-18T00:00:{second:02d}Z',priority=priority)
              for i,second,priority in ((1,1,1),(2,2,3),(3,3,2),(4,21,7),(5,22,4),(6,40,0))]
    def control(identity,second):
        return dict(row_id=-identity,event_time=f'2026-10-18T00:00:{second:02d}Z',priority=0)
    source=(real[:3]+[control(i,1) for i in range(1,6)]
            +[real[3]]+[control(i,21) for i in range(6,13)]+real[4:])
    live, states = [], []
    for row in source:
        if row['row_id']>0:
            live.append(row)
        grouped = {}
        for member in live:
            grouped.setdefault(int(member['event_time'][17:19])//10,[]).append(member)
        snapshot=[]
        for members in grouped.values():
            ordered=sorted(members,key=lambda member: -member['priority'])
            for index,member in enumerate(ordered):
                selected=(ordered[min(index+1,len(ordered)-1)]['priority'] if last
                          else ordered[0]['priority'])
                snapshot.append(dict(row_id=member['row_id'],selected=selected))
        states.append(sorted(snapshot,key=lambda member: member['row_id']))
    query=Path(__file__).with_name('native-analytic-selectors.sql').read_text()
    expression=('LAST_VALUE(priority) OVER (PARTITION BY window ORDER BY priority DESC '
                'ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING)' if last else
                'FIRST_VALUE(priority) OVER (PARTITION BY window ORDER BY priority DESC '
                'ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)')
    query=query.replace('{{ANALYTIC_EXPRESSION}}',expression)
    checkpoint=[row for row in states[15] if row['row_id']<=3]
    return source,states,query,16,checkpoint,['row_id']


def prepare(directory, mode='cdc'):
    if mode.startswith('analytic-'):
        source,states,query,checkpoint,checkpoint_state,keys=analytic_fixture(mode=='analytic-last')
    else:
        cdc=events()
        query=Path(__file__).with_name('native-grouped-selectors.sql').read_text()
        checkpoint=CHECKPOINT
        keys=['canonical_id']
        if mode=='append':
            source=[event['after'] for event in cdc if event['op']=='c']
            # Append-only source keeps the very same native grouped SQL.
            query=query.replace('row_id BIGINT PRIMARY KEY','row_id BIGINT')
            query=query.replace("format='debezium_json',type='source'","format='json',type='source'")
            append_events=[dict(before=None,after=row,op='c') for row in source]
            states=prefix_states(append_events)
        else:
            source=cdc
            states=prefix_states(source)
        checkpoint_state=states[checkpoint-1]
    directory.mkdir(parents=True,exist_ok=False)
    (directory/'input.jsonl').write_text(''.join(json.dumps(e)+'\n' for e in source))
    (directory/'query.sql').write_text(query)
    expected = dict(key_fields=keys,strict_prefix_rows=True,prefix_states=states,
                    checkpoint=checkpoint_state,final=states[-1])
    (directory/'expected.json').write_text(json.dumps(expected,indent=2)+'\n')
    manifest = dict(query='query.sql',input='input.jsonl',expected='expected.json',
                    checkpoint_input_rows=checkpoint,max_capture_rows=256,timeout_seconds=120,
                    require_matching_cdc=True)
    (directory/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    return dict(status='prepared; runtime unverified',mode=mode,source_events=len(source),
                checkpoint=expected['checkpoint'],final=expected['final'])


if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory',type=Path)
    parser.add_argument('--mode',choices=('cdc','append','analytic-first','analytic-last'),default='cdc')
    args=parser.parse_args()
    print(json.dumps(prepare(args.directory.resolve(),args.mode),indent=2))
