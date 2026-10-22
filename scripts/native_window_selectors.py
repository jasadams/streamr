#!/usr/bin/env python3
"""Prepare finite append-only window selector SQL and independent source oracle.

The comparator is shared with the ordinary grouped fixture. Window membership
and sessions are calculated from accepted input rows, never operator outputs.
Arrival identity is assigned before event-time ordering or window expansion.
"""
import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

from native_grouped_selectors import fixed_outputs, preserved_aggregates, select

BASE=datetime(2026,10,18)
BASE_EPOCH=int(BASE.replace(tzinfo=timezone.utc).timestamp())
CHECKPOINT=19
WINDOWS=dict(tumble="TUMBLE(INTERVAL '20' SECOND)",
             hop="HOP(INTERVAL '10' SECOND,INTERVAL '20' SECOND)",
             session="SESSION(INTERVAL '10' SECOND)")


def second(row):
    return int((datetime.fromisoformat(row['event_time'])-BASE).total_seconds())


def events():
    source=[]
    def write(key,time,value,priority=0):
        source.append(dict(row_id=len(source)+1,canonical_id=key,
            event_time=(BASE+timedelta(seconds=time)).isoformat(),value=value,priority=priority))
    write('seed',1,'seed')
    write('control',90,'advance')
    write('raw',128,'A')
    write('raw',120,'B',999)
    write('ties',120,'A')
    write('ties',120,'B',1)
    write('ordinal',120,'A')
    write('ordinal',120,'B')
    write('nullable',120,'A')
    write('nullable',121,None)
    write('panes',122,'early',999)
    write('panes',130,'A')
    write('merge',120,'A')
    write('merge',136,'B')
    write('merge',128,'bridge')
    write('reorder',128,'A')
    write('reorder',120,'earlier',999)
    write('reorder',128,'B')
    write('blank',121,'',None)
    # Original ordinals must survive both checkpoint and sorted retained readers.
    write('ties',120,'C',1)
    write('ordinal',120,'C')
    write('nullable',122,None)
    write('panes',130,'B')
    write('merge',136,'C')
    write('reorder',128,'C')
    write('control',300,'close')
    return source


def members(source,kind):
    """Return (key,start,end)->[(original row, accepted arrival ordinal)]."""
    groups={}
    for ordinal,row in enumerate(source,1):
        if row['canonical_id']!='control':
            groups.setdefault(row['canonical_id'],[]).append((row,ordinal))
    result={}
    for key,rows in groups.items():
        if kind=='session':
            components=[]
            for member in sorted(rows,key=lambda member:(second(member[0]),member[1])):
                time=second(member[0])
                if components and time<=components[-1][1]+10:
                    components[-1][1]=time
                    components[-1][2].append(member)
                else:
                    components.append([time,time,[member]])
            for start,end,component in components:
                result[key,start,end+10]=component
        else:
            for row,ordinal in rows:
                time=second(row)
                starts=[(time//20)*20] if kind=='tumble' else [(time//10)*10-10,(time//10)*10]
                for start in starts:
                    result.setdefault((key,start,start+20),[]).append((row,ordinal))
    return result


def output(key,start,end,rows):
    value=lambda row:row['value']
    priority=lambda row:row['priority']
    pick=lambda expression,**kwargs:select(rows,expression,**kwargs)
    return dict(**fixed_outputs(rows),**preserved_aggregates(rows),canonical_id=key,start_epoch=BASE_EPOCH+start,end_epoch=BASE_EPOCH+end,n=len(rows),
        earliest_value=pick(value,last=False),latest_value=pick(value,secondary=priority),
        descending_value=pick(value,secondary=priority,descending=True,nulls_first=True),
        ordinal_first=pick(value,last=False,secondary=priority),ordinal_last=pick(value),
        latest_nonblank=pick(lambda row:row['value'] if row['value']!='' else None,ignore_nulls=True))


def states(source,kind):
    # Watermark lags maximum accepted source time by 60s. EOF closes remaining
    # windows. Control input advances progress but is removed by ordinary WHERE.
    snapshots=[]
    for size in range(1,len(source)+1):
        prefix=source[:size]
        watermark=max(second(row) for row in prefix)-60
        rows=[output(key,start,end,members_) for (key,start,end),members_ in members(prefix,kind).items()
              if end<watermark or size==len(source)]
        snapshots.append(sorted(rows,key=lambda row:(row['canonical_id'],row['start_epoch'],row['end_epoch'])))
    return snapshots


def prepare(directory,kind):
    source=events()
    snapshots=states(source,kind)
    directory.mkdir(parents=True,exist_ok=False)
    (directory/'input.jsonl').write_text(''.join(json.dumps(row)+'\n' for row in source))
    query=Path(__file__).with_name('native-window-selectors.sql').read_text().replace('{{WINDOW}}',WINDOWS[kind])
    (directory/'query.sql').write_text(query)
    expected=dict(key_fields=['canonical_id','start_epoch','end_epoch'],row_format='append_json',
        strict_prefix_rows=True,prefix_states=snapshots,checkpoint=snapshots[CHECKPOINT-1],final=snapshots[-1])
    (directory/'expected.json').write_text(json.dumps(expected,indent=2)+'\n')
    manifest=dict(query='query.sql',input='input.jsonl',expected='expected.json',
        checkpoint_input_rows=CHECKPOINT,max_capture_rows=256,timeout_seconds=120,
        require_matching_cdc=True,native_windows=True)
    (directory/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    return dict(status='prepared; runtime unverified',kind=kind,source_rows=len(source),
        checkpoint_input_rows=CHECKPOINT,checkpoint_rows=len(expected['checkpoint']),final_rows=len(expected['final']))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory',type=Path)
    parser.add_argument('--kind',choices=tuple(WINDOWS),required=True)
    args=parser.parse_args()
    print(json.dumps(prepare(args.directory.resolve(),args.kind),indent=2))
