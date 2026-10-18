#!/usr/bin/env python3
"""Prepare finite generic native MERGE/lookup Kafka fixtures and a full-row oracle.

This exercises native state-table insert/update/delete/reinsert and lookup, not
Debezium RowKind aggregate retractions. No runtime or build is launched.
"""
import argparse
import hashlib
import json
from pathlib import Path


def prepare_cdc(root, baseline):
    def write(name,value):
        (root/name).write_text(json.dumps(value,indent=2,sort_keys=True)+'\n')
    rows={}
    phases=[]
    inputs=[]
    operations=[('c',1,'a',5),('c',2,'a',7),('u',1,'a',9),('d',2,'a',None),
                ('c',3,'b',3),('c',4,'b',11),('d',3,'b',None),('c',2,'a',6)]
    for index,(op,row_id,key,amount) in enumerate(operations):
        before=rows.get(row_id)
        after=None if op=='d' else dict(row_id=row_id,k=key,amount=amount,payload='a'*16384)
        inputs.append(dict(before=before,after=after,op=op,ts_ms=1000*(index+1)))
        if after is None:del rows[row_id]
        else:rows[row_id]=after
        state={}
        for group in sorted({row['k'] for row in rows.values()}):
            members=[row for row in rows.values() if row['k']==group]
            state[group]=dict(k=group,n=len(members),total=sum(row['amount'] for row in members),payload='a'*262144)
        phases.append(state)
    query="""SET updating_ttl = NULL;
CREATE TABLE cdc_input (row_id BIGINT PRIMARY KEY, k TEXT NOT NULL, amount BIGINT NOT NULL, payload TEXT NOT NULL)
WITH (connector='kafka',bootstrap_servers='${broker}',topic='${namespace}-input',format='debezium_json',type='source','source.offset'='earliest','source.group_id'='${namespace}','source.read_mode'='read_committed');
CREATE VIEW totals AS SELECT k,COUNT(*) AS n,SUM(amount) AS total,repeat(MAX(payload),16) AS payload FROM cdc_input GROUP BY k;
"""
    for index in range(2):
        query+=f"CREATE TABLE cdc_result_{index}(k TEXT,n BIGINT,total BIGINT,payload TEXT) WITH(connector='kafka',bootstrap_servers='${{broker}}',topic='${{namespace}}-output-{index}',format='debezium_json',type='sink','sink.commit_mode'='exactly_once','sink.recovery_topic'='${{namespace}}-commits-{index}',client_configs='transaction.timeout.ms=10000,queue.buffering.max.kbytes=8192');\n"
    for index in range(2):query+=f'INSERT INTO cdc_result_{index} SELECT k,n,total,payload FROM totals;\n'
    config=json.loads((baseline/'config.json').read_text())
    original_key_limit=config['worker']['aggregate-state']['key-bytes']
    # Arrow 55.2.0 variable-row encoding: one-character group=10 bytes;
    # 16KiB MAX argument=4+ceil(16384/32)*33=16900 bytes. Native M and R
    # keys contain group, generation, aggregate, argument and ordinal.
    argument_row_bytes=4+((16384+31)//32)*33
    group_row_bytes=1+9
    primary_key_bytes=1+4+group_row_bytes+8+4+argument_row_bytes+8
    secondary_key_bytes=primary_key_bytes+4
    assert secondary_key_bytes==16939
    config['worker']['aggregate-state']['key-bytes']=secondary_key_bytes
    write('config.json',config)
    write('inputs.json',inputs)
    write('expected.json',dict(phases=phases))
    (root/'query.sql').write_text(query)
    write('manifest.json',dict(kind='cdc-phases',coverage=['updates','rowkind-retractions','count','sum','amplified-output'],
                              input_records=len(inputs),scheduling='one logical Debezium event per public checkpoint-stop/resume; no source pacing or sleeps',
                              rowkind_cdc_retractions=True,lookup_coverage='separate MERGE fixture',
                              configuration_delta={'worker.aggregate-state.key-bytes':{'before':original_key_limit,'after':secondary_key_bytes}},
                              key_bound_evidence={'group_utf8_bytes':1,'group_row_bytes':group_row_bytes,'max_argument_utf8_bytes':16384,'argument_row_bytes':argument_row_bytes,'native_M_key_bytes':primary_key_bytes,'native_R_key_bytes':secondary_key_bytes,
                                                  'reason':'native MAX(payload) on changelog retains ordered M and reverse R member keys; preserve MAX and all COUNT/SUM/retraction semantics, no guard changes',
                                                  'arrow_source':'https://github.com/apache/arrow-rs/blob/55.2.0/arrow-row/src/variable.rs#L50',
                                                  'native_sources':['crates/arroyo-worker/src/arrow/incremental_aggregator.rs:native_member_keys/native_member_prefix/native_tuple_prefix','crates/arroyo-worker/src/arrow/aggregate_store.rs:AggregateScope::check_key']},
                              sha256={name:hashlib.sha256((root/name).read_bytes()).hexdigest() for name in ['config.json','query.sql','inputs.json','expected.json']}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--kind', choices=['merge','cdc'],default='merge')
    args = parser.parse_args()
    root = args.directory.resolve()
    root.mkdir(parents=True, exist_ok=False)
    if args.kind=='cdc':
        prepare_cdc(root,args.baseline)
        print(root)
        return

    def write(name, value):
        (root / name).write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')

    inputs = []
    expected = []
    quantities = {}
    mirrored = {}
    for seq in range(65):
        key = seq % 4
        mode = 'delete' if seq // 4 % 3 == 2 else 'write'
        delta = seq % 7 + 1
        event = dict(seq=seq, k=key, mode=mode, delta=delta, payload='a' * 16384)
        inputs.append(event)
        old = quantities.get(key)
        mirror_old = mirrored.get(key)
        if mode == 'delete':
            quantities.pop(key, None)
            mirrored.pop(key, None)
            action = 'delete' if old is not None else 'none'
        else:
            quantities[key] = (old or 0) + delta
            mirrored[key] = quantities[key]
            action = 'insert' if old is None else 'update'
        expected.append(dict(seq=seq, k=key, old_value=old,
                             new_value=quantities.get(key), action=action,
                             mirror_old=mirror_old, mirror_new=mirrored.get(key),
                             mirror_action=action, lookup_quantity=quantities.get(key),
                             payload=event['payload'] * 16))
    config = json.loads((args.baseline / 'config.json').read_text())
    typed=config['worker']['typed-sql-state']
    auxiliary_limits={'max-working-event-bytes':2*1048576,'max-captured-event-bytes':1048576,'max-pending-output-bytes':2*1048576}
    configuration_delta={'worker.typed-sql-state.'+name:{'before':typed[name],'after':value} for name,value in auxiliary_limits.items()}
    typed.update(auxiliary_limits)
    write('config.json', config)
    write('inputs.json', inputs)
    write('expected.json', expected)
    query = """CREATE TABLE mutation_input (seq BIGINT NOT NULL, k BIGINT NOT NULL, mode TEXT NOT NULL, delta BIGINT NOT NULL, payload TEXT NOT NULL)
WITH (connector='kafka', bootstrap_servers='${broker}', topic='${namespace}-input', format='json', type='source', 'source.offset'='earliest', 'source.group_id'='${namespace}', 'source.read_mode'='read_committed');
CREATE STATE TABLE quantities (k BIGINT PRIMARY KEY, quantity BIGINT) PARTITION BY k;
CREATE STATE TABLE mirrored (k BIGINT PRIMARY KEY, quantity BIGINT) PARTITION BY k;
CREATE VIEW applied AS MERGE INTO quantities AS target USING mutation_input AS source ON target.k=source.k
WHEN MATCHED AND source.mode='delete' THEN DELETE
WHEN MATCHED AND source.mode='write' THEN UPDATE SET quantity=target.quantity+source.delta
WHEN NOT MATCHED AND source.mode='write' THEN INSERT (k, quantity) VALUES (source.k, source.delta)
RETURNING source AS source, old AS old, new AS new, action AS action;
CREATE VIEW changes AS SELECT source.seq AS seq, source.k AS k, source.payload AS payload,
old.quantity AS old_value, new.quantity AS new_value, action AS action FROM applied;
CREATE VIEW mirrored_changes AS MERGE INTO mirrored AS target USING changes AS source ON target.k=source.k
WHEN MATCHED AND source.action='delete' THEN DELETE
WHEN MATCHED AND source.action IN ('insert','update') THEN UPDATE SET quantity=source.new_value
WHEN NOT MATCHED AND source.action IN ('insert','update') THEN INSERT (k, quantity) VALUES (source.k, source.new_value)
RETURNING source AS source, old AS old, new AS new, action AS action;
CREATE VIEW checked AS SELECT r.source.seq AS seq, r.source.k AS k,
r.source.old_value AS old_value, r.source.new_value AS new_value, r.source.action AS action,
r.old.quantity AS mirror_old, r.new.quantity AS mirror_new, r.action AS mirror_action,
q.quantity AS lookup_quantity, repeat(r.source.payload,16) AS payload
FROM mirrored_changes r LEFT JOIN quantities q ON q.k=r.source.k;
"""
    query=query.replace('repeat(r.source.payload,16)','concat('+','.join(['r.source.payload']*16)+')')
    schema = 'seq BIGINT, k BIGINT, old_value BIGINT, new_value BIGINT, action TEXT, mirror_old BIGINT, mirror_new BIGINT, mirror_action TEXT, lookup_quantity BIGINT, payload TEXT'
    for index in range(2):
        query += f"CREATE TABLE result_{index} ({schema}) WITH (connector='kafka', bootstrap_servers='${{broker}}', topic='${{namespace}}-output-{index}', format='json', type='sink', 'sink.commit_mode'='exactly_once', 'sink.recovery_topic'='${{namespace}}-commits-{index}', client_configs='transaction.timeout.ms=10000,queue.buffering.max.kbytes=8192');\n"
    for index in range(2):
        query += f'INSERT INTO result_{index} SELECT seq,k,old_value,new_value,action,mirror_old,mirror_new,mirror_action,lookup_quantity,payload FROM checked;\n'
    (root / 'query.sql').write_text(query)
    write('manifest.json', dict(coverage=['updates', 'state-deletions', 'lookups', 'amplified-output'],
                               input_records=65, prefix_input_count=1, prefix_output_count=1,
                               fault_input_count=1, expected_output_records=65,
                               rowkind_cdc_retractions=False,
                               configuration_delta=configuration_delta,
                               bounds_evidence={'source_payload_bytes':16384,'expanded_payload_bytes_per_exit':262144,'captured_exit_count':2,'captured_payload_bytes':524288,
                                                'captured_limit_rationale':'1MiB bounds two 256KiB payloads plus numeric/text fields, Arrow buffers/objects and the capture envelope; guards remain mandatory',
                                                'working_limit_rationale':'2MiB covers the source row, retained source/old/new and dependent-MERGE/lookup/projection outputs, the 256KiB CONCAT backing allocation, and the expanded projection plus two sink projections. The owner cumulatively charges every retained step; this is a finite configured allowance, not a measured exact minimum.',
                                                'pending_limit_rationale':'2MiB admits a complete captured envelope under the existing 1MiB per-event bound plus pending-envelope overhead; source batch remains one',
                                                'runtime_lowered_allocation_verified':False,
                                                'unchanged_limits':['live-state-resources.decoded-value-bytes=16777216','typed-sql-state.max-resident-bytes=8388608','engine container=2GiB','Kafka producer queue.buffering.max.kbytes=8192'],
                                                'native_sources':['crates/arroyo-planner/src/state_table_fusion.rs:BoundedScalarAdmission stock UTF8 CONCAT','crates/arroyo-worker/src/arrow/state_table_concat.rs:backing_charge/finish_charge','crates/arroyo-worker/src/arrow/state_table_owner.rs:execute_event cumulative working/capture checks','crates/arroyo-worker/src/arrow/state_table_runtime.rs:one_event_headroom/PendingEnvelopes']},
                               sql_delta='existing guarded stock CONCAT with sixteen identical NOT NULL source payload arguments replaces unqualified fused repeat; exact input/oracle unchanged; frozen baseline repeat remains unchanged',
                               oracle='deterministic full-row state-table mutation model, ordered source partition',
                               sha256={name: hashlib.sha256((root / name).read_bytes()).hexdigest()
                                       for name in ['config.json', 'query.sql', 'inputs.json', 'expected.json']}))
    print(root)


if __name__ == '__main__':
    main()
