#!/usr/bin/env python3
"""One isolated single-sink operation qualification; not a production command.

run receives the existing supervisor's owned case/process/API/broker/oracle
functions. External reviewed helpers alone resolve Kafka and mutate catalog.
"""
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import time

KAFKA_SHA = 'f0beda9ea90c9adbba527f4a6582f1a79df88345ee78fcbf546e0c3db2e857cb'
CATALOG_SHA = '7ca9063352adc89207ee3b652cb6d6e156c8ad4ba90dd00678fb08b9c4ea888d'
CLOSURE_SHA = 'ccab4fc4c06d88948ca785820f84724aacdcb586093cb62cad02f729dbf4be87'
PATCH_SHA = '634ec71e26c54e3b2be7421593c8097e79a09cfe6d2815bae6eba987a81ed330'


def run(c):
    a,h,case=c.a,c.h,c.case
    assert a.scenario=='one-time-repair' and len(c.outputs)==len(c.commits)==1
    assert a.broker=='broker:9092' and case.parent==a.operation_root.resolve()
    helpers=a.operation_root/'operational-helper'
    for name,expected in [('kafka_helper.py',KAFKA_SHA),('catalog_helper.py',CATALOG_SHA),('checkpoint_closure.py',CLOSURE_SHA)]:
        assert h.sha(helpers/name)==expected, 'external helper differs from reviewed source'
    assert h.sha(a.operation_root/'prevention/prepare_timeout_patch.py')==PATCH_SHA
    c.write('one-time-source.json',dict(module_sha256=h.sha(Path(__file__)),helpers={name:h.sha(helpers/name) for name in ['kafka_helper.py','catalog_helper.py','checkpoint_closure.py']},production_operation=False))
    sys.path.insert(0,str(helpers))
    import catalog_helper as catalog
    import checkpoint_closure as closure
    patch_spec=importlib.util.spec_from_file_location('reviewed_patch',a.operation_root/'prevention/prepare_timeout_patch.py')
    patch=importlib.util.module_from_spec(patch_spec);patch_spec.loader.exec_module(patch)
    operation_python=str(a.operation_python.resolve()) if a.operation_python else sys.executable
    if a.operation_python:
        if getattr(a,'immutable_image_manifest',None):
            expected_python=Path(c.immutable['python']['path'])
        else:expected_python=a.operation_root/'production-runtime-312/interpreter/python/bin/python3.12'
        assert a.operation_python.resolve()==expected_python.resolve()
        assert h.sha(expected_python)=='147ab3b44ef31b301e3fe39e12dd3502b462977371ad7cece59c703f8f7d0f0e'
    database=case/'catalog.sqlite'
    storage=case/'checkpoints'
    job=c.original_job

    def wait(fn,label,seconds=180):
        end=time.monotonic()+seconds
        while time.monotonic()<end:
            assert c.child.poll() is None, 'cluster exited during '+label
            processes=h.session_processes(c.child.pid)
            c.bind_workers(processes,label)
            c.owned.update({h.identity(p):p for p in processes})
            c.result['max_sampled_owned_rss_bytes']=max(c.result.get('max_sampled_owned_rss_bytes',0),sum(p['rss'] for p in processes))
            with (case/'one-time-process-samples.jsonl').open('a') as stream:stream.write(json.dumps(dict(time_ns=time.time_ns(),processes=processes))+'\n')
            value=fn()
            if value:return value
            time.sleep(.1)
        raise AssertionError('timeout '+label)

    def log_since(start=0):
        with (case/'runtime.log').open('rb') as stream:
            stream.seek(start)
            return re.sub(chr(27)+r'\[[0-?]*[ -/]*[@-~]','',stream.read().decode(errors='replace'))

    def exact_resumes(text,transaction,epoch,transactional_id):
        return [line for line in text.splitlines() if 'Kafka compact transaction resume' in line and job in line and sink in line and transactional_id in line and all(re.search(rf'\b{name}={value}\b',line) for name,value in [('epoch',epoch),('transaction_index',transaction['transaction_index']),('producer_id',transaction['producer_id']),('producer_epoch',transaction['producer_epoch'])])]

    def read_rows(P_id=None,C_id=None):
        with sqlite3.connect('file:'+str(database)+'?mode=ro',uri=True) as db:
            db.row_factory=sqlite3.Row
            jc=dict(db.execute('SELECT * FROM job_configs WHERE id=?',(job,)).fetchone())
            js=dict(db.execute('SELECT * FROM job_statuses WHERE id=?',(job,)).fetchone())
            pl=dict(db.execute('SELECT * FROM pipelines WHERE id=?',(jc['pipeline_id'],)).fetchone())
            rows=dict(job_config=jc,job_status=js,pipeline=pl)
            if P_id:rows['P']=dict(db.execute('SELECT * FROM checkpoints WHERE pub_id=?',(P_id,)).fetchone())
            if C_id:rows['C']=dict(db.execute('SELECT * FROM checkpoints WHERE pub_id=?',(C_id,)).fetchone())
            return rows

    def checkpoints():
        with sqlite3.connect('file:'+str(database)+'?mode=ro',uri=True) as db:
            db.row_factory=sqlite3.Row
            return [dict(row) for row in db.execute('SELECT * FROM checkpoints WHERE job_id=? ORDER BY id',(job,))]

    def root(epoch):return storage/job/'checkpoints'/f'checkpoint-{epoch:07}'
    def pending(epoch,operator):
        op=h.wire((root(epoch)/('operator-'+operator)/'metadata').read_bytes())
        table=h.wire(h.pairs(op,13)[b'i']);payload=h.wire(h.one(table,2,b''))
        commits=h.pairs(payload,2,numeric=True)
        assert set(commits)=={0}
        return json.loads(commits[0])

    def source_next(epoch,operator):
        import pyarrow.parquet as parquet
        op=h.wire((root(epoch)/('operator-'+operator)/'metadata').read_bytes())
        table=h.wire(h.pairs(op,13)[b'k']);files=h.wire(h.one(table,2,b''))[1]
        assert len(files)==1
        rows=parquet.read_table(storage/files[0].decode(),columns=['key','value']).to_pylist()
        assert len(rows)==1
        partition,pos=h.bincode_uint(rows[0]['value']);offset,end=h.bincode_uint(rows[0]['value'],pos)
        assert partition==0 and end==len(rows[0]['value'])
        return (offset>>1)^-(offset&1)

    def full_output(label):
        raw=c.committed_raw(c.outputs[0],label+'-full',0,'%p\t%o\t%k{hex}\t%d\t%v\n')
        (case/(label+'-full-output.tsv')).write_text(raw)
        return raw

    def helper_call(label,argv,timeout):
        """Foreground child with a whole-process cap and descendant teardown."""
        process=subprocess.Popen(argv,stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
        owned={h.identity(p):p for p in h.session_processes(process.pid)}
        timed_out=False
        try:
            stdout,stderr=process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out=True
            owned.update({h.identity(p):p for p in h.session_processes(process.pid)})
            process.kill();stdout,stderr=process.communicate(timeout=5)
        finally:
            cleanup=h.cleanup(process,owned)
        (case/(label+'.stdout')).write_bytes(stdout)
        (case/(label+'.stderr')).write_bytes(stderr)
        c.write(label+'.json',dict(command=argv,timeout_seconds=timeout,timed_out=timed_out,exit=process.returncode,cleanup=cleanup))
        assert not timed_out and cleanup['complete'] and not cleanup['errors'], 'ambiguous helper outcome; no catalog/restart permitted'
        assert process.returncode==0, 'external helper refused; inspect retained receipt'
        return json.loads(stdout) if stdout.strip() else None

    def stop_cluster(label):
        cleanup=h.cleanup(c.child,c.owned)
        c.write(label+'.json',dict(pid=c.child.pid,cleanup=cleanup))
        assert cleanup['complete'] and not cleanup['errors']
    def start_cluster(label,expect_failed=True):
        c.child=subprocess.Popen([str(a.binary),'--config',str(case/'config.json'),'cluster'],stdout=c.log,stderr=subprocess.STDOUT,start_new_session=True,env=c.env,cwd=case)
        def ready():
            try:return c.api('pipelines')
            except Exception:return None
        wait(ready,label+' API')
        if expect_failed:
            wait(lambda:c.job()['state']=='Failed',label+' unchanged durable Failed state')
            assert not any(h.worker(p,a.binary) for p in h.session_processes(c.child.pid)), 'Failed job unexpectedly scheduled a worker'
    probe="""import sys,json,hashlib,importlib,sqlite3;from pathlib import Path
assert sys.implementation.name=='cpython' and sys.version_info[:2] in [(3,11),(3,12)]
if sys.version_info[:2]==(3,12):assert sys.version_info[:3]==(3,12,5) and sqlite3.sqlite_version=='3.46.0'
sys.path.insert(0,sys.argv[1]);import kafka_helper
client,receipt=kafka_helper.load_client();native=importlib.import_module('confluent_kafka.cimpl')
paths=sorted({line.split()[-1] for line in Path('/proc/self/maps').read_text().splitlines() if 'librdkafka' in line and line.split()[-1].startswith('/')})
dependency_root,abi,wheel_sha=kafka_helper.dependency_selection()
runtime=(dependency_root/'python-runtime').resolve()
assert paths and all(Path(p).resolve().is_relative_to(runtime) for p in paths)
receipt.update(python=sys.executable,python_version=sys.version,sqlite_version=sqlite3.sqlite_version,cimpl_path=native.__file__,cimpl_sha256=hashlib.sha256(Path(native.__file__).read_bytes()).hexdigest(),native_libraries={p:hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in paths})
print(json.dumps(receipt))
"""
    operation_runtime=helper_call('pinned-client-import',[operation_python,'-c',probe,str(helpers)],45)
    assert operation_runtime['python_abi']==('cp312' if a.operation_python else 'cp311')
    c.write('operation-runtime.json',dict(supervisor_python=sys.executable,operation_python=operation_python,operation_binary_sha256=h.sha(Path(operation_python)),receipt=operation_runtime))
    P=next(row for row in reversed(checkpoints()) if row['state']=='ready')
    p_epoch=P['epoch'];c_epoch=p_epoch+1
    global_metadata=h.wire((root(p_epoch)/'metadata').read_bytes())
    sink_ids=[op.decode() for op in global_metadata[6] if op.decode().startswith('sink_')]
    sources=[op.decode() for op in global_metadata[6] if op.decode().startswith('source_')]
    assert len(sink_ids)==len(sources)==1
    sink=sink_ids[0];P_pending=pending(p_epoch,sink)
    assert source_next(p_epoch,sources[0])==1
    prefix_full=full_output('prefix')
    def markers(label,epoch,transaction):
        raw=c.committed_raw(c.commits[0],label+'-markers',0,'%p\t%o\t%x\t%y\t%k\t%v\n')
        (case/(label+'-markers.tsv')).write_text(raw)
        records=[row.split('\t',5) for row in raw.splitlines()]
        key=f'streamr-v4/{job}/{sink}/{c.outputs[0]}/0'
        assert records and all(row[4]==key for row in records)
        matches=[row for row in records if json.loads(row[5])==dict(version=4,generation=transaction['generation'],epoch=epoch)]
        assert len(matches)==1 and int(matches[0][2])==transaction['producer_id'] and int(matches[0][3])==transaction['producer_epoch']
        c.write(label+'-marker-identity.json',dict(epoch=epoch,pending=transaction,records=records))
        return raw
    prefix_markers=markers('prefix',p_epoch,P_pending)
    gate=c.fault_dir/('before-'+job);gate.touch()
    start_log=(case/'runtime.log').stat().st_size
    c.api(f'pipelines/{c.pid}',{'stop':'none'},method='PATCH')
    def prepared_C():
        rows=[row for row in checkpoints() if row['epoch']==c_epoch and row['state']=='committing']
        text=log_since(start_log)
        return rows[0] if len(rows)==1 and any('Kafka commit fault pause' in line and job in line and re.search(rf'\bepoch={c_epoch}\b',line) for line in text.splitlines()) else None
    C=wait(prepared_C,'durable committing C before original EndTxn')
    original_committing_pause_observed_ns=time.time_ns()
    C_pending=pending(c_epoch,sink)
    assert C_pending['generation']==P_pending['generation'] and C_pending['transaction_index']==P_pending['transaction_index']+1
    assert source_next(c_epoch,sources[0])==2
    transaction_ids=[f'streamr-v4-id-{job}-{sink}-{c.outputs[0]}-0-{slot}' for slot in [0,1]]
    selected_id=transaction_ids[C_pending['transaction_index']%2]
    describe_observations={}
    def describe(label):
        request_start_ns=time.time_ns()
        raw=c.broker('cluster','txn','describe',*transaction_ids,'--format=json',command_timeout=10)
        describe_observations[label]=dict(request_start_ns=request_start_ns,response_observed_ns=time.time_ns())
        (case/(label+'.json.stdout')).write_text(raw)
        c.write(label+'-observation.json',describe_observations[label])
        return json.loads(raw)
    original_workers=[h.worker(p,a.binary) for p in h.session_processes(c.child.pid)]
    original_workers=[worker for worker in original_workers if worker and worker['job']==job]
    assert len(original_workers)==1
    original_owner=original_workers[0]
    c.write('operation-original-owner.json',original_owner)
    def owner_alive():
        return any(h.identity(process)==h.identity(original_owner) for process in h.session_processes(c.child.pid))
    ongoing=describe('operation-original-ongoing')
    original_ongoing_observed_ns=time.time_ns()
    selected=[row for row in ongoing if row['transaction_id']==selected_id]
    assert selected and all(row['producer_id']==C_pending['producer_id'] and row['producer_epoch']==C_pending['producer_epoch'] and row['state']=='Ongoing' and row['timeout']==60000 for row in selected)
    original_last_updates={int(row['start_timestamp']) for row in selected};assert len(original_last_updates)==1
    timing=dict(original_broker_last_update_ms=original_last_updates.pop(),original_committing_pause_observed_ns=original_committing_pause_observed_ns,original_ongoing_observed_ns=original_ongoing_observed_ns,last_original_ongoing_request_start_ns=describe_observations['operation-original-ongoing']['request_start_ns'])
    raw=c.broker('topic','consume',c.outputs[0],'--read-committed=false','--offset',':end','--format','%p\t%o\t%x\t%y\t%k{hex}\t%d\t%v\n')
    (case/'original-C-uncommitted.tsv').write_text(raw)
    records=[row.split('\t',6) for row in raw.splitlines()]
    values=[json.loads(row[6]) for row in records];c.validate_uuid_values(values)
    assert len(values)==2 and int(records[-1][2])==C_pending['producer_id'] and int(records[-1][3])==C_pending['producer_epoch']
    abandoned_binding=values[-1]['new_binding']
    if a.operation_cut=='exact-c':
        after_gate=c.fault_dir/('after-'+job);after_gate.touch()
        assert owner_alive()
        gate.unlink()
        wait(lambda:any('Kafka commit fault pause' in line and job in line and 'point="after"' in line and re.search(rf'\bepoch={c_epoch}\b',line) for line in log_since(start_log).splitlines()),'original C committed before acknowledgement')
        exact_C_markers=markers('exact-C-before-stop',c_epoch,C_pending)
        exact_C_output=full_output('exact-C-before-stop')
        assert len(exact_C_output.splitlines())==2 and owner_alive()
        stop_cluster('exact-C-quiescent-native-stop')
        after_gate.unlink()
    else:
        probe=0
        def aborted():
            nonlocal probe
            assert owner_alive(), 'original prepared owner exited before expiry/release'
            assert 'Kafka compact transaction resume' not in log_since(start_log), 'replacement resumed before original release'
            label=f'operation-expiry-{probe:04}'
            rows=describe(label);probe+=1
            selected=[row for row in rows if row['transaction_id']==selected_id]
            if selected and all(row['producer_id']==C_pending['producer_id'] and row['producer_epoch']==C_pending['producer_epoch'] and row['state']=='Ongoing' for row in selected):timing['last_original_ongoing_request_start_ns']=describe_observations[label]['request_start_ns']
            return selected if selected and all(row['producer_id']==C_pending['producer_id'] and row['producer_epoch']==C_pending['producer_epoch']+1 and row['state']=='CompleteAbort' for row in selected) else None
        expired=wait(aborted,'actual original selected transaction broker abort',90)
        timing['original_abort_observed_ns']=time.time_ns()
        c.write('operation-expired.json',dict(P=P,C=C,pending=C_pending,ongoing=ongoing,aborted=expired))
        assert owner_alive() and 'Kafka compact transaction resume' not in log_since(start_log)
        gate.unlink()
        original_error=wait(lambda:[line for line in log_since(start_log).splitlines() if 'Kafka transaction failed without acknowledgement:' in line and sink in line and 'fenced by a newer instance' in line and 'Kafka prepared transaction recovery failed:' not in line],'original EndTxn fencing before worker loss',15)
        timing['original_error_observed_ns']=time.time_ns()
        assert owner_alive(), 'original error owner exited before actual SIGKILL'
        assert 'Kafka compact transaction resume' not in log_since(start_log), 'replacement error cannot qualify original failure'
        assert h.terminate_identity(original_owner,signal.SIGKILL)
        timing['original_worker_SIGKILL_ns']=time.time_ns()
        restore_boundary=(case/'runtime.log').stat().st_size
        c.write('operation-original-loss.json',dict(time_ns=time.time_ns(),worker=original_owner,error=original_error,restore_log_boundary=restore_boundary))
        wait(lambda:c.job()['state']=='Failed','ordinary selected-C recovery exhausted with missing marker')
        text=log_since(restore_boundary)
        resumes=[line for line in text.splitlines() if 'Kafka compact transaction resume' in line and job in line and sink in line and selected_id in line and all(re.search(rf'\b{name}={value}\b',line) for name,value in [('epoch',c_epoch),('transaction_index',C_pending['transaction_index']),('producer_id',C_pending['producer_id']),('producer_epoch',C_pending['producer_epoch'])])]
        refusals=[line for line in text.splitlines() if 'Kafka prepared transaction recovery failed:' in line and job in line and sink in line and f'no exact committed marker (observed {p_epoch}, expected {c_epoch})' in line]
        assert resumes and refusals
        c.write('operation-selected-refusal.json',dict(pending=C_pending,resumes=resumes,refusals=refusals))
        assert full_output('failed')==prefix_full and markers('failed',p_epoch,P_pending)==prefix_markers

        stop_cluster('failed-controller-stop')
        start_cluster('fresh-failed-controller')
        stop_cluster('held-controller-stop')
    rows=read_rows(P['pub_id'],C['pub_id'])
    assert rows['job_config']['restart_nonce']==rows['job_status']['restart_nonce']
    if a.operation_cut!='exact-c':assert rows['job_status']['state']=='Failed'
    assert rows['P']['state']=='ready' and rows['C']['state']=='committing'
    with sqlite3.connect('file:'+str(database)+'?mode=ro',uri=True) as source,sqlite3.connect(case/'catalog-before-operation.sqlite') as backup:source.backup(backup)
    c.write('operation-original-rows.json',{name:catalog.snapshot(row) for name,row in rows.items()})
    program=rows['pipeline']['program']
    connectors=[]
    for field,kind,node,_,_ in patch.fields(program):
        if field!=1 or kind!=2:continue
        for field,kind,op,_,_ in patch.fields(node):
            if field==5 and kind==2 and patch.single(op,2)==b'ConnectorSink':
                cfg=patch.single(op,3)
                assert patch.single(cfg,1)==b'kafka'
                connectors.append((patch.single(op,1).decode(),json.loads(patch.single(cfg,2))))
    assert len(connectors)==1 and connectors[0][0]==sink
    cfg=connectors[0][1]
    assert cfg['connection']['bootstrapServers']==a.broker and cfg['table']['topic']==c.outputs[0]
    assert cfg['table']['type']==dict(commit_mode='exactly_once',recovery_topic=c.commits[0])
    assert cfg['table']['client_configs']['transaction.timeout.ms']=='60000'
    inventories={str(epoch):closure.inventory(storage,job,epoch) for epoch in [p_epoch,c_epoch]}
    verified={epoch:closure.verify(storage,inventory) for epoch,inventory in inventories.items()}
    archive=case/'immutable-checkpoint-archive';archive.mkdir()
    paths={row['path'] for inventory in inventories.values() for row in inventory}
    paths.update(str(path.relative_to(storage)) for path in root(c_epoch).rglob('*') if path.is_file())
    files=[]
    for relative in sorted(paths):
        source=storage/relative;target=archive/relative
        assert not source.is_symlink() and source.resolve().is_relative_to(storage.resolve())
        target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(source,target)
        assert target.stat().st_nlink==1 and source.stat().st_ino!=target.stat().st_ino and h.sha(source)==h.sha(target)
        files.append(dict(path=str(target.resolve()),size=target.stat().st_size,sha256=h.sha(target)))
    for inventory in inventories.values():closure.verify(archive,inventory)
    c.write('operation-closure.json',dict(inventories=inventories,verified=verified,copied_files=len(files),copied_bytes=sum(f['size'] for f in files)))
    archive_manifest=case/'archive-manifest.json'
    archive_manifest.write_text(json.dumps(dict(version=1,job=job,P_pub_id=P['pub_id'],C_pub_id=C['pub_id'],files=files),indent=2)+'\n')
    def descriptor(path):return dict(path=str(path.resolve()),size=path.stat().st_size,sha256=h.sha(path))
    def source_headroom(label):
        topics=json.loads(c.broker('topic','describe',c.src,c.outputs[0],c.commits[0],'--format=json'))
        for topic in topics:
            assert len(topic['partitions'])==1 and topic['partitions'][0]['partition']==0
            settings={entry['key']:entry['value'] for entry in topic['configs']}
            assert settings['cleanup.policy']==('compact' if topic['summary']['name']==c.commits[0] else 'delete')
            if topic['summary']['name']==c.src:
                part=topic['partitions'][0];assert part['log_start_offset']<=1<part['high_watermark'], 'native P source cursor is no longer retained'
        c.write(label+'.json',topics)
        raw=c.broker('topic','consume',c.src,'--read-committed','--offset','1','--num','1','--format','%p\t%o\t%k{hex}\t%d\t%v\n')
        (case/(label+'-source-record.tsv')).write_text(raw)
        assert json.loads(raw.strip().split('\t',4)[4])==c.inputs[1]
    if a.operation_cut=='source-retention-refused':
        before_refusal=read_rows(P['pub_id'],C['pub_id'])
        trim=c.broker('topic','trim-prefix',c.src,'--offset','2','--partitions','0','--no-confirm')
        (case/'source-retention-trim.stdout').write_text(trim)
        refusal=None
        try:source_headroom('operation-source-retention-refused')
        except AssertionError as error:
            assert str(error)=='native P source cursor is no longer retained', 'unexpected preflight refusal'
            refusal=str(error)
        assert refusal is not None, 'source retained frontier loss was not refused'
        topics=json.loads(c.broker('topic','describe',c.src,'--format=json'))
        assert len(topics)==1 and topics[0]['partitions'][0]['log_start_offset']>source_next(p_epoch,sources[0])
        after_refusal=read_rows(P['pub_id'],C['pub_id'])
        assert all(catalog.snapshot(after_refusal[name])==catalog.snapshot(before_refusal[name]) for name in before_refusal)
        assert full_output('retention-refused')==prefix_full and markers('retention-refused',p_epoch,P_pending)==prefix_markers
        assert not any((case/name).exists() for name in ['kafka-operation-manifest.json','kafka-operation.jsonl','catalog-operation-manifest.json','catalog-apply.json','operation-public-restart.json'])
        c.write('operation-source-retention-refusal.json',dict(source=c.src,P_next=source_next(p_epoch,sources[0]),topics=topics,refusal=refusal,catalog_unchanged=True,kafka_resolution_attempted=False,catalog_apply_attempted=False,restart_attempted=False))
        c.result.update(status='passed',scenario=a.scenario,operation_cut=a.operation_cut,job_id=job,pipeline_id=c.pid,committed_rows=[1],physical_prefix_preserved=True,scope='isolated actual selected-source low frontier beyond native P cursor refused before Kafka resolution/catalog mutation/restart; exact catalog/nonces/program/marker/output unchanged')
        return
    source_headroom('operation-source-before')
    marker_key=f'streamr-v4/{job}/{sink}/{c.outputs[0]}/0'
    kafka_manifest=case/'kafka-operation-manifest.json'
    kafka_manifest.write_text(json.dumps(dict(manifest_version=1,protocol_version=4,job=job,sink=sink,transaction_ids=transaction_ids,marker_topic=c.commits[0],marker_key=marker_key,generation=P_pending['generation'],P=p_epoch,C=c_epoch,broker_config={'bootstrap.servers':a.broker}),indent=2)+'\n')
    kafka_receipt=case/'kafka-operation.jsonl'
    helper_call('kafka-inspect',[operation_python,str(helpers/'kafka_helper.py'),'--manifest',str(kafka_manifest),'--casefile',str(case/'kafka-inspect.jsonl')],45)
    helper_call('kafka-resolve',[operation_python,str(helpers/'kafka_helper.py'),'--manifest',str(kafka_manifest),'--casefile',str(kafka_receipt),'--resolve-init-only','--expected-manifest-sha256',h.sha(kafka_manifest)],125)
    final_event=json.loads(kafka_receipt.read_text().splitlines()[-1])
    if a.operation_cut=='exact-c':
        assert final_event['outcome']=='exact_C_ordinary_selected_C_route' and not final_event['eligible_for_external_P_checks']
        unchanged=read_rows(P['pub_id'],C['pub_id'])
        assert all(catalog.snapshot(unchanged[name])==catalog.snapshot(rows[name]) for name in rows), 'exact-C helper changed catalog'
        assert markers('exact-C-after-helper',c_epoch,C_pending)==exact_C_markers and full_output('exact-C-after-helper')==exact_C_output
        restore_start=(case/'runtime.log').stat().st_size
        start_cluster('exact-C-native-restart',expect_failed=False)
        def acknowledged():
            current=read_rows(P['pub_id'],C['pub_id'])
            return current['C'] if current['C']['state']=='ready' else None
        ready_C=wait(acknowledged,'ordinary original C acknowledgement without reclassification')
        text=log_since(restore_start)
        assert any('restoring checkpoint' in line and job in line and re.search(rf'\bepoch={c_epoch}\b',line) for line in text.splitlines())
        assert exact_resumes(text,C_pending,c_epoch,selected_id)
        assert any('Kafka prepared transaction recovered' in line and job in line and sink in line for line in text.splitlines())
        assert full_output('exact-C-acknowledged')==exact_C_output and markers('exact-C-acknowledged',c_epoch,C_pending)==exact_C_markers
        workers=[h.worker(process,a.binary) for process in h.session_processes(c.child.pid)]
        workers=[worker for worker in workers if worker and worker['job']==job];assert len(workers)==1
        replacement=workers[0]
        c.broker('topic','produce',c.src,data=''.join(json.dumps(row)+'\n' for row in c.inputs[2:]))
        final=wait(lambda:c.snapshot('exact-C-final') if len(c.snapshot('exact-C-observed')[0][0])==4 else None,'exact C plus fresh native UUID/state outputs')
        assert full_output('exact-C-final').startswith(exact_C_output)
        assert any(h.identity(process)==h.identity(replacement) for process in h.session_processes(c.child.pid))
        assert read_rows(P['pub_id'],C['pub_id'])['job_config']['restart_nonce']==rows['job_config']['restart_nonce'], 'exact C required no public restart mutation'
        for inventory in inventories.values():closure.verify(archive,inventory)
        assert all(h.sha(Path(file['path']))==file['sha256'] for file in files)
        c.write('operation-exact-C-final.json',dict(original_C=ready_C,worker=replacement,committed_rows=final[0][0],catalog_reclassified=False,public_restart_requested=False,archive_reverified=True))
        c.result.update(status='passed',scenario=a.scenario,operation_cut=a.operation_cut,job_id=job,pipeline_id=c.pid,committed_rows=[4],physical_prefix_preserved=True,scope='isolated exact original C marker route; catalog unchanged by helper, ordinary C restore acknowledgement and fresh native UUID/state output; no P rollback or timeout patch')
        return
    assert final_event['outcome']=='exact_P_uncommitted_suffix' and final_event['eligible_for_external_P_checks']
    new_program=patch.patch(program,sink,'60000','900000')
    assert patch.patch(new_program,sink,'900000','60000')==program
    old_sql=rows['pipeline']['textual_repr'].encode();assert old_sql.count(b'transaction.timeout.ms=60000')==1
    new_sql=old_sql.replace(b'transaction.timeout.ms=60000',b'transaction.timeout.ms=900000')
    prepared_files={}
    for name,value in [('old_program',program),('new_program',new_program),('old_sql',old_sql),('new_sql',new_sql)]:
        path=case/(name+'.bin');path.write_bytes(value);prepared_files[name]=descriptor(path)
    manifest=dict(version=1,**{name:catalog.snapshot(row) for name,row in rows.items()},operator=sink,files=prepared_files,archive=descriptor(archive_manifest),kafka_manifest=descriptor(kafka_manifest),kafka_receipt=descriptor(kafka_receipt))
    catalog_manifest=case/'catalog-operation-manifest.json';catalog_manifest.write_text(json.dumps(manifest,indent=2)+'\n')
    dry=helper_call('catalog-readonly',[operation_python,str(helpers/'catalog_helper.py'),'--database',str(database),'--manifest',str(catalog_manifest)],45)
    assert dry['state']=='eligible_dry_run' and dry['writes']==0
    applied=helper_call('catalog-apply',[operation_python,str(helpers/'catalog_helper.py'),'--database',str(database),'--manifest',str(catalog_manifest),'--apply','--expected-manifest-sha256',h.sha(catalog_manifest)],45)
    assert applied['state']=='applied_verified' and applied['writes']==2
    after=read_rows(P['pub_id'],C['pub_id'])
    assert after['C']['state']=='failed' and after['pipeline']['program']==new_program and after['pipeline']['textual_repr'].encode()==new_sql
    assert catalog.snapshot(after['job_config'])==manifest['job_config'] and catalog.snapshot(after['job_status'])==manifest['job_status']
    start_cluster('post-operation-controller')
    if a.operation_cut=='after-cas':
        before_loss=read_rows(P['pub_id'],C['pub_id'])
        parent=h.process(c.child.pid);assert parent is not None
        assert h.terminate_identity(parent,signal.SIGKILL)
        c.write('operation-after-CAS-controller-loss.json',dict(time_ns=time.time_ns(),controller=parent,rows={name:catalog.snapshot(before_loss[name]) for name in ['job_config','job_status','C']}))
        stop_cluster('after-CAS-controller-descendants-stop')
        start_cluster('after-CAS-fresh-controller')
        after_loss=read_rows(P['pub_id'],C['pub_id'])
        assert all(catalog.snapshot(after_loss[name])==catalog.snapshot(before_loss[name]) for name in ['job_config','job_status','C'])
    recognized=helper_call('catalog-recognize',[operation_python,str(helpers/'catalog_helper.py'),'--database',str(database),'--manifest',str(catalog_manifest)],45)
    assert recognized['state']=='already_applied' and recognized['writes']==0
    source_headroom('operation-source-before-restart')
    assert full_output('before-restart')==prefix_full
    gate.touch()  # Observe replacement C's actual configured producer before EndTxn.
    if a.operation_cut=='replay-inprogress':
        completion_scope=f"completion-{job.encode().hex()}-{sources[0].encode().hex()}-0-{c_epoch}"
        (c.fault_dir/(completion_scope+'.request')).touch()
        completion_armed=time.monotonic()
    if a.operation_cut=='replay-committed':
        replay_after_gate=c.fault_dir/('after-'+job);replay_after_gate.touch()

    restore_start=(case/'runtime.log').stat().st_size
    response=c.api(f'pipelines/{c.pid}/restart',{'force':False,'ignore_state':False},method='POST')
    c.write('operation-public-restart.json',dict(time_ns=time.time_ns(),request={'force':False,'ignore_state':False},response=response))
    def restored_P():
        text=log_since(restore_start)
        lines=[line for line in text.splitlines() if 'restoring checkpoint' in line and job in line]
        assert not any(re.search(rf'\bepoch={c_epoch}\b',line) for line in lines), 'operation restored abandoned original C'
        return lines if any(re.search(rf'\bepoch={p_epoch}\b',line) for line in lines) else None
    c.write('operation-restore-P.json',wait(restored_P,'actual ordinary native P restore'))
    if a.operation_cut=='replay-inprogress':
        def incomplete_replacement():
            text=log_since(restore_start)
            entered=any('Checkpoint completion fault entered' in line and completion_scope in line for line in text.splitlines())
            prepared=any('Kafka compact transaction prepared' in line and sink in line and job in line and re.search(rf'\bepoch={c_epoch}\b',line) for line in text.splitlines())
            current=[row for row in checkpoints() if row['epoch']==c_epoch and row['pub_id']!=C['pub_id'] and row['state']=='inprogress']
            return current[0] if entered and prepared and len(current)==1 else None
        incomplete=wait(incomplete_replacement,'replacement C native completion withheld',15)
        workers=[h.worker(process,a.binary) for process in h.session_processes(c.child.pid)]
        workers=[worker for worker in workers if worker and worker['job']==job];assert len(workers)==1
        lost=workers[0]
        text=log_since(restore_start)
        assert any('Checkpoint completion fault entered' in line and completion_scope in line and re.search(rf"\bpid={lost['pid']}\b",line) for line in text.splitlines()), 'completion gate was not entered by saved replacement owner'
        prepared_lines=[line for line in text.splitlines() if 'Kafka compact transaction prepared' in line and sink in line and job in line and re.search(rf'\bepoch={c_epoch}\b',line)]
        assert len(prepared_lines)==1
        prepared_fields=dict(re.findall(r'\b(transactional_id|transaction_index|producer_id|producer_epoch)=(\S+)',prepared_lines[0]))
        assert set(prepared_fields)=={'transactional_id','transaction_index','producer_id','producer_epoch'}
        assert prepared_fields['transactional_id']==transaction_ids[int(prepared_fields['transaction_index'])%2]
        assert full_output('replacement-inprogress-prefix')==prefix_full
        incomplete_raw=c.broker('topic','consume',c.outputs[0],'--read-committed=false','--offset',':end','--format','%p\t%o\t%x\t%y\t%k{hex}\t%d\t%v\n')
        (case/'replacement-inprogress-uncommitted.tsv').write_text(incomplete_raw)
        incomplete_physical=[row.split('\t',6) for row in incomplete_raw.splitlines()]
        assert len(incomplete_physical)==3 and json.loads(incomplete_physical[-1][6])['seq']==1
        assert int(incomplete_physical[-1][2])==int(prepared_fields['producer_id']) and int(incomplete_physical[-1][3])==int(prepared_fields['producer_epoch']), 'abandoned physical output differs from native prepared identity'
        abandoned_replay_record=incomplete_physical[-1]

        current=[row for row in checkpoints() if row['pub_id']==incomplete['pub_id']]
        assert len(current)==1 and current[0]['state']=='inprogress' and current[0]['finish_time'] is None
        assert not json.loads(current[0]['operators']).get(sources[0],{}).get('finishTime')
        original_global=archive/(root(c_epoch)/'metadata').relative_to(storage)
        live_global=root(c_epoch)/'metadata'
        assert not live_global.exists() or h.sha(live_global)==h.sha(original_global), 'replacement inprogress published new global metadata'
        assert (c.fault_dir/(completion_scope+'.claimed')).exists() and not (c.fault_dir/(completion_scope+'.release')).exists()
        assert time.monotonic()-completion_armed<25 and 'checkpoint completion fault deadline exceeded' not in log_since(restore_start)
        assert h.terminate_identity(lost,signal.SIGKILL)
        loss_boundary=(case/'runtime.log').stat().st_size
        c.write('operation-replay-inprogress-loss.json',dict(time_ns=time.time_ns(),worker=lost,checkpoint=incomplete,scope=completion_scope,prepared=prepared_fields,prepared_log=prepared_lines,log_boundary=loss_boundary,global_metadata_scope='live path absent or unchanged original failed C bytes; no new replacement global publication'))
        def restored_after_incomplete():
            text=log_since(loss_boundary)
            assert not any('restoring checkpoint' in line and job in line and re.search(rf'\bepoch={c_epoch}\b',line) for line in text.splitlines())
            assert 'Kafka compact transaction resume' not in text
            replacements=[h.worker(process,a.binary) for process in h.session_processes(c.child.pid)]
            replacements=[worker for worker in replacements if worker and worker['job']==job and worker['generation']>lost['generation']]
            restored=any('restoring checkpoint' in line and job in line and re.search(rf'\bepoch={p_epoch}\b',line) for line in text.splitlines())
            old=[row for row in checkpoints() if row['pub_id']==incomplete['pub_id']]
            return dict(worker=replacements[0],checkpoint=old[0],log=text) if replacements and restored and len(old)==1 and old[0]['state']=='failed' else None
        c.write('operation-replay-inprogress-restore.json',wait(restored_after_incomplete,'ordinary P restore after replacement inprogress loss'))
    def replacement_prepared():
        candidates=[row for row in checkpoints() if row['epoch']==c_epoch and row['pub_id']!=C['pub_id'] and row['state']=='committing']
        text=log_since(restore_start)
        paused=any('Kafka commit fault pause' in line and job in line and re.search(rf'\bepoch={c_epoch}\b',line) for line in text.splitlines())
        return candidates[0] if len(candidates)==1 and paused else None
    prepared_replacement=wait(replacement_prepared,'replacement C before EndTxn for actual timeout proof')
    timing['replacement_committing_pause_observed_ns']=time.time_ns()
    replacement_pending=pending(c_epoch,sink)
    assert replacement_pending['generation']==P_pending['generation'] and replacement_pending['epoch']==c_epoch
    assert (replacement_pending['producer_id'],replacement_pending['producer_epoch'])!=(C_pending['producer_id'],C_pending['producer_epoch'])
    replacement_id=transaction_ids[replacement_pending['transaction_index']%2]
    replacement_transactions=describe('operation-replacement-broker')
    matches=[row for row in replacement_transactions if row['transaction_id']==replacement_id]
    assert matches and all(row['producer_id']==replacement_pending['producer_id'] and row['producer_epoch']==replacement_pending['producer_epoch'] and row['state']=='Ongoing' and row['timeout']==900000 for row in matches), 'replacement did not adopt compiled client timeout'
    replacement_last_updates={int(row['start_timestamp']) for row in matches};assert len(replacement_last_updates)==1
    timing['replacement_broker_last_update_ms']=replacement_last_updates.pop()
    timing['replacement_ongoing_observed_ns']=time.time_ns()
    prepared_workers=[h.worker(process,a.binary) for process in h.session_processes(c.child.pid)]
    prepared_workers=[worker for worker in prepared_workers if worker and worker['job']==job];assert len(prepared_workers)==1
    prepared_owner=prepared_workers[0]
    c.write('operation-replacement-prepared.json',dict(checkpoint=prepared_replacement,pending=replacement_pending,broker=matches,worker=prepared_owner))
    gate.unlink()
    if a.operation_cut=='replay-committed':
        wait(lambda:any('Kafka commit fault pause' in line and job in line and 'point="after"' in line and re.search(rf'\bepoch={c_epoch}\b',line) for line in log_since(restore_start).splitlines()),'replacement C EndTxn before acknowledgement')
        acknowledged_output=full_output('replacement-committed-before-loss')
        acknowledged_markers=markers('replacement-committed-before-loss',c_epoch,replacement_pending)
        current=[row for row in checkpoints() if row['pub_id']==prepared_replacement['pub_id']]
        assert len(current)==1 and current[0]['state']=='committing' and current[0]['finish_time'] is None
        assert any(h.identity(process)==h.identity(prepared_owner) for process in h.session_processes(c.child.pid)), 'prepared replacement owner exited before committed loss'
        lost=prepared_owner;assert h.terminate_identity(lost,signal.SIGKILL)
        loss_boundary=(case/'runtime.log').stat().st_size
        replay_after_gate.unlink()
        c.write('operation-replay-committed-loss.json',dict(time_ns=time.time_ns(),worker=lost,checkpoint=current[0],log_boundary=loss_boundary,pending=replacement_pending))
        def recovered_C():
            text=log_since(loss_boundary)
            restored=any('restoring checkpoint' in line and job in line and re.search(rf'\bepoch={c_epoch}\b',line) for line in text.splitlines())
            recovered=any('Kafka prepared transaction recovered' in line and job in line and sink in line for line in text.splitlines())
            return text if restored and recovered and exact_resumes(text,replacement_pending,c_epoch,replacement_id) else None
        c.write('operation-replay-committed-restore.json',wait(recovered_C,'ordinary exact C restore acknowledgement'))
        assert full_output('replacement-committed-after-restore')==acknowledged_output
        assert markers('replacement-committed-after-restore',c_epoch,replacement_pending)==acknowledged_markers
    replayed=wait(lambda:c.snapshot('operation-replayed') if len(c.snapshot('operation-replay-observed')[0][0])==2 else None,'native replayed suffix UUID/state output')
    def replacement_C():
        rows=checkpoints();old=[row for row in rows if row['pub_id']==C['pub_id']];new=[row for row in rows if row['epoch']==c_epoch and row['pub_id']!=C['pub_id'] and row['state']=='ready']
        assert len(old)==1 and old[0]['state']=='failed'
        return dict(old=old[0],new=new[0]) if len(new)==1 else None
    replacement_rows=wait(replacement_C,'distinct replacement C ready')
    timing['replacement_ready_observed_ns']=time.time_ns()
    if getattr(a,'immutable_image_manifest',None):
        for label,transaction,boundary in [('original',C_pending,start_log),('replacement',replacement_pending,restore_start)]:
            prepared=[line for line in log_since(boundary).splitlines() if 'Kafka compact transaction prepared' in line and job in line and sink in line and all(re.search(rf'\b{name}={value}\b',line) for name,value in [('epoch',c_epoch),('producer_id',transaction['producer_id']),('producer_epoch',transaction['producer_epoch']),('transaction_index',transaction['transaction_index'])])]
            assert len(prepared)==1
            timing[label+'_native_prepare_line']=prepared[0]
            timing[label+'_native_prepare_utc']=prepared[0].split()[0]
        timing.update(original_last_update_to_abort_observation_bracket_ms=[timing['last_original_ongoing_request_start_ns']/1e6-timing['original_broker_last_update_ms'],timing['original_abort_observed_ns']/1e6-timing['original_broker_last_update_ms']],replacement_last_update_to_ready_observed_ms=timing['replacement_ready_observed_ns']/1e6-timing['replacement_broker_last_update_ms'],replacement_ready_catalog_finish= replacement_rows['new']['finish_time'],scope='Only this actual immutable artifact/fixture: Redpanda v24.3.7 substitutes last_update_ts for Kafka start_timestamp. These last-update-relative observations do not establish full transaction age, broker start or exact EndTxn time; frozen older receipt labels require this correction')
        c.write('immutable-transaction-timing.json',timing)
    c.write('operation-replacement-C.json',replacement_rows)
    assert replacement_rows['new']['pub_id']==prepared_replacement['pub_id']
    markers('replayed',c_epoch,replacement_pending)
    physical=full_output('replayed-physical').splitlines()
    assert len(physical)==2 and not any(row.split('\t',2)[:2]==records[-1][:2] for row in physical), 'abandoned C physical record became committed'
    if a.operation_cut=='replay-inprogress':assert not any(row.split('\t',2)[:2]==abandoned_replay_record[:2] for row in physical), 'abandoned replacement inprogress physical output became committed'
    restarted_rows=read_rows(P['pub_id'],C['pub_id'])
    assert restarted_rows['job_config']['restart_nonce']==rows['job_config']['restart_nonce']+1
    assert restarted_rows['job_status']['restart_nonce']==restarted_rows['job_config']['restart_nonce']
    c.write('operation-restart-adopted.json',{name:catalog.snapshot(restarted_rows[name]) for name in ['job_config','job_status']})
    assert replayed[1][0].startswith(c.prefix[1][0]) and full_output('replayed').startswith(prefix_full)
    workers=[h.worker(p,a.binary) for p in h.session_processes(c.child.pid)];workers=[w for w in workers if w and w['job']==job]
    assert len(workers)==1
    replacement=workers[0]
    c.broker('topic','produce',c.src,data=''.join(json.dumps(row)+'\n' for row in c.inputs[2:]))
    c.write('operation-fresh-suffix.json',dict(time_ns=time.time_ns(),source_records=2,worker=replacement))
    final=wait(lambda:c.snapshot('observed') if len(c.snapshot('observed-check')[0][0])==4 else None,'all native UUID binding/state/fresh outputs')
    assert final[1][0].startswith(replayed[1][0]) and full_output('final').startswith(prefix_full)
    assert any(h.identity(w)==h.identity(replacement) for p in h.session_processes(c.child.pid) if (w:=h.worker(p,a.binary)))
    for inventory in inventories.values():closure.verify(archive,inventory)
    assert all(h.sha(Path(file['path']))==file['sha256'] for file in files), 'immutable archive changed after live C reuse'
    c.write('operation-final.json',dict(job=c.job(),rows={name:catalog.snapshot(row) for name,row in read_rows(P['pub_id'],C['pub_id']).items()},committed_rows=final[0][0],discarded_C_binding=abandoned_binding,replayed_binding=final[0][0][1]['new_binding'],uuid_regenerated=abandoned_binding!=final[0][0][1]['new_binding'],archive_reverified=True,worker=replacement))
    c.result.update(status='passed',scenario=a.scenario,operation_cut=a.operation_cut,job_id=job,pipeline_id=c.pid,committed_rows=[4],physical_prefix_preserved=True,scope='isolated exact external init/scan and guarded catalog+timeout patch, ordinary P native UUID/state replay, new C/fresh suffix; no production authorization')
