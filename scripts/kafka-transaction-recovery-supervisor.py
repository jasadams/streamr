#!/usr/bin/env python3
"""Finite unpaced real-Kafka liveness and worker-loss qualification."""
import argparse, importlib.util, json, os, re, subprocess, time, urllib.request, signal, sqlite3, shutil
from pathlib import Path

def main():
    p = argparse.ArgumentParser()
    for n in ('binary', 'helper', 'baseline', 'case', 'rpk'):
        p.add_argument('--' + n, type=Path, required=True)
    p.add_argument('--fixture', type=Path)
    p.add_argument('--backend', choices=['memory', 'rocksdb'], default='rocksdb')
    p.add_argument('--scenario', choices=['liveness', 'before', 'after', 'expired', 'original-expired', 'checkpoint-expired', 'orphan'], required=True)
    p.add_argument('--commit-delay-seconds', type=int, choices=[0, 3], default=0)
    p.add_argument('--broker-pause-seconds', type=int, choices=[0, 3], default=0)
    p.add_argument('--broker', default='broker:9092')
    p.add_argument('--namespace', required=True)
    p.add_argument('--port', type=int, default=18146)
    p.add_argument('--checkpoint-seconds', type=int, default=2, choices=[2])
    a = p.parse_args()
    assert os.environ.get('RUST_BUILD_QUEUE_HELD') and re.fullmatch('str146-[a-z0-9-]+', a.namespace)
    assert not (a.scenario != 'liveness' and (a.commit_delay_seconds or a.broker_pause_seconds)), 'delay variants use liveness scenario'
    case = a.case.resolve()
    case.mkdir(exist_ok=True)
    spec = importlib.util.spec_from_file_location('h', a.helper)
    h = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(h)
    h.enable_subreaper()

    def write(n, v):
        (case / n).write_text(json.dumps(v, indent=2, sort_keys=True) + '\n')
    commands = []

    def broker(*args, data=None, command_timeout=60):
        cmd = [str(a.rpk), *args, '-X', 'brokers=' + a.broker]
        r = subprocess.run(cmd, input=data, text=True, capture_output=True, timeout=command_timeout)
        commands.append(dict(command=cmd, exit=r.returncode, stderr=r.stderr))
        write('broker-commands.json', commands)
        assert r.returncode == 0, r.stderr
        return r.stdout

    def api(path, body=None, method=None):
        req = urllib.request.Request(f'http://127.0.0.1:{a.port}/api/v1/' + path, data=None if body is None else json.dumps(body).encode(), headers={'Content-Type': 'application/json'}, method=method)
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.load(r)
    src = a.namespace + '-input'
    outputs = [a.namespace + '-output-' + str(i) for i in range(2)]
    commits = [a.namespace + '-commits-' + str(i) for i in range(2)]
    for t in [src, *outputs, *commits]:
        broker('topic', 'create', t, '--partitions', '1', '--replicas', '1', '--topic-config', 'cleanup.policy=' + ('compact' if t in commits else 'delete'))
    frozen = json.loads((a.baseline / 'frozen-input-hashes.json').read_text())
    for name, digest in frozen.items():
        assert h.sha(a.baseline / name) == digest, 'baseline changed: ' + name
    config = json.loads((a.baseline / 'config.json').read_text())
    original_case = str(a.baseline)
    original_case = str(Path(config['checkpoint-url']).parent)
    original_namespace = json.loads((a.baseline / 'pipeline.json').read_text())['name']
    replacements = {original_case: str(case), original_namespace: a.namespace}
    changed = {}
    for name in ('config.json', 'query.sql'):
        text = (a.baseline / name).read_text()
        for old, new in replacements.items():
            text = text.replace(old, new)
        if name == 'query.sql':
            assert text.count("'sink.recovery_max_bytes'='8388608',") == 2
            text = text.replace("'sink.recovery_max_bytes'='8388608',", '').replace('transaction.timeout.ms=10000', 'transaction.timeout.ms=10000,queue.buffering.max.kbytes=8192')
        (case / name).write_text(text)
        changed[name] = dict(before=frozen[name], after=h.sha(case / name))
    for name in ('inputs.json', 'expected.json'):
        (case / name).write_bytes((a.baseline / name).read_bytes())
    config = json.loads((case / 'config.json').read_text())
    query = (case / 'query.sql').read_text()
    inputs = json.loads((case / 'inputs.json').read_text())
    expected = json.loads((case / 'expected.json').read_text())
    payload = 'a' * 16384
    assert len(inputs) == 65 and inputs == [dict(id=i, payload=payload) for i in range(65)]
    assert expected == [dict(id=i, payload=payload * 16) for i in range(65)]
    assert config['pipeline']['source-batch-size'] == 1 and a.checkpoint_seconds == 2
    assert 'kafka-source-messages-per-second' not in config['pipeline']
    recovery_prefix_count = 1
    fault_count = 1
    if a.fixture:
        manifest = json.loads((a.fixture / 'manifest.json').read_text())
        assert manifest['coverage'] in (['updates', 'state-deletions', 'lookups', 'amplified-output'],['updates','rowkind-retractions','count','sum','amplified-output']), 'fixture must declare required graph coverage'
        for name in ('config.json', 'query.sql', 'inputs.json', 'expected.json'):
            assert h.sha(a.fixture / name) == manifest['sha256'][name], 'external fixture changed'
        config = json.loads((a.fixture / 'config.json').read_text())
        config['checkpoint-url'] = str(case / 'checkpoints')
        config['database']['sqlite']['path'] = str(case / 'catalog.sqlite')
        config['worker']['disk-sql-state']['directory'] = str(case / 'live-state')
        config['worker']['sql-state-backend'] = a.backend
        config['api'].update({'bind-address': '127.0.0.1', 'http-port': a.port, 'run-http-port': a.port})
        assert config['pipeline']['source-batch-size'] == 1 and 'kafka-source-messages-per-second' not in config['pipeline']
        query = (a.fixture / 'query.sql').read_text().replace('${namespace}', a.namespace).replace('${broker}', a.broker)
        assert 'CREATE STATE TABLE' in query.upper() or manifest.get('kind')=='cdc-phases', 'state-table graph missing'
        inputs = json.loads((a.fixture / 'inputs.json').read_text())
        expected = json.loads((a.fixture / 'expected.json').read_text())
        recovery_prefix_count = manifest.get('prefix_input_count',1)
        fault_count = manifest.get('fault_input_count',1)
        assert 0 < recovery_prefix_count < recovery_prefix_count + fault_count < len(inputs)
        for name, value in [('config.json', config), ('inputs.json', inputs), ('expected.json', expected)]:
            write(name, value)
        (case / 'query.sql').write_text(query)
        write('external-fixture.json', manifest)
    else:
        assert a.backend == 'rocksdb', 'frozen baseline backend must remain rocksdb'
    timeout_ms = 120000 if a.scenario in ('before', 'after', 'orphan') or a.commit_delay_seconds or a.broker_pause_seconds else 10000
    assert query.count('transaction.timeout.ms=10000') == 2
    query = query.replace('transaction.timeout.ms=10000', f'transaction.timeout.ms={timeout_ms}')
    (case / 'query.sql').write_text(query)
    write('timeout-delta.json', dict(original_transaction_timeout_ms=10000, actual_transaction_timeout_ms=timeout_ms, reason='fault/delay recovery allows finite worker replacement; expired scenario deliberately retains10s'))
    write('frozen-input-hashes.json', {n: h.sha(case / n) for n in frozen})
    for name in changed:
        changed[name]['initial_candidate_after']=changed[name]['after']
        changed[name]['after']=h.sha(case/name)
    write('baseline-binding.json', dict(baseline_hashes=frozen, allowed_replacements=replacements, changes=changed, external_fixture=bool(a.fixture), query_delta=f'remove obsolete sink.recovery_max_bytes=8388608; add existing client queue.buffering.max.kbytes=8192; transaction.timeout.ms={timeout_ms}; external fixture replaces SQL/config/input/oracle when declared'))
    result = dict(status='failed', binary_sha256=h.sha(a.binary), checkpoint_seconds=a.checkpoint_seconds, input_records=len(inputs), backlog_records=len(inputs) - recovery_prefix_count, expected_payload_bytes_per_sink=None if a.fixture else 64 * len(payload) * 16, producer_buffer_kib=8192, source_batch_size=1, pacing=False)
    child = None
    owned = {}
    log = None

    def wait(fn, label, seconds=180):
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            assert child.poll() is None, 'cluster exited ' + label
            procs = h.session_processes(child.pid)
            result['max_sampled_owned_rss_bytes'] = max(result.get('max_sampled_owned_rss_bytes', 0), sum((x['rss'] for x in procs)))
            for x in procs:
                owned[h.identity(x)] = x
            with (case / 'process-samples.jsonl').open('a') as samples:
                samples.write(json.dumps(dict(time_ns=time.time_ns(), processes=procs)) + '\n')
            v = fn()
            if v:
                return v
            time.sleep(0.1)
        raise AssertionError('timeout ' + label)

    def committed_raw(output,label,index,record_format='%p\t%o\t%v\n'):
        frontier=broker('topic','consume',output,'--read-committed','--offset','start','--num','1','--fetch-max-wait','100ms','--format','%|\n',command_timeout=10)
        positions=frontier.splitlines()
        assert len(positions)==1 and positions[0].isdigit(),'missing actual Fetch last-stable-offset frontier'
        lso=int(positions[0])
        write(f'{label}-{index}-frontier.json',dict(time_ns=time.time_ns(),topic=output,last_stable_offset=lso,scope='actual Fetch LSO; numeric range includes control records for finite completion',source='https://github.com/redpanda-data/redpanda/blob/v24.3.7/src/go/rpk/pkg/cli/topic/consume.go'))
        if lso==0:return ''
        return broker('topic','consume',output,'--read-committed','--offset',f':{lso}','--fetch-max-wait','100ms','--format',record_format,command_timeout=10)

    def snapshot(label):
        rows = []
        raws = []
        for i, out in enumerate(outputs):
            raw = committed_raw(out,label,i)
            (case / f'{label}-{i}.tsv').write_text(raw)
            values = [json.loads(x.split('\t', 2)[2]) for x in raw.splitlines()]
            assert values == expected[:len(values)], 'committed output is not exact ordered oracle prefix'
            rows.append(values)
            raws.append(raw)
        return (rows, raws)

    def cdc_snapshot(label,phase):
        states=[]
        raws=[]
        for index,output in enumerate(outputs):
            raw=committed_raw(output,label,index)
            (case/f'{label}-{index}.tsv').write_text(raw)
            current={}
            positions={}
            records=[json.loads(line.split('\t',2)[2]) for line in raw.splitlines()]
            for record in records:
                envelope=record['payload'] if set(record)=={'payload'} else record
                assert set(envelope)=={'before','after','op'},'CDC envelope fields differ'
                before,after,op=envelope['before'],envelope['after'],envelope['op']
                assert op in ('c','u','d') and (before is None)==(op=='c') and (after is None)==(op=='d'),'invalid CDC operation/images'
                row=after if after is not None else before
                assert set(row)=={'k','n','total','payload'},'CDC aggregate row schema differs'
                key=row['k']
                assert before==current.get(key),'CDC before image is discontinuous or duplicated'
                assert op!='u' or before!=after,'duplicate unchanged CDC update'
                candidates=[step for step,state in enumerate(expected['phases'][:phase+1]) if step>positions.get(key,-1) and state.get(key)==after]
                assert candidates,'CDC transition is not a forward declared source-prefix aggregate'
                positions[key]=candidates[0]
                if after is None:current.pop(key)
                else:current[key]=after
            states.append(current);raws.append(raw)
        return states,raws
    try:
        env = {k: v for k, v in os.environ.items() if not k.startswith(('ARROYO__', 'STREAMR_TEST_'))}
        env['RUST_LOG'] = 'info'
        fault_dir = case / 'fault-gates'
        fault_dir.mkdir()
        env['STREAMR_TEST_CHECKPOINT_COMPLETION_FAULT_DIR'] = str(fault_dir)
        env['STREAMR_TEST_KAFKA_COMMIT_FAULT_DIR'] = str(fault_dir)
        env['STREAMR_TEST_KAFKA_DISPATCH_FAULT_DIR'] = str(fault_dir)
        log = (case / 'runtime.log').open('wb')
        child = subprocess.Popen([str(a.binary), '--config', str(case / 'config.json'), 'cluster'], stdout=log, stderr=subprocess.STDOUT, start_new_session=True, env=env, cwd=case)

        def ready():
            try:
                return api('pipelines')
            except Exception:
                return None
        wait(ready, 'API')
        validation = api('pipelines/validate_query', dict(query=query, udfs=[]))
        write('validation.json', validation)
        assert not validation.get('errors'), validation
        pipeline = api('pipelines', dict(name=a.namespace, query=query, udfs=[], parallelism=1, checkpoint_interval_micros=a.checkpoint_seconds * 1000000))
        write('pipeline.json', pipeline)
        pid = pipeline['id']

        def job():
            return api(f'pipelines/{pid}/jobs')['data'][0]
        wait(lambda: job()['state'] == 'Running', 'Running')
        if a.fixture and manifest.get('kind')=='cdc-phases':
            assert a.scenario=='liveness' and not a.commit_delay_seconds and not a.broker_pause_seconds,'CDC phases are a separate correctness case'
            assert len(inputs)==len(expected['phases'])
            original_job=job()['id']
            previous=['','']
            last_epoch=0
            for phase,event in enumerate(inputs):
                broker('topic','produce',src,data=json.dumps(event)+'\n')
                def phase_visible():
                    observed=cdc_snapshot(f'phase-{phase}-observed',phase)
                    return observed if all(state==expected['phases'][phase] for state in observed[0]) else None
                wait(phase_visible,f'CDC phase{phase} native aggregate committed')
                api(f'pipelines/{pid}',{'stop':'checkpoint'},method='PATCH')
                wait(lambda:job()['state']=='Stopped',f'CDC phase{phase} checkpoint stop')
                observed=phase_visible();assert observed,'stopped phase aggregate differs'
                for index,raw in enumerate(observed[1]):
                    assert raw.startswith(previous[index]),'CDC physical committed prefix changed'
                    (case/f'phase-{phase}-committed-{index}.tsv').write_text(raw)
                previous=observed[1]
                assert job()['id']==original_job,'CDC phase changed job identity'
                with sqlite3.connect(case/'catalog.sqlite') as db:
                    db.row_factory=sqlite3.Row
                    durable=[dict(row) for row in db.execute("SELECT checkpoints.* FROM checkpoints WHERE checkpoints.job_id=? AND checkpoints.state='ready' AND checkpoints.finish_time IS NOT NULL ORDER BY checkpoints.epoch DESC LIMIT 1",(original_job,))]
                assert len(durable)==1,'CDC stopped phase has no ready durable checkpoint'
                assert durable[0]['epoch']>last_epoch,'CDC phase did not advance durable checkpoint'
                last_epoch=durable[0]['epoch']
                saved=case/f'phase-{phase}-checkpoint'
                source=case/'checkpoints'/original_job/'checkpoints'/f'checkpoint-{last_epoch:07}'
                shutil.copytree(source,saved)
                metadata=h.wire((saved/'metadata').read_bytes())
                assert h.one(metadata,1)==original_job.encode() and h.one(metadata,2,0)==last_epoch,'CDC phase checkpoint identity differs'
                artifacts={str(path.relative_to(saved)):h.sha(path) for path in saved.rglob('*') if path.is_file()}
                write(f'phase-{phase}-checkpoint.json',dict(time_ns=time.time_ns(),checkpoint=durable[0],materialized=observed[0],artifacts=artifacts))
                if phase<len(inputs)-1:
                    api(f'pipelines/{pid}',{'stop':'none'},method='PATCH')
                    wait(lambda:job()['state']=='Running',f'CDC phase{phase+1} resume')
            write('final-job.json',job())
            write('final-checkpoints.json',api(f'pipelines/{pid}/jobs/{original_job}/checkpoints'))
            result.update(status='passed',scenario='cdc-phases',phases=len(inputs),pipeline_id=pid,job_id=original_job,physical_prefix_preserved=True,scope='checkpoint-driven correctness of native COUNT/SUM under Debezium updates/deletes/reinsert; every observed transition validated; no unpaced/performance claim')
            return
        broker('topic', 'produce', src, data=''.join((json.dumps(x) + '\n' for x in inputs[:recovery_prefix_count])))

        def prefix_ready():
            x = snapshot('prefix')
            return x if all((v == expected[:manifest['prefix_output_count'] if a.fixture else 1] for v in x[0])) else None
        prefix = wait(prefix_ready, 'committed prefix')
        write('prefix-job.json', job())
        api(f'pipelines/{pid}', {'stop': 'checkpoint'}, method='PATCH')
        wait(lambda: job()['state'] == 'Stopped', 'checkpoint stop')
        write('stopped-job.json', job())
        write('prefix-checkpoints.json', api(f"pipelines/{pid}/jobs/{job()['id']}/checkpoints"))
        cut = len(inputs) if a.scenario == 'liveness' else recovery_prefix_count + fault_count
        broker('topic', 'produce', src, data=''.join((json.dumps(x) + '\n' for x in inputs[recovery_prefix_count:cut])))
        write('input-scheduling.json', dict(prefix_count=recovery_prefix_count, preloaded_count=cut - recovery_prefix_count, fresh_suffix_count=len(inputs) - cut, reason='frozen liveness preloads64; fault variants withhold a declared fresh suffix until replacement worker restore'))
        original_job = job()['id']
        point = 'dispatch' if a.scenario=='orphan' else ('before' if a.scenario in ('expired', 'original-expired', 'checkpoint-expired') or a.commit_delay_seconds or a.broker_pause_seconds else a.scenario)
        gate = fault_dir / f'{point}-{original_job}'
        if a.scenario != 'liveness' or a.commit_delay_seconds or a.broker_pause_seconds:
            gate.touch()
        if a.scenario=='checkpoint-expired':
            with sqlite3.connect(case/'catalog.sqlite') as db:
                db.row_factory=sqlite3.Row
                stopped=dict(db.execute("SELECT * FROM checkpoints WHERE job_id=? AND state='ready' ORDER BY epoch DESC LIMIT 1",(original_job,)).fetchone())
            targets=[op for op in json.loads(stopped['operators']) if op.startswith('source_')]
            assert len(targets)==1, 'frozen graph must have exactly one checkpointed source operator'
            completion_epoch=stopped['epoch']+1
            completion_operator=targets[0]
            completion_scope=f"completion-{original_job.encode().hex()}-{completion_operator.encode().hex()}-0-{completion_epoch}"
            (fault_dir/(completion_scope+'.request')).touch()
            write('operator-completion-request.json',dict(time_ns=time.time_ns(),job=original_job,operator=completion_operator,subtask=0,epoch=completion_epoch,scope=completion_scope))
        api(f'pipelines/{pid}', {'stop': 'none'}, method='PATCH')
        if a.scenario=='checkpoint-expired':
            def clean_runtime():
                return re.sub(chr(27)+'\\[[0-?]*[ -/]*[@-~]','',(case/'runtime.log').read_text(errors='replace'))
            def checkpoint_incomplete(label):
                with sqlite3.connect(case/'catalog.sqlite') as db:
                    db.row_factory=sqlite3.Row
                    rows=[dict(row) for row in db.execute('SELECT * FROM checkpoints WHERE job_id=? AND epoch=?',(original_job,completion_epoch))]
                assert len(rows)==1 and rows[0]['state']=='inprogress' and rows[0]['finish_time'] is None, 'checkpoint already authorized commit while completion withheld'
                details=json.loads(rows[0]['operators'])
                assert not details.get(completion_operator,{}).get('finishTime'), 'selected operator completion already reported'
                assert not any('Kafka commit fault pause' in line and original_job in line and re.search(rf'\bepoch={completion_epoch}\b',line) for line in clean_runtime().splitlines()), 'commit dispatched before withheld completion released'
                write(label,dict(time_ns=time.time_ns(),checkpoint=rows[0]))
                return rows[0]
            def prepared_while_incomplete():
                text=clean_runtime()
                if not any('Checkpoint completion fault entered' in line and completion_scope in line for line in text.splitlines()):return None
                prepared=[]
                for line in text.splitlines():
                    if 'Kafka compact transaction prepared' not in line or original_job not in line or not re.search(rf'\bepoch={completion_epoch}\b',line):continue
                    fields=dict(re.findall(r'\b(operator_id|transactional_id|transaction_index|producer_id|producer_epoch)=(\S+)',line))
                    if len(fields)!=5:continue
                    prepared.append(dict(operator=fields['operator_id'],transactional_id=fields['transactional_id'],transaction_index=int(fields['transaction_index']),producer_id=int(fields['producer_id']),producer_epoch=int(fields['producer_epoch']),epoch=completion_epoch))
                return prepared if len(prepared)==2 else None
            pre_transactions=wait(prepared_while_incomplete,'dual sink prepare while source operator completion withheld',15)
            completion_observed_monotonic=time.monotonic()
            completion_observed_ns=time.time_ns()
            checkpoint_incomplete('operator-checkpoint-incomplete-ongoing.json')
            def describe_prepared(label,state):
                raw=broker('cluster','txn','describe',*[t['transactional_id'] for t in pre_transactions],'--format=json',command_timeout=10)
                (case/(label+'.json.stdout')).write_text(raw)
                rows=json.loads(raw)
                matched=[]
                for transaction in pre_transactions:
                    values=[row for row in rows if row['transaction_id']==transaction['transactional_id']]
                    assert values and all(row['producer_id']==transaction['producer_id'] and row['timeout']==10000 for row in values), 'broker identity/timeout differs from prepared sink'
                    matched.extend(values)
                write(label+'.json',dict(time_ns=time.time_ns(),transactions=matched))
                return matched if all(row['state']==state for row in matched) else None
            ongoing=describe_prepared('operator-broker-ongoing','Ongoing')
            assert ongoing and all(row['producer_epoch']==next(t['producer_epoch'] for t in pre_transactions if t['transactional_id']==row['transaction_id']) for row in ongoing), 'prepared sink not Ongoing before delayed completion'
            expiry_probes=0
            def expired_while_incomplete():
                nonlocal expiry_probes
                checkpoint_incomplete(f'operator-checkpoint-incomplete-{expiry_probes:04}.json')
                rows=describe_prepared(f'operator-broker-probe-{expiry_probes:04}','CompleteAbort')
                expiry_probes+=1
                return rows if rows and time.monotonic()-completion_observed_monotonic>=10.25 else None
            expired=wait(expired_while_incomplete,'broker expiry while global operator checkpoint incomplete',20)
            assert all(row['producer_epoch']==next(t['producer_epoch']+1 for t in pre_transactions if t['transactional_id']==row['transaction_id']) for row in expired), 'expiry did not bump exact prepared identity once'
            checkpoint_incomplete('operator-checkpoint-incomplete-aborted.json')
            write('operator-expiry-before-release.json',dict(time_ns=time.time_ns(),prepared=pre_transactions,ongoing=ongoing,aborted=expired,scope=completion_scope,prepared_observed_ns=completion_observed_ns,completion_withheld_seconds=time.monotonic()-completion_observed_monotonic))
            (fault_dir/(completion_scope+'.release')).touch()
        if a.scenario != 'liveness' or a.commit_delay_seconds or a.broker_pause_seconds:

            def paused():
                text = re.sub(chr(27) + '\\[[0-?]*[ -/]*[@-~]', '', (case / 'runtime.log').read_text(errors='replace'))
                message='Kafka commit dispatch fault pause' if a.scenario=='orphan' else 'Kafka commit fault pause'
                lines = [line for line in text.splitlines() if message in line and original_job in line and (f'point="{point}"' in line)]
                return lines[-1] if len(lines) >= (1 if a.scenario=='orphan' else 2) else None
            pause = wait(paused, 'selected commit fault gate')
            if a.scenario == 'liveness':
                if a.broker_pause_seconds:
                    write('broker-pause-request.json', dict(time_ns=time.time_ns(), monotonic_ns=time.monotonic_ns(), point=point, log=pause))
                    wait(lambda: (case / 'broker-paused.json').exists(), 'owned broker pause acknowledgement')
                started = time.monotonic()
                if a.commit_delay_seconds:
                    while time.monotonic() - started < a.commit_delay_seconds:
                        wait(lambda: True, 'commit delay process sample')
                        time.sleep(0.1)
                gate.unlink()
                if a.broker_pause_seconds:
                    time.sleep(.5)
                    delay_epoch=int(re.search(r'epoch=(\d+)',pause).group(1))
                    with sqlite3.connect(case/'catalog.sqlite') as db:
                        db.row_factory=sqlite3.Row
                        blocked=[dict(row) for row in db.execute('SELECT checkpoints.* FROM checkpoints WHERE checkpoints.epoch=? AND checkpoints.job_id=?',(delay_epoch,original_job))]
                    assert len(blocked)==1 and blocked[0]['state']=='committing', 'broker pause did not retain pending commit'
                    assert not (case/'broker-resumed.json').exists(), 'broker already resumed before blocked observation'
                    write('broker-blocked-checkpoint.json',dict(time_ns=time.time_ns(),monotonic_ns=time.monotonic_ns(),checkpoint=blocked[0]))
                    wait(lambda: (case / 'broker-resumed.json').exists(), 'owned broker unpause acknowledgement')
                write('commit-delay.json', dict(time_ns=time.time_ns(), seconds=a.commit_delay_seconds, broker_pause_seconds=a.broker_pause_seconds, gate_log=pause))
            if a.scenario != 'liveness':
                restore_log_start = (case / 'runtime.log').stat().st_size
                match = re.search('epoch=(\\d+)', pause)
                assert match, 'missing gate epoch'
                epoch = int(match.group(1))
                with sqlite3.connect(case / 'catalog.sqlite') as db:
                    db.row_factory = sqlite3.Row
                    selected = [dict(row) for row in db.execute('SELECT checkpoints.* FROM checkpoints WHERE checkpoints.epoch=? AND checkpoints.state=? AND checkpoints.job_id=?', (epoch, 'committing', original_job))]
                assert selected and all((row['job_id'] == selected[0]['job_id'] for row in selected))
                assert len(selected) == 1, 'selected checkpoint is not durably committing'
                if a.scenario=='checkpoint-expired':
                    operator_span=next(event for event in json.loads(selected[0]['event_spans']) if event['event']=='CheckpointingOperators')
                    elapsed_us=operator_span['finish_time']-operator_span['start_time']
                    assert elapsed_us>10000000, 'actual global operator checkpoint did not cross configured10s transaction window'
                    write('operator-global-checkpoint-duration.json',dict(time_ns=time.time_ns(),epoch=epoch,operator_span=operator_span,elapsed_us=elapsed_us,transaction_timeout_ms=10000))
                retained = case / 'selected-checkpoint-before-loss'
                shutil.copytree(case / 'checkpoints', retained)
                write('checkpoint-artifacts.json', {str(path.relative_to(retained)): h.sha(path) for path in retained.rglob('*') if path.is_file()})
                metadata_root = retained / original_job / 'checkpoints' / f'checkpoint-{epoch:07}'
                metadata = h.wire((metadata_root / 'metadata').read_bytes())
                assert h.one(metadata, 1) == original_job.encode() and h.one(metadata, 2, 0) == epoch
                transactions = []
                source_offsets=[]
                declared_sinks={}
                for declaration in re.finditer(r'CREATE\s+TABLE\s+([A-Za-z_]\w*)\b.*?WITH\s*\((.*?)\);',query,re.I|re.S):
                    for output_topic in outputs:
                        if re.search(r"\btopic\s*=\s*'"+re.escape(output_topic)+"'",declaration.group(2)):
                            declared_sinks[declaration.group(1)]=output_topic
                assert len(declared_sinks)==2, 'query must declare exactly two qualified Kafka outputs'
                for operator in metadata.get(6, []):
                    op = h.wire((metadata_root / ('operator-' + operator.decode()) / 'metadata').read_bytes())
                    info = h.wire(h.one(op, 1))
                    assert h.one(info, 1) == original_job.encode() and h.one(info, 3, 0) == epoch
                    for name, encoded in h.pairs(op, 13).items():
                        table = h.wire(encoded)
                        if h.one(table, 1, 0) != 1:
                            continue
                        payload = h.wire(h.one(table, 2, b''))
                        if name==b'k' and operator.decode().startswith('source_'):
                            import pyarrow.parquet as parquet
                            for relative in payload.get(1,[]):
                                state=parquet.read_table(retained/relative.decode(),columns=['key','value'])
                                for encoded_key,encoded_value in zip(state['key'].to_pylist(),state['value'].to_pylist()):
                                    key,end=h.bincode_uint(encoded_key);assert end==len(encoded_key)
                                    partition,pos=h.bincode_uint(encoded_value)
                                    offset,end=h.bincode_uint(encoded_value,pos);assert end==len(encoded_value)
                                    signed=lambda value:(value>>1)^-(value&1)
                                    assert signed(key)==signed(partition)==0
                                    source_offsets.append(dict(operator=operator.decode(),partition=0,next_offset=signed(offset)))
                        for task, encoded_pending in h.pairs(payload, 2, numeric=True).items():
                            assert name==b'i', 'prepared commit metadata must belong to Kafka identity table i'
                            matched_topics=[topic for sink,topic in declared_sinks.items() if operator.decode().startswith('sink_'+sink+'_')]
                            assert len(matched_topics)==1, 'compact commit is not owned by a declared Kafka output sink'
                            assert len(encoded_pending) <= 4096
                            pending = json.loads(encoded_pending)
                            assert set(pending) == {'version', 'generation', 'epoch', 'transaction_index', 'producer_id', 'producer_epoch'}
                            assert pending['version'] == 4 and pending['epoch'] == epoch and pending['generation']
                            assert pending['producer_id'] >= 0 and pending['producer_epoch'] >= 0 and (task == 0)
                            transaction_id=f"streamr-v4-id-{original_job}-{operator.decode()}-{matched_topics[0]}-{task}-{pending['transaction_index']%2}"
                            transactions.append(dict(operator=operator.decode(), subtask=task, transactional_id=transaction_id, metadata_bytes=len(encoded_pending), **pending))
                assert len(transactions) == 2 and len({t['operator'] for t in transactions}) == 2, 'missing dual sink compact prepared metadata'
                if a.scenario=='checkpoint-expired':
                    assert all(any(all(t[k]==p[k] for k in ('operator','transactional_id','transaction_index','producer_id','producer_epoch','epoch')) for p in pre_transactions) for t in transactions), 'durable pending identity differs from before operator completion'
                write('selected-transactions.json', transactions)
                if a.scenario=='original-expired':
                    raw=broker('cluster','txn','describe',*[t['transactional_id'] for t in transactions],'--format=json',command_timeout=10)
                    (case/'broker-ongoing-before-delay.json.stdout').write_text(raw)
                    descriptions=json.loads(raw)
                    for transaction in transactions:
                        matching=[row for row in descriptions if row['transaction_id']==transaction['transactional_id']]
                        assert matching and all(row['producer_id']==transaction['producer_id'] and row['producer_epoch']==transaction['producer_epoch'] and row['state']=='Ongoing' and row['timeout']==10000 for row in matching), 'saved identity was not broker-proven Ongoing before expiry'
                    write('broker-ongoing-before-delay.json',dict(time_ns=time.time_ns(),transactions=descriptions))
                if a.scenario in ('original-expired','checkpoint-expired'):
                    source_raw=broker('topic','consume',src,'--offset',':end','--fetch-max-wait','100ms','--format','%p\t%o\t%v\n',command_timeout=10)
                    (case/'literal-source.tsv').write_text(source_raw)
                    assert [json.loads(row.split('\t',2)[2]) for row in source_raw.splitlines()]==inputs[:cut], 'actual broker source differs from literal declared prefix'
                assert len(source_offsets)==1 and source_offsets[0]['next_offset']==cut,'selected source checkpoint must contain exactly the declared fault input prefix'
                write('selected-source-offsets.json',source_offsets)
                if a.scenario=='orphan':
                    broker('topic','produce',src,data=json.dumps(inputs[cut])+'\n')
                    def orphan_delivered():
                        records=[]
                        for index,output in enumerate(outputs):
                            raw=broker('topic','consume',output,'--read-committed=false','--offset',':end','--fetch-max-wait','100ms','--format','%p\t%o\t%x\t%y\t%v\n')
                            (case/f'orphan-read-uncommitted-{index}.tsv').write_text(raw)
                            physical=[line.split('\t',4) for line in raw.splitlines()]
                            values=[json.loads(row[4]) for row in physical]
                            assert values==expected[:len(values)],'uncommitted records differ from exact source oracle'
                            if values!=expected[:cut+1]:return None
                            partition,offset,producer_id,producer_epoch,payload=physical[cut]
                            transaction=next(t for t in transactions if f'-{output}-' in t['transactional_id'])
                            q_index=transaction['transaction_index']+1
                            q_id=f"streamr-v4-id-{original_job}-{transaction['operator']}-{output}-0-{q_index%2}"
                            description_raw=broker('cluster','txn','describe',q_id,'--format=json',command_timeout=10)
                            (case/f'orphan-transaction-{index}.json.stdout').write_text(description_raw)
                            descriptions=json.loads(description_raw)
                            matches=[row for row in descriptions if row['transaction_id']==q_id]
                            assert matches and all(row['producer_id']==int(producer_id) and row['producer_epoch']==int(producer_epoch) and row['state']=='Ongoing' for row in matches),'actual Q record producer identity is not the expected ongoing alternate transaction'
                            assert any(row['topic']==output and row['partition']==int(partition) for row in matches),'Q producer transaction does not own actual output partition'
                            records.append(dict(output=output,partition=int(partition),offset=int(offset),producer_id=int(producer_id),producer_epoch=int(producer_epoch),payload_sha256=__import__('hashlib').sha256(payload.encode()).hexdigest(),operator=transaction['operator'],transaction_index=q_index,transactional_id=q_id,broker_description=matches))
                        return records
                    orphan_records=wait(orphan_delivered,'actual alternate Q payload delivered to both broker outputs')
                    write('orphan-records-before-loss.json',dict(time_ns=time.time_ns(),source_record_index=cut,records=orphan_records,scope='actual read-uncommitted Q data; no Q marker exists under one pending checkpoint'))
                before = snapshot('before-loss')
                def markers(label):
                    observed=[]
                    for index,topic in enumerate(commits):
                        raw=committed_raw(topic,label+'-marker',index,'%p\t%o\t%k\t%v\n')
                        (case/f'{label}-marker-{index}.tsv').write_text(raw)
                        physical=[line.split('\t',3) for line in raw.splitlines()]
                        values=[json.loads(line[3]) for line in physical]
                        transaction=next(t for t in transactions if f'-{outputs[index]}-' in t['transactional_id'])
                        expected_key=f"streamr-v4/{original_job}/{transaction['operator']}/{outputs[index]}/0"
                        assert all(row[2]==expected_key for row in physical), 'marker key is not exact selected sink key'
                        assert values and all(v['version']==4 and v['generation']==transaction['generation'] and v['epoch']<epoch for v in values), 'marker does not prove prior-only selected generation'
                        observed.append(values)
                    return observed
                if a.scenario in ('expired','original-expired','checkpoint-expired'):
                    prior_markers=markers('before-loss')
                workers = [h.worker(p, a.binary) for p in h.session_processes(child.pid)]
                workers = [w for w in workers if w and w['job'] == original_job]
                assert len(workers) == 1, 'expected one owned worker'
                owner = workers[0]
                assert h.identity(owner) in owned
                if a.scenario in ('expired', 'original-expired', 'checkpoint-expired'):
                    probe_number=0
                    def broker_aborted():
                        nonlocal probe_number
                        label=f'broker-transaction-probe-{probe_number:04}'
                        probe_number+=1
                        ids=[transaction['transactional_id'] for transaction in transactions]
                        raw=broker('cluster','txn','describe',*ids,'--format=json',command_timeout=10)
                        (case/(label+'.json.stdout')).write_text(raw)
                        descriptions=json.loads(raw)
                        assert isinstance(descriptions,list),'pinned rpk transaction description must be an array'
                        # Pinned rpk v24.3.7 JSON enumerates partitions, dropping
                        # completed transactions whose topic list is empty.
                        # Its text formatter always emits transaction identity.
                        if not all(any(row['transaction_id']==transaction_id for row in descriptions) for transaction_id in ids):
                            text=broker('cluster','txn','describe',*ids,'--format=text',command_timeout=10)
                            (case/(label+'.text.stdout')).write_text(text)
                            descriptions=[]
                            for line in text.splitlines():
                                columns=line.split()
                                if len(columns)>=5 and columns[1] in ids:
                                    descriptions.append(dict(transaction_id=columns[1],producer_id=int(columns[2]),producer_epoch=int(columns[3]),state=columns[4]))
                        states=[]
                        for transaction in transactions:
                            matches=[row for row in descriptions if row['transaction_id']==transaction['transactional_id']]
                            assert matches,'broker transaction identity missing from pinned rpk response'
                            assert all(row['producer_id']==transaction['producer_id'] and row['producer_epoch']>=transaction['producer_epoch'] for row in matches),'broker transaction producer identity differs before fault'
                            states.append(dict(transactional_id=transaction['transactional_id'],saved_producer_id=transaction['producer_id'],saved_producer_epoch=transaction['producer_epoch'],observed=matches))
                        write(label+'-bound.json',dict(time_ns=time.time_ns(),transactions=states))
                        return states if all(all(row['state']=='CompleteAbort' for row in item['observed']) for item in states) else None
                    aborted=wait(broker_aborted,'broker-observed CompleteAbort for both saved prepared identities',60)
                    write('broker-aborted-before-loss.json',aborted)
                    if a.scenario in ('original-expired','checkpoint-expired'):
                        assert all(row['producer_epoch']==item['saved_producer_epoch']+1 for item in aborted for row in item['observed']), 'expiry did not produce exactly one broker epoch bump'
                        # Unlike expired, exercise EndTxn on the original live
                        # producer before imposing actual worker process loss.
                        write('original-release.json',dict(time_ns=time.time_ns(),worker=owner,transactions=transactions))
                        assert any(h.identity(w)==h.identity(owner) for p in h.session_processes(child.pid) if (w:=h.worker(p,a.binary))), 'original worker absent at gate release'
                        gate.unlink()
                        def original_failed():
                            with (case/'runtime.log').open('rb') as stream:
                                stream.seek(restore_log_start)
                                text=stream.read().decode(errors='replace')
                            lines=[line for line in text.splitlines() if 'Kafka transaction failed without acknowledgement:' in line and any(t['operator'] in line for t in transactions) and 'Kafka prepared transaction recovery failed:' not in line]
                            assert 'Kafka compact transaction resume' not in text, 'recovery overtook original-failure capture'
                            assert not lines or any('fenced' in line.lower() for line in lines), 'original EndTxn error is not fencing'
                            return lines if lines else None
                        failures=wait(original_failed,'original producer EndTxn failure',15)
                        write('original-failure.json',dict(time_ns=time.time_ns(),worker=owner,lines=failures))
                assert h.terminate_identity(owner, signal.SIGKILL), 'worker exited before SIGKILL'
                loss_log_start=(case/'runtime.log').stat().st_size
                if gate.exists():gate.unlink()
                write('fault.json', dict(time_ns=time.time_ns(), point=a.scenario, worker=owner, checkpoint=selected[0], gate_log=pause))

                def restarted():
                    candidates = [h.worker(p, a.binary) for p in h.session_processes(child.pid)]
                    return [w for w in candidates if w and w['job'] == original_job and (w['generation'] > owner['generation'])]
                if a.scenario in ('expired', 'original-expired', 'checkpoint-expired'):
                    observed_replacements=[]
                    def missing_marker_refusal():
                        observed_replacements.extend(w for w in restarted() if h.identity(w) not in {h.identity(old) for old in observed_replacements})
                        with (case/'runtime.log').open('rb') as stream:
                            stream.seek(loss_log_start)
                            text=stream.read().decode(errors='replace')
                        return 'Kafka prepared transaction recovery failed:' in text and 'no exact committed marker' in text
                    wait(missing_marker_refusal, 'explicit missing-marker refusal after broker-proven transaction abort')
                    if a.scenario in ('original-expired','checkpoint-expired'):
                        assert observed_replacements, 'no live post-loss replacement generation observed'
                        replacement=observed_replacements
                        write('replacement-workers.json',replacement)
                        def selected_refusal():
                            with (case/'runtime.log').open('rb') as stream:
                                stream.seek(loss_log_start)
                                text=re.sub(chr(27)+'\\[[0-?]*[ -/]*[@-~]','',stream.read().decode(errors='replace'))
                            restores=[line for line in text.splitlines() if 'restoring checkpoint' in line and original_job in line and re.search(rf'\bepoch={epoch}\b',line)]
                            if not restores:return None
                            receipts=[]
                            for transaction in transactions:
                                output_index=next(i for i,out in enumerate(outputs) if f'-{out}-' in transaction['transactional_id'])
                                observed_epoch=prior_markers[output_index][-1]['epoch']
                                resumes=[line for line in text.splitlines() if 'Kafka compact transaction resume' in line and original_job in line and transaction['operator'] in line and transaction['transactional_id'] in line and all(re.search(rf'\b{name}={value}\b',line) for name,value in [('epoch',epoch),('transaction_index',transaction['transaction_index']),('producer_id',transaction['producer_id']),('producer_epoch',transaction['producer_epoch'])])]
                                errors=[line for line in text.splitlines() if 'Kafka prepared transaction recovery failed:' in line and transaction['operator'] in line and f'no exact committed marker (observed {observed_epoch}, expected {epoch})' in line]
                                if resumes and errors:receipts.append(dict(transaction=transaction,resume=resumes,error=errors))
                            return dict(restores=restores,transactions=receipts) if receipts else None
                        write('selected-restore-refusal.json',wait(selected_refusal,'exact selected checkpoint/identity resume and refusal'))
                    wait(lambda: job()['state'] == 'Failed', 'terminal refusal')
                    assert markers('expired-refusal')==prior_markers, 'refused transaction changed committed markers'
                    rows, raws = snapshot('expired-refusal')
                    assert rows == prefix[0] and raws == prefix[1], 'expired transaction published or changed physical prefix'
                    write('expired-job.json', job())
                    result.update(status='passed', scenario=a.scenario, scope='actual broker CompleteAbort observed for both saved prepared identities under configured10s timeout before worker loss; explicit missing-marker recovery refusal and unchanged physical prefix')
                    return
                replacement = wait(restarted, 'fresh worker generation')
                write('replacement-workers.json', replacement)

                def restored():
                    with (case / 'runtime.log').open('rb') as stream:
                        stream.seek(restore_log_start)
                        text = re.sub(chr(27) + '\\[[0-?]*[ -/]*[@-~]', '', stream.read().decode(errors='replace'))
                    matching = [line for line in text.splitlines() if 'restoring checkpoint' in line and original_job in line and re.search(f'\\bepoch={epoch}\\b', line)]
                    return matching and 'restored checkpoint was in committing phase, sending commits' in text
                wait(restored, 'committing checkpoint restore log')

                def native_resumed():
                    with (case / 'runtime.log').open('rb') as stream:
                        stream.seek(restore_log_start)
                        text = re.sub(chr(27) + '\\[[0-?]*[ -/]*[@-~]', '', stream.read().decode(errors='replace'))
                    lines = [line for line in text.splitlines() if 'Kafka prepared transaction recovered' in line and original_job in line]
                    for transaction in transactions:
                        if not any((re.search(f'\\bepoch={epoch}\\b', line) and re.search(f"\\bproducer_id={transaction['producer_id']}\\b", line) and (transaction['operator'] in line) and re.search(rf"\bproducer_epoch={transaction['producer_epoch']}\b",line) and re.search(rf"\btransaction_index={transaction['transaction_index']}\b",line) and re.search(r'\btransactional_id="?'+re.escape(transaction['transactional_id'])+r'(?:"|\s|$)',line) for line in lines)):
                            return None
                    return lines
                write('native-resume-lines.json', wait(native_resumed, 'dual native prepared transaction resume identity'))
                if a.scenario=='orphan':
                    with (case/'runtime.log').open('rb') as stream:
                        stream.seek(restore_log_start);restore_text=re.sub(chr(27)+r'\[[0-?]*[ -/]*[@-~]','',stream.read().decode(errors='replace'))
                    fences=[]
                    for orphan in orphan_records:
                        matching=[line for line in restore_text.splitlines() if 'Kafka alternate transaction fenced' in line and original_job in line and orphan['operator'] in line and re.search(r'\btransactional_id="?'+re.escape(orphan['transactional_id'])+r'(?:"|\s|$)',line) and re.search(rf"\btransaction_index={orphan['transaction_index']}\b",line)]
                        assert len(matching)==1,'missing exact Q-slot alternate fencing receipt'
                        committed_lines=[line for line in restore_text.splitlines() if 'Kafka prepared transaction recovered' in line and orphan['operator'] in line]
                        assert committed_lines and restore_text.index(matching[0])<restore_text.index(committed_lines[0]),'Q must be fenced before pending P recovery'
                        fences.extend(matching)
                    write('orphan-fence-lines.json',fences)
                recovered = wait(lambda: job()['state'] == 'Running', 'recovered Running')
                committed = wait(lambda: snapshot('recovered-prefix') if all((len(v) > len(prefix[0][i]) for i, v in enumerate(snapshot('recovered-observed')[0]))) else None, 'recovered committing transaction visible')
                fresh_start=cut+1 if a.scenario=='orphan' else cut
                broker('topic', 'produce', src, data=''.join((json.dumps(x) + '\n' for x in inputs[fresh_start:])))
                write('fresh-suffix.json', dict(time_ns=time.time_ns(), source_records=len(inputs) - fresh_start, replayed_source_records=1 if a.scenario=='orphan' else 0, recovered_workers=replacement, physical_prefix_rows=[len(v) for v in committed[0]]))

        def complete():
            rows, raws = snapshot('observed')
            return (rows, raws) if all((v == expected for v in rows)) else None
        final = wait(complete, 'all declared committed exact rows', 180)
        assert job()['id'] == original_job, 'recovery changed job identity'
        for index, raw in enumerate(final[1]):
            assert raw.startswith(prefix[1][index]), 'physical committed prefix changed'
            if a.scenario != 'liveness':
                assert raw.startswith(before[1][index]), 'physical pre-loss prefix changed'
                assert raw.startswith(committed[1][index]) and len(final[0][index]) > len(committed[0][index]), 'missing fresh suffix'
                current = [h.worker(p, a.binary) for p in h.session_processes(child.pid)]
                assert any((w and h.identity(w) == h.identity(replacement[0]) for w in current)), 'recovered worker changed during fresh suffix'
            if a.scenario=='orphan':
                physical=[row.split('\t',2) for row in raw.splitlines()]
                orphan=next(record for record in orphan_records if record['output']==outputs[index])
                assert not any(int(partition)==orphan['partition'] and int(offset)==orphan['offset'] for partition,offset,payload in physical),'aborted Q physical record became read-committed visible'
                partition,offset,payload=physical[cut]
                assert int(partition)==orphan['partition'] and int(offset)>orphan['offset'] and json.loads(payload)==expected[cut],'Q source input was not replayed once at a fresh committed physical offset'
            (case / f'final-{index}.tsv').write_text(raw)
        write('final-job.json', job())
        write('final-checkpoints.json', api(f'pipelines/{pid}/jobs/{original_job}/checkpoints'))
        assert 'Kafka checkpoint replay journal exceeds' not in (case / 'runtime.log').read_text(errors='replace')
        result.update(status='passed', scenario=a.scenario, pipeline_id=pid, job_id=original_job, committed_rows=[len(x) for x in final[0]], physical_prefix_preserved=True, scope='finite realKafka correctness/liveness; sampledRSS notpeak/performance')
    finally:
        if child:
            result['cleanup'] = dict(time_ns=time.time_ns(), **h.cleanup(child, owned))
            log.close()
        write('result.json', result)
        assert result.get('cleanup', {}).get('complete') and not result['cleanup'].get('errors'), 'owned process cleanup failed'
    assert result['status'] == 'passed' and result.get('cleanup', {}).get('complete'), result
if __name__ == '__main__':
    main()
