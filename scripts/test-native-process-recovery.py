#!/usr/bin/env python3
"""Finite Linux production CLI worker-loss qualification; no source/test binary mount.

Preparation and execution are separate. Caller provenance is retained, not trusted
as proof of a build. Actual native checkpoint ownership and exact output are gates.
"""
import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request


def require(ok, message):
    if not ok:
        raise ValueError(message)


def sha(path):
    with path.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256')
    return digest.hexdigest()


def write(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def process(pid):
    try:
        base = Path('/proc') / str(pid)
        stat = (base / 'stat').read_text().rsplit(')', 1)[1].split()
        result = dict(pid=pid, parent=int(stat[1]), group=int(stat[2]), session=int(stat[3]),
                      start=stat[19], state=stat[0], rss=int(stat[21])*os.sysconf('SC_PAGE_SIZE'))
        if result['state'] == 'Z':
            return dict(result, exe=None,args=[])
        args = (base / 'cmdline').read_bytes().split(b'\0')[:-1]
        return dict(result,exe=str((base/'exe').resolve(strict=True)),args=[x.decode() for x in args])
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        return None


def session_processes(session):
    return [p for item in Path('/proc').iterdir() if item.name.isdigit()
            and (p := process(int(item.name))) and p['session'] == session]


def enable_subreaper():
    # Reparent owned orphan workers to this finite supervisor, not container PID1.
    libc = ctypes.CDLL(None,use_errno=True)
    require(libc.prctl(36,1,0,0,0) == 0, f'cannot enable Linux subreaper: errno{ctypes.get_errno()}')


def cleanup(child,owned):
    errors, reaped = [], []
    started = time.monotonic()
    signalled = False
    while time.monotonic()-started < 25:
        observed = session_processes(child.pid)
        if child.poll() is not None:
            accepted = {p['pid'] for p in observed if identity(p) in owned or p['parent'] == os.getpid()}
            while True:
                added = {p['pid'] for p in observed if p['parent'] in accepted} - accepted
                if not added:
                    break
                accepted.update(added)
            observed = [p for p in observed if p['pid'] in accepted]
        owned.update({identity(p):p for p in observed})
        # Parent may spawn replacements during graceful shutdown. Rescan the
        # owned session until all live members and adopted zombies are gone.
        if not signalled:
            parent = process(child.pid) if child.poll() is None else None
            if parent and identity(parent) in owned and parent['session'] == child.pid:
                try:
                    terminate_identity(parent,signal.SIGTERM)
                except OSError as error:
                    errors.append(f'parent SIGTERM: {error}')
            signalled = True
        if time.monotonic()-started >= 10:
            if child.poll() is None:
                try:
                    child.kill()
                except OSError as error:
                    errors.append(f'parent SIGKILL: {error}')
            for p in observed:
                try:
                    terminate_identity(p,signal.SIGKILL)
                except OSError as error:
                    errors.append(f'owned SIGKILL: {error}')
        child.poll()  # Reap only Popen's own child through Popen.
        for old in owned.values():
            if old['pid'] == child.pid:
                continue
            try:
                pid,status = os.waitpid(old['pid'],os.WNOHANG)
                if pid:
                    reaped.append(dict(pid=pid,status=status))
            except ChildProcessError:
                pass  # Still controller-owned, already reaped, or no longer child.
            except OSError as error:
                errors.append(f'owned waitpid: {error}')
        remaining = [p for p in session_processes(child.pid) if identity(p) in owned or p['parent'] == os.getpid()]
        if child.poll() is not None and not remaining:
            return dict(complete=True,reaped=reaped,errors=errors)
        time.sleep(0.05)
    remaining = [p for p in session_processes(child.pid) if identity(p) in owned or p['parent'] == os.getpid()]
    return dict(complete=False,reaped=reaped,remaining=remaining,
                errors=['owned session did not become fully terminal/reaped within25s'])


def descendants(parent):
    records = {int(x.name): process(int(x.name)) for x in Path('/proc').iterdir() if x.name.isdigit()}
    owned = {parent}
    while True:
        added = {pid for pid, p in records.items() if p and p['parent'] in owned} - owned
        if not added:
            return [records[pid] for pid in owned if records.get(pid)]
        owned.update(added)


def identity(p):
    return p['pid'], p['start']


def terminate_identity(p, sig):
    # pidfd binds signals to one process even if the numeric PID is recycled.
    try:
        descriptor = os.pidfd_open(p['pid'])
    except ProcessLookupError:
        return False
    try:
        current = process(p['pid'])
        if current and identity(current) == identity(p) and current['state'] != 'Z':
            try:
                signal.pidfd_send_signal(descriptor, sig)
                return True
            except ProcessLookupError:
                return False
        return False
    finally:
        os.close(descriptor)


def worker(p, binary):
    if p is None:
        return None
    if p['exe'] != str(binary) or not p['args'] or p['args'][-1] != 'worker':
        return None
    try:
        encoded_env = (Path('/proc') / str(p['pid']) / 'environ').read_bytes()
    except (FileNotFoundError, ProcessLookupError):
        return None
    env = dict(item.split(b'=', 1) for item in encoded_env.split(b'\0') if b'=' in item)
    require(env.get(b'UNDER_PROCESS_SCHEDULER') == b'true', 'owned worker is not process scheduled')
    p = dict(p)
    p['job'] = env[b'JOB_ID'].decode()
    p['pipeline'] = env[b'PIPELINE_ID'].decode()
    p['generation'] = int(env[b'GENERATION'])
    return p


def api(port, route):
    with urllib.request.urlopen(f'http://127.0.0.1:{port}/api/v1/{route}', timeout=2) as response:
        return json.load(response)


def wire(data):
    """Small protobuf wire reader for existing checkpoint metadata only."""
    pos, result = 0, {}
    def number():
        nonlocal pos
        value = 0
        for shift in range(0, 70, 7):
            require(pos < len(data), 'truncated protobuf')
            byte = data[pos]
            pos += 1
            value |= (byte & 127) << shift
            if byte < 128:
                return value
        raise ValueError('oversized protobuf varint')
    while pos < len(data):
        tag = number()
        field, kind = tag >> 3, tag & 7
        require(field > 0, 'invalid protobuf field')
        if kind == 0:
            value = number()
        elif kind == 2:
            length = number()
            require(length <= len(data)-pos, 'truncated protobuf bytes')
            value = data[pos:pos+length]
            pos += length
        elif kind in (1, 5):
            length = 8 if kind == 1 else 4
            require(length <= len(data)-pos, 'truncated protobuf fixed field')
            value = data[pos:pos+length]
            pos += length
        else:
            raise ValueError('unsupported protobuf wire type')
        result.setdefault(field, []).append(value)
    return result


def one(fields, field, default=None):
    values = fields.get(field, [])
    require(len(values) <= 1, f'duplicate protobuf scalar {field}')
    return values[0] if values else default


def pairs(fields, field, numeric=False):
    result = {}
    for encoded in fields.get(field, []):
        entry = wire(encoded)
        key = one(entry, 1, 0 if numeric else b'')
        require(type(key) is (int if numeric else bytes) and key is not None and key not in result, 'invalid checkpoint map')
        result[key] = one(entry, 2)
    return result


def artifact(root, relative, records, checksum=None):
    require(relative and not relative.startswith('/') and all(p not in ('', '.', '..') for p in relative.split('/')), 'unsafe checkpoint object name')
    path = (root / relative).resolve(strict=True)
    require(path.is_relative_to(root) and path.is_file(), 'checkpoint artifact escapes owned root')
    digest = sha(path)
    require(checksum is None or bytes.fromhex(digest) == checksum, 'checkpoint object checksum differs')
    records.append(dict(path=relative, bytes=path.stat().st_size, sha256=digest))
    return path


def checkpoint(root, epoch, owner, protocol):
    records = []
    def metadata(relative):
        path = artifact(root,relative,records)
        require(path.stat().st_size <= 3*1048576,'checkpoint metadata exceeds existing3MiB limit')
        return wire(path.read_bytes())
    if protocol == 'controller':
        base = f"{owner['job']}/checkpoints/checkpoint-{epoch:07}"
        cp = metadata(base+'/metadata')
        require(one(cp, 1) == owner['job'].encode() and one(cp, 2, 0) == epoch, 'checkpoint identity differs')
        operators = [metadata(base+'/operator-'+name.decode()+'/metadata') for name in cp.get(6, [])]
    else:
        base = f"{owner['pipeline']}/{owner['job']}/generations/{owner['generation']}/checkpoints/checkpoint-{epoch:07}"
        cp = metadata(base+'/checkpoint-manifest.pb')
        require(one(cp, 1) == owner['pipeline'].encode() and one(cp, 2) == owner['job'].encode()
                and one(cp, 3, 0) == owner['generation'] and one(cp, 4, 0) == epoch, 'worker checkpoint identity differs')
        operators = [wire(value) for value in cp.get(10, [])]
    owners = []
    for op in operators:
        info = wire(one(op, 1))
        require(one(info, 1) == owner['job'].encode() and one(info, 3, 0) == epoch and one(info, 6, 0) == 1, 'operator ownership/parallelism differs')
        configs, tables = pairs(op, 14), pairs(op, 13)
        for name, encoded in tables.items():
            require(name in configs, 'checkpoint table has no config')
            table = wire(encoded)
            kind, payload = one(table, 1, 0), wire(one(table, 2, b''))
            files = []
            wrapper = wire(configs[name])
            require(one(wrapper,1,0) == kind,'table config/checkpoint discriminator differs')
            config = wire(one(wrapper,2,b''))
            require(one(config,1,b'') == name,'checkpoint config table name differs')
            operator_id = one(info,2,b'').decode()
            path_prefix = f'{base}/operator-{operator_id}/table-{name.decode()}-000/'
            if kind == 1:
                require(name in (b'i',b'f',b's') and one(wrapper,3,0) == 0,'unsupported global table/state version')
                files = [(x.decode(), None) for x in payload.get(1, [])]
            elif kind == 3:
                require(name == b'native-aggregate-v1' and one(wrapper,3,0) == 1
                        and one(config,2,0) == 1,'native table config/encoding differs')
                schema = one(config,3,b'')
                require(len(schema) == 32 and one(payload,1,0) == 2,'native schema/task format differs')
                subtasks = pairs(payload,2,numeric=True)
                require(set(subtasks) == {0},'native checkpoint must own exactlysubtask zero')
                subtask = wire(subtasks[0])
                expected_generation = 0 if protocol == 'controller' else owner['generation']
                namespace = bytes([1,0])+bytes(4)+(1).to_bytes(4,'big')+len(name).to_bytes(4,'big')+name
                require(one(subtask,1,0) == 0 and one(subtask,2,0) == 2 and one(subtask,3,0) == 1
                        and one(subtask,4,b'') == schema and one(subtask,5,b'') == namespace
                        and one(subtask,6,0) == expected_generation and one(subtask,7,0) == epoch
                        and one(subtask,8,0) == 0,'native subtask namespace/schema/version/epoch/generation differs')
                for f in subtask.get(9,[]):
                    desc = wire(f)
                    relative,checksum = one(desc,1,b'').decode(),one(desc,3,b'')
                    require(relative.startswith(path_prefix+'disk-') and relative.endswith('.parquet')
                            and '/' not in relative[len(path_prefix):],'native object belongs to another owner/epoch')
                    require(len(checksum) == 32 and 0 < one(desc,2,0) <= 2097152
                            and 0 < one(desc,4,0) <= 4096,'native file size/rows/checksum descriptor differs')
                    path = artifact(root,relative,records,checksum)
                    require(path.stat().st_size == one(desc,2,0),'native object size differs')
                    files.append((relative,checksum))
            else:
                raise ValueError(f'unexpected checkpoint table type {kind}')
            require(files and len({f[0] for f in files}) == len(files),'missing/duplicate checkpoint objects')
            if kind == 1:
                for relative,checksum in files:
                    require(relative == path_prefix.rstrip('/'),
                            'global object belongs to another owner/epoch')
                    artifact(root,relative,records,checksum)
            owners.append(dict(operator=one(info, 2).decode(), table=name.decode(), kind=kind, files=[f[0] for f in files]))
    require(sum(x['table'] == 'i' and x['kind'] == 1 for x in owners) == 1, 'missing unique Impulse source checkpoint owner')
    require(sum(x['table'] == 's' and x['kind'] == 1 for x in owners) == 1, 'missing unique source watermark checkpoint owner')
    require(sum(x['table'] == 'f' and x['kind'] == 1 for x in owners) == 2, 'missing two file sink checkpoint owners')
    require(sum(x['table'] == 'native-aggregate-v1' and x['kind'] == 3 for x in owners) == 1, 'missing native aggregate checkpoint owner')
    return dict(epoch=epoch, base=base, owners=owners, artifacts=records)


def unique_object(items):
    result = {}
    for key,value in items:
        require(key not in result, 'duplicate JSON field')
        result[key] = value
    return result


def rows(path):
    with path.open() as stream:
        for line in stream:
            require(line.endswith('\n') and line.strip(), 'incomplete/blank output record')
            yield json.loads(line,object_pairs_hook=unique_object)


def totals(path, limit, final):
    current, transitions = None, 0
    for payload in rows(path):
        if type(payload) is dict and set(payload) == {'payload'}:
            payload = payload['payload']
        require(type(payload) is dict and set(payload) == {'before', 'after', 'op'}, 'unexpected CDC envelope')
        before, after, op = payload['before'], payload['after'], payload['op']
        require(op in ('c', 'u') and (before is None) == (op == 'c') and after is not None, 'unexpected aggregate CDC action')
        require(json.dumps(before,sort_keys=True) == json.dumps(current,sort_keys=True), 'aggregate before-image continuity/types differ')
        require(type(after) is dict and set(after) == {'subtask_index', 'n', 'total', 'lo', 'hi'}, 'aggregate fields differ')
        require(all(type(v) is int for v in after.values()), 'aggregate scalar types differ')
        n = after['n']
        require(0 < n <= limit and after == dict(subtask_index=0, n=n, total=n*(n-1)//2, lo=0, hi=n-1), 'aggregate prefix values differ')
        require(current is None or n > current['n'], 'aggregate prefix did not advance')
        current, transitions = after, transitions+1
    require(current is not None and (not final or current['n'] == limit), 'missing exact final aggregate state')
    return dict(last=current, transitions=transitions)


def bincode_uint(data, pos=0):
    require(pos < len(data), 'truncated bincode integer')
    marker = data[pos]
    pos += 1
    if marker < 251:
        return marker,pos
    require(marker in (251,252,253), 'unsupported bincode integer marker')
    size = {251:2,252:4,253:8}[marker]
    require(size <= len(data)-pos, 'truncated bincode integer payload')
    return int.from_bytes(data[pos:pos+size],'little'),pos+size


def committed_prefix(case, proof, limit, label='committed'):
    import pyarrow.parquet as parquet
    source, sinks = [], {}
    for owner in proof['owners']:
        if owner['table'] not in ('i','f'):
            continue
        values = []
        for relative in owner['files']:
            table = parquet.read_table(case/'checkpoints'/relative,columns=['key','value'])
            require(table.column_names == ['key','value'], 'global state schema differs')
            values.extend(zip(table['key'].to_pylist(),table['value'].to_pylist()))
        require(len(values) == 1, 'small singleton fixture global table must have exactlyone row')
        key,value = values[0]
        require(type(key) is bytes and type(value) is bytes,'global state values are not binary')
        if owner['table'] == 'i':
            subtask,end = bincode_uint(key)
            require(subtask == 0 and end == len(key),'source checkpoint key differs')
            count,end = bincode_uint(value)
            seconds,end = bincode_uint(value,end)
            nanos,end = bincode_uint(value,end)
            require(end == len(value) and nanos < 1000000000,'Impulse counter/starttime state encoding differs')
            source.append(dict(counter=count,start_seconds=seconds,start_nanos=nanos))
        else:
            size,end = bincode_uint(key)
            require(end+size == len(key),'sink checkpoint string encoding differs')
            path = key[end:].decode()
            offset,end = bincode_uint(value)
            require(end == len(value) and path not in sinks,'sink checkpoint offset encoding differs')
            sinks[path] = offset
    require(len(source) == 1 and 0 < source[0]['counter'] < limit,'checkpoint source counter must be proper prefix')
    count = source[0]['counter']
    snapshots = {}
    for name in ('raw.jsonl','totals.jsonl'):
        output = case/'outputs'/name
        require(str(output) in sinks,'checkpoint sink path ownership differs')
        with output.open('rb') as stream:
            data = stream.read(sinks[str(output)])
        require(len(data) == sinks[str(output)],'sink committed bytes are unavailable')
        snapshot = case/(label+'.'+name)
        snapshot.write_bytes(data)
        snapshots[name] = dict(offset=len(data),sha256=sha(snapshot))
    actual = list(rows(case/(label+'.raw.jsonl')))
    expected = [dict(counter=i,subtask_index=0) for i in range(count)]
    require(all(type(row) is dict and set(row) == {'counter','subtask_index'} and all(type(v) is int for v in row.values()) for row in actual),'committed raw output types differ')
    require(actual == expected,'checkpoint source counter and committed raw sink prefix differ')
    snapshots['aggregate'] = totals(case/(label+'.totals.jsonl'),count,True)
    return dict(source=source[0],sinks=snapshots)


def prepare(args):
    require(args.fault_mode != 'controller' or args.protocol == 'controller',
            'controller fault mode currently qualifies controller protocol only')
    case = args.case.resolve()
    require(not case.exists(), 'case directory must be new; evidence is immutable')
    root = args.source_free_root.resolve(strict=True)
    require(not (root/'.git').exists() and not (root/'Cargo.toml').exists(),'source-free root must be a package, not a repository checkout')
    binary = args.binary.resolve(strict=True)
    require(binary.is_relative_to(root), 'binary must be inside caller source-free artifact root')
    require(not case.is_relative_to(root), 'writable case must be outside read-only artifact root')
    with binary.open('rb') as executable:
        require(executable.read(4) == b'\x7fELF', 'top-level binary must be an ELF executable')
    require(args.rows >= 1000 and args.rate > 0 and args.rows/args.rate >= 60, 'fixture must run >=60s and contain >=1000 rows')
    require(args.timeout >= args.rows/args.rate+90, 'timeout must allow complete source plus90s recovery')
    case.mkdir(parents=True)
    for name in ('scratch', 'checkpoints', 'outputs', 'api'):
        (case/name).mkdir()
    config = {'disable-telemetry': True, 'hostname':'127.0.0.1', 'job-controller': args.protocol, 'default-checkpoint-interval': '5s',
              'controller': {'scheduler':'process','bind-address':'127.0.0.1'}, 'process-scheduler': {'slots-per-process':64, 'shutdown-with-controller':True},
              'api': {'run-http-port':args.port, 'bind-address':'127.0.0.1'},
              'worker': {'bind-address':'127.0.0.1','sql-state-backend':args.backend, 'queue-size':128,
                         'execution-resources': {'memory-bytes':16777216, 'max-batch-bytes':1048576},
                         'aggregate-state': {'key-bytes':512,'value-bytes':32768,'page-bytes':131072,'page-entries':64,
                                             'write-bytes':2097152,'write-operations':128,'overlay-bytes':2097152,
                                             'max-pending-output-rows':64,'max-pending-output-bytes':524288,'max-resident-bytes':134217728}}}
    config['worker']['live-state-resources'] = {
        'block-cache-bytes':8388608,'memtable-bytes':4194304,'queued-write-bytes':33554432,
        'decoded-value-bytes':16777216,'scan-page-bytes':2097152,'max-blocking-operations':2,
        'max-snapshots':2,'max-open-databases':2,'disk-reserve-bytes':67108864}
    if args.backend == 'rocksdb':
        config['worker']['disk-sql-state'] = {'directory':str(case/'scratch'),'max-row-bytes':16384}
    write(case/'config.yaml', config)  # JSON is a YAML subset; no third-party parser.
    def sqlpath(path):
        return str(path).replace("'", "''")
    query = f"""CREATE TABLE ticks WITH (connector='impulse', event_rate='{args.rate}', message_count='{args.rows}', event_time_interval='10000');
CREATE TABLE raw WITH (connector='single_file', path='{sqlpath(case/'outputs/raw.jsonl')}', format='json', type='sink');
CREATE TABLE totals WITH (connector='single_file', path='{sqlpath(case/'outputs/totals.jsonl')}', format='debezium_json', type='sink');
INSERT INTO raw SELECT counter, subtask_index FROM ticks;
INSERT INTO totals SELECT subtask_index, COUNT(*) AS n, SUM(counter) AS total, MIN(counter) AS lo, MAX(counter) AS hi FROM ticks GROUP BY subtask_index;
"""
    (case/'query.sql').write_text(query)
    with (case/'expected.raw.jsonl').open('w') as stream:
        for counter in range(args.rows):
            stream.write(json.dumps(dict(counter=counter,subtask_index=0))+'\n')
    write(case/'expected.final.json',dict(subtask_index=0,n=args.rows,total=args.rows*(args.rows-1)//2,lo=0,hi=args.rows-1))
    provenance = json.loads(args.provenance.read_text())
    require(type(provenance) is dict and provenance, 'caller provenance must be a nonempty JSON object')
    write(case/'provenance.json',provenance)
    files = ['config.yaml','query.sql','expected.raw.jsonl','expected.final.json','provenance.json']
    manifest = dict(status='prepared; runtime unverified', binary=str(binary),binary_sha256=sha(binary),source_free_root=str(root),
                    backend=args.backend,protocol=args.protocol,fault_mode=args.fault_mode,rows=args.rows,rate=args.rate,port=args.port,
                    timeout_seconds=args.timeout,startup_timeout_seconds=args.startup_timeout,rss_limit_bytes=args.rss_limit_mib*1048576,
                    artifact_files={str(p.relative_to(root)):sha(p) for p in root.rglob('*') if p.is_file()},
                    harness_sha256=sha(Path(__file__).resolve()), files={name:sha(case/name) for name in files})
    write(case/'manifest.json',manifest)
    return case, manifest


def catalog_snapshot(case, label):
    """Consistent, read-only source connection; backup writes only NEW evidence."""
    import sqlite3
    from contextlib import closing
    database = case/'checkpoints/state.sqlite'
    require(database.is_file(), 'persisted local SQLite catalog missing')
    backup = case/('catalog-'+label+'.sqlite')
    require(not backup.exists(), 'catalog evidence must be new')
    deadline = time.monotonic()+10
    def progress(status, remaining, total):
        require(time.monotonic() < deadline, 'finite catalog backup deadline exceeded')
    with closing(sqlite3.connect(database.as_uri()+'?mode=ro',uri=True,timeout=2)) as source:
        source.execute('PRAGMA query_only=ON')
        with closing(sqlite3.connect(backup,timeout=2)) as destination:
            source.backup(destination,pages=128,progress=progress,sleep=0.01)
    with closing(sqlite3.connect(backup.as_uri()+'?mode=ro',uri=True,timeout=2)) as snapshot:
        snapshot.row_factory = sqlite3.Row
        clusters = [dict(row) for row in snapshot.execute('SELECT id,name FROM cluster_info')]
        pipelines = []
        for row in snapshot.execute('SELECT id,pub_id,textual_repr,program FROM pipelines'):
            require(type(row['program']) is bytes, 'catalog program encoding differs')
            pipelines.append(dict(id=row['id'],pub_id=row['pub_id'],query=row['textual_repr'],
                                  program_sha256=hashlib.sha256(row['program']).hexdigest()))
        jobs = [dict(row) for row in snapshot.execute('''SELECT c.id,c.pipeline_id,c.stop,c.parallelism_overrides,
            c.ignore_state_before_epoch,s.pub_id,s.state,s.run_id FROM job_configs c
            JOIN job_statuses s ON c.id=s.id''')]
        require(len(clusters) == len(pipelines) == len(jobs) == 1, 'isolated catalog must contain exactlyone cluster/pipeline/job')
        job,pipeline = jobs[0],pipelines[0]
        require(job['pipeline_id'] == pipeline['id'] and job['stop'] == 'none'
                and job['ignore_state_before_epoch'] is None, 'catalog job ownership/stop/recovery threshold differs')
        require(pipeline['query'] == (case/'query.sql').read_text(), 'persisted query differs')
        checkpoints = [dict(row) for row in snapshot.execute('''SELECT pub_id,job_id,epoch,min_epoch,state,finish_time
            FROM checkpoints WHERE job_id=? AND state IN ('ready','committing') ORDER BY epoch DESC''',(job['id'],))]
    record = dict(cluster=clusters[0],pipeline=pipeline,job=job,checkpoints=checkpoints,
                  snapshot=str(backup.name),snapshot_sha256=sha(backup))
    write(case/('catalog-'+label+'.json'),record)
    return record


def catalog_identity(catalog):
    return dict(cluster=catalog['cluster'],pipeline=catalog['pipeline'],
                job={key:catalog['job'][key] for key in ('id','pipeline_id','pub_id','parallelism_overrides')})


def retain_checkpoint(case, proof, directory):
    for record in proof['artifacts']:
        source = case/'checkpoints'/record['path']
        destination = case/directory/record['path']
        require(not destination.exists(), 'checkpoint evidence must be new')
        destination.parent.mkdir(parents=True,exist_ok=True)
        destination.write_bytes(source.read_bytes())
        require(sha(destination) == record['sha256'], 'checkpoint changed while retaining evidence')


def preserve_outputs(case, label):
    retained = {}
    for name in ('raw.jsonl','totals.jsonl'):
        source = case/'outputs'/name
        require(source.is_file(), 'missing sink output before fault')
        destination = case/(label+'.'+name)
        require(not destination.exists(), 'output evidence must be new')
        destination.write_bytes(source.read_bytes())
        retained[name] = dict(file=destination.name,bytes=destination.stat().st_size,sha256=sha(destination))
    return retained


def assert_controller_restore_log(path, owner, proof):
    import re
    text = re.sub(r'\x1b\[[0-?]*[ -/]*[@-~]', '', path.read_text(errors='replace'))
    epoch = proof['epoch']
    lines = [line for line in text.splitlines() if 'restoring checkpoint' in line.lower() and owner['job'] in line]
    if not any(re.search(rf'\bepoch={epoch}\b',line) or f'Restoring checkpoint {epoch} for job {owner["job"]}' in line for line in lines):
        return False
    state = proof['committed_prefix']['source']
    restored = re.compile(r'ImpulseSourceState\s*\{\s*counter:\s*(\d+),\s*start_time:\s*SystemTime\s*\{\s*tv_sec:\s*(\d+),\s*tv_nsec:\s*(\d+)\s*\}\s*\}\s*restored')
    observed = [tuple(map(int,match.groups())) for match in restored.finditer(text)]
    if not observed:
        return False
    require((state['counter'],state['start_seconds'],state['start_nanos']) in observed,
            'actual Impulse restored counter/starttime differs from selected checkpoint')
    if proof['catalog_state'] == 'committing':
        if 'restored checkpoint was in committing phase, sending commits' not in text:
            return False
    return True


def verify_manifest(case, manifest):
    binary = Path(manifest['binary'])
    require(sha(binary) == manifest['binary_sha256'], 'binary hash changed')
    require(sha(Path(__file__).resolve()) == manifest['harness_sha256'], 'harness hash changed')
    for name,digest in manifest['artifact_files'].items():
        require(sha(Path(manifest['source_free_root'])/name) == digest, 'packaged artifact hash changed')
    for name,digest in manifest['files'].items():
        require(sha(case/name) == digest, 'fixture/provenance hash changed')
    return binary


def run_controller(case, manifest):
    from contextlib import ExitStack
    require(manifest['protocol'] == 'controller', 'controller fault mode currently qualifies controller protocol only')
    binary = verify_manifest(case,manifest)
    require(hasattr(os,'pidfd_open') and hasattr(signal,'pidfd_send_signal'), 'Linux pidfd signal support is required')
    import pyarrow
    import sqlite3
    with socket.socket() as probe:
        probe.bind(('127.0.0.1',manifest['port']))
    env = {k:v for k,v in os.environ.items() if not k.startswith(('ARROYO__','STREAMR_TEST_','STREAMR_CAPTURE_'))}
    # Restoration logs are assertions for this fault mode, not inferred from completion.
    env['RUST_LOG'] = 'info'
    command = [str(binary),'--config',str(case/'config.yaml'),'run','--name','m3-process-recovery',
               '--parallelism','1','--state-dir',str(case/'checkpoints'),str(case/'query.sql')]
    enable_subreaper()
    start = time.monotonic()
    deadline = start+manifest['timeout_seconds']
    timeline,children,peak = [],[],0
    fault,replacement,original_catalog,recovery_proof = None,None,None,None
    result = dict(status='failed',fault_mode='controller',command=command,pyarrow_version=pyarrow.__version__,sqlite_version=sqlite3.sqlite_version)
    with ExitStack() as logs:
        try:
            log = logs.enter_context((case/'runtime.log').open('xb'))
            child = subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT,env=env,cwd=case,start_new_session=True)
            active = dict(child=child,owned={},cleanup=None,log='runtime.log')
            children.append(active)
            phase_started = time.monotonic()
            startup_complete = False
            while time.monotonic() < deadline:
                require(startup_complete or time.monotonic()-phase_started < manifest['startup_timeout_seconds'],
                        f"production {'restart' if fault else 'initial'} startup deadline exceeded; lastjobstate={result.get('last_job_state','API unavailable')}")
                require(child.poll() is None, 'owned top-level process exited before qualification completed')
                ps = session_processes(child.pid)
                active['owned'].update({identity(p):p for p in ps})
                peak = max(peak,sum(p['rss'] for p in ps))
                require(peak <= manifest['rss_limit_bytes'], 'declared combined process RSS envelope exceeded')
                workers = [w for p in ps if p['pid'] != child.pid and (w := worker(p,binary))]
                try:
                    pipelines = api(manifest['port'],'pipelines')['data']
                    if not pipelines:
                        time.sleep(0.25)
                        continue
                    require(len(pipelines) == 1, 'isolated runtime must have exactlyone pipeline')
                    pipeline = pipelines[0]
                    jobs = api(manifest['port'],f"pipelines/{pipeline['id']}/jobs")['data']
                    if not jobs:
                        time.sleep(0.25)
                        continue
                    require(len(jobs) == 1, 'isolated runtime must have exactlyone job')
                    job = jobs[0]
                    result['last_job_state'] = job['state']
                    sequence = len(timeline)
                    write(case/'api'/f'{sequence:05}-pipeline.json',pipeline)
                    write(case/'api'/f'{sequence:05}-job.json',job)
                    timeline.append(dict(elapsed=time.monotonic()-start,phase=2 if fault else 1,job=job,workers=workers))
                    require(job['state'] != 'Failed','production job failed; see retained API/runtime evidence')
                    if job['state'] == 'Running' and fault is None:
                        startup_complete = True
                    if fault is None and job['state'] == 'Running' and len(workers) == 1:
                        checkpoints = api(manifest['port'],f"pipelines/{pipeline['id']}/jobs/{job['id']}/checkpoints")
                        write(case/'api'/f'{sequence:05}-checkpoints.json',checkpoints)
                        completed = [c for c in checkpoints['data'] if c['finish_time'] is not None]
                        if completed and (case/'outputs/raw.jsonl').exists():
                            original_catalog = catalog_snapshot(case,'before-loss')
                            require(original_catalog['checkpoints'], 'no durable finished checkpoint in initialcatalog')
                            ready = [c for c in original_catalog['checkpoints'] if c['state'] == 'ready' and c['finish_time'] is not None]
                            require(ready, 'no durable ready/finished checkpoint before fault')
                            selected = ready[0]
                            owner = workers[0]
                            proof = checkpoint((case/'checkpoints').resolve(),selected['epoch'],owner,'controller')
                            proof['committed_prefix'] = committed_prefix(case,proof,manifest['rows'],label='committed-before-loss')
                            retain_checkpoint(case,proof,'selected-checkpoint-before-loss')
                            require(original_catalog['job']['id'] == owner['job'] == job['id']
                                    and original_catalog['pipeline']['pub_id'] == owner['pipeline'] == pipeline['id']
                                    and original_catalog['job']['state'] == 'Running'
                                    and original_catalog['job']['run_id'] == owner['generation'] == job['run_id'],
                                    'initial catalog/API/worker identity or generation differs')
                            write(case/'checkpoint-before-loss.json',proof)
                            before_outputs = preserve_outputs(case,'before-loss')
                            parent = process(child.pid)
                            require(parent and identity(parent) in active['owned'] and parent['session'] == child.pid
                                    and parent['exe'] == str(binary) and parent['args'] == command and child.poll() is None, 'parent identity/ownership changed before fault')
                            current = worker(process(owner['pid']),binary)
                            require(current is not None and identity(current) == identity(owner)
                                    and owner['session'] == child.pid and owner['group'] == child.pid, 'worker changed/escaped before fault')
                            require(terminate_identity(parent,signal.SIGKILL), 'controller exited before SIGKILL delivery; no fault injected')
                            fault = dict(controller=parent,worker=owner,checkpoint=selected,elapsed=time.monotonic()-start,
                                         before_outputs=before_outputs,catalog_identity=catalog_identity(original_catalog))
                            write(case/'fault.json',fault)
                            # SIGKILL skips scheduler destructors. Tear down every
                            # identity-owned/adopted worker before identical relaunch.
                            child.wait(timeout=5)
                            signalled = []
                            for p in session_processes(child.pid):
                                if identity(p) in active['owned'] or p['parent'] == os.getpid():
                                    active['owned'][identity(p)] = p
                                    if p['state'] != 'Z' and terminate_identity(p,signal.SIGKILL):
                                        signalled.append(dict(pid=p['pid'],start=p['start']))
                            report = cleanup(child,active['owned'])
                            active['cleanup'] = report
                            require(report['complete'] and not report.get('errors'), 'first generation cleanup/reaping failed')
                            fault.update(worker_teardown_signals=signalled,first_top_level_exit=child.returncode,cleanup=report)
                            require(child.returncode == -signal.SIGKILL, 'controller did not terminate from injected SIGKILL')
                            write(case/'fault.json',fault)
                            # Copy raw DB sidecars only after all writers are gone;
                            # the read-only backup additionally gives a consistent oracle.
                            physical = []
                            for name in ('state.sqlite','state.sqlite-wal','state.sqlite-shm'):
                                source = case/'checkpoints'/name
                                if source.exists():
                                    destination = case/('post-loss.'+name)
                                    require(not destination.exists(), 'catalog sidecar evidence must be new')
                                    destination.write_bytes(source.read_bytes())
                                    physical.append(dict(file=destination.name,bytes=destination.stat().st_size,sha256=sha(destination)))
                            write(case/'catalog-physical-after-loss.json',physical)
                            persisted = catalog_snapshot(case,'after-loss')
                            require(catalog_identity(persisted) == catalog_identity(original_catalog), 'catalog identity changed across processloss')
                            require(persisted['job']['state'] == 'Running' and persisted['job']['run_id'] == owner['generation'], 'unexpected persisted job state/generation afterloss')
                            require(persisted['checkpoints'], 'no durable checkpoint after controllerloss')
                            latest = persisted['checkpoints'][0]
                            require(latest['state'] == 'committing' or latest['finish_time'] is not None,
                                    'selected ready checkpoint has no finish time')
                            recovery_proof = checkpoint((case/'checkpoints').resolve(),latest['epoch'],owner,'controller')
                            recovery_proof.update(catalog_state=latest['state'],catalog_checkpoint=latest,
                                committed_prefix=committed_prefix(case,recovery_proof,manifest['rows'],label='committed-for-restart'))
                            retain_checkpoint(case,recovery_proof,'selected-checkpoint-for-restart')
                            write(case/'checkpoint-for-restart.json',recovery_proof)
                            preserve_outputs(case,'after-loss-before-restart')
                            verify_manifest(case,manifest)
                            require(time.monotonic() < deadline, 'deadline exhausted before restart')
                            log = logs.enter_context((case/'runtime.restart.log').open('xb'))
                            child = subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT,env=env,cwd=case,start_new_session=True)
                            active = dict(child=child,owned={},cleanup=None,log='runtime.restart.log')
                            children.append(active)
                            require(child.pid != parent['pid'], 'restart must create a new parent PID')
                            phase_started = time.monotonic()
                            startup_complete = False
                            continue
                    elif fault is not None:
                        require(job['id'] == fault['worker']['job'] and pipeline['id'] == fault['worker']['pipeline'], 'restart created another pipeline/job')
                        if job['state'] == 'Running' and len(workers) == 1 and replacement is None:
                            require(job['run_id'] > fault['worker']['generation'], 'restart did not increase persisted generation')
                            candidate = workers[0]
                            require(candidate['job'] == fault['worker']['job'] and candidate['pipeline'] == fault['worker']['pipeline']
                                    and candidate['generation'] == job['run_id'] and identity(candidate) != identity(fault['worker'])
                                    and candidate['pid'] != fault['worker']['pid'], 'replacement worker ownership/identity/generation differs')
                            if not assert_controller_restore_log(case/'runtime.restart.log',candidate,recovery_proof):
                                time.sleep(0.25)
                                continue
                            resumed = catalog_snapshot(case,'restarted')
                            require(catalog_identity(resumed) == fault['catalog_identity'], 'restart catalog cluster/pipeline/job/program identity differs')
                            require(resumed['job']['run_id'] == candidate['generation'] and resumed['job']['state'] == 'Running', 'restart catalog/API/worker state differs')
                            current_parent = process(child.pid)
                            require(current_parent and current_parent['exe'] == str(binary) and current_parent['args'] == command
                                    and identity(current_parent) != identity(fault['controller'])
                                    and current_parent['session'] == child.pid, 'replacement parent identity/ELF/argv/ownership differs')
                            replacement = dict(worker=candidate,controller=current_parent,catalog=resumed,
                                               restored_epoch=recovery_proof['epoch'],source=recovery_proof['committed_prefix']['source'])
                            startup_complete = True
                            write(case/'replacement.json',replacement)
                        if job['state'] == 'Finished':
                            require(replacement is not None, 'no observed samejob controller/worker restart restoration')
                            actual,expected = list(rows(case/'outputs/raw.jsonl')),list(rows(case/'expected.raw.jsonl'))
                            require(len(actual) == len(expected), 'raw output cardinality differs')
                            require(all(type(row) is dict and set(row) == {'counter','subtask_index'} and all(type(v) is int for v in row.values()) for row in actual), 'raw output types/fields differ')
                            require(actual == expected, 'raw counter/order/value oracle differs')
                            aggregate = totals(case/'outputs/totals.jsonl',manifest['rows'],True)
                            for name in ('raw.jsonl','totals.jsonl'):
                                committed = case/('committed-for-restart.'+name)
                                with (case/'outputs'/name).open('rb') as final_output:
                                    require(final_output.read(committed.stat().st_size) == committed.read_bytes(), 'restored committed sink prefix changed')
                            finished = catalog_snapshot(case,'finished')
                            require(catalog_identity(finished) == fault['catalog_identity'] and finished['job']['state'] == 'Finished'
                                    and finished['job']['run_id'] == replacement['worker']['generation'], 'finished catalog identity/state/generation differs')
                            result.update(status='passed',raw_rows=len(actual),aggregate=aggregate,fault=fault,
                                          replacement=replacement,recovery_checkpoint=recovery_proof,finished_catalog=finished)
                            break
                except (urllib.error.URLError,TimeoutError,ConnectionError) as error:
                    result['last_api_error'] = f'{type(error).__name__}: {error}'
                time.sleep(0.25)
            require(result['status'] == 'passed', 'finite controller restart qualification deadline exceeded')
        except BaseException as error:
            result['error'] = f'{type(error).__name__}: {error}'
            tails = {}
            for name in ('runtime.log','runtime.restart.log'):
                path = case/name
                if path.exists():
                    with path.open('rb') as stream:
                        stream.seek(0,os.SEEK_END)
                        stream.seek(max(0,stream.tell()-16384))
                        tails[name] = stream.read().decode(errors='replace')
            result['runtime_log_tails'] = tails
            raise
        finally:
            reports = []
            for entry in children:
                if entry['cleanup'] is None:
                    try:
                        entry['cleanup'] = cleanup(entry['child'],entry['owned'])
                    except BaseException as error:
                        entry['cleanup'] = dict(complete=False,error=f'{type(error).__name__}: {error}')
                reports.append(dict(pid=entry['child'].pid,exit=entry['child'].returncode,log=entry['log'],report=entry['cleanup']))
            complete = bool(reports) and all(r['report']['complete'] and not r['report'].get('errors') for r in reports)
            result['cleanup'] = dict(complete=complete,phases=reports)
            if not complete:
                result.update(status='failed',cleanup_error='owned-session cleanup/reaping failed')
            result.update(elapsed_seconds=time.monotonic()-start,peak_combined_sampled_rss_bytes=peak,
                          rss_sampling_interval_seconds=0.25,output_hashes={p.name:sha(p) for p in (case/'outputs').iterdir() if p.is_file()})
            write(case/'timeline.json',timeline)
            write(case/'result.json',result)
    require(result['status'] == 'passed',result.get('error','qualification failed'))
    print(json.dumps(result,sort_keys=True))


def run(case, manifest):
    if manifest.get('fault_mode','worker') == 'controller':
        return run_controller(case,manifest)
    binary = verify_manifest(case,manifest)
    require(hasattr(os,'pidfd_open') and hasattr(signal,'pidfd_send_signal'),'Linux pidfd signal support is required')
    import pyarrow  # Required for independent committed source/sink prefix decoding.
    with socket.socket() as probe:
        probe.bind(('127.0.0.1',manifest['port']))
    env = {k:v for k,v in os.environ.items() if not k.startswith(('ARROYO__','STREAMR_TEST_','STREAMR_CAPTURE_'))}
    command = [str(binary),'--config',str(case/'config.yaml'),'run','--name','m3-process-recovery','--parallelism','1','--state-dir',str(case/'checkpoints'),str(case/'query.sql')]
    enable_subreaper()
    timeline, owned, fault, replacement, peak = [], {}, None, None, 0
    startup_complete = False
    start = time.monotonic()
    deadline = start+manifest['timeout_seconds']
    result = dict(status='failed', command=command, pyarrow_version=pyarrow.__version__)
    with (case/'runtime.log').open('wb') as log:
        child = subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT,env=env,cwd=case,start_new_session=True)
        try:
            while time.monotonic() < deadline:
                require(startup_complete or time.monotonic()-start < manifest['startup_timeout_seconds'],
                        f"production startup deadline exceeded; last jobstate={result.get('last_job_state','API unavailable')}; see runtime_log_tail/result/runtime.log")
                ps = session_processes(child.pid)
                owned.update({identity(p):p for p in ps})
                peak = max(peak,sum(p['rss'] for p in ps))
                require(peak <= manifest['rss_limit_bytes'], 'declared combined process RSS envelope exceeded')
                require(child.poll() is None, 'top-level process exited before qualification completed')
                workers = [w for p in ps if p['pid'] != child.pid and (w := worker(p,binary))]
                try:
                    pipelines = api(manifest['port'],'pipelines')['data']
                    if not pipelines:
                        time.sleep(0.25)
                        continue
                    require(len(pipelines) == 1, 'isolated runtime must have exactly one pipeline')
                    pipeline = pipelines[0]
                    jobs = api(manifest['port'],f"pipelines/{pipeline['id']}/jobs")['data']
                    if not jobs:
                        time.sleep(0.25)
                        continue
                    require(len(jobs) == 1, 'isolated runtime must have one job')
                    job = jobs[0]
                    result['last_job_state'] = job['state']
                    if job['state'] == 'Running':
                        startup_complete = True
                    sequence = len(timeline)
                    write(case/'api'/f'{sequence:05}-pipeline.json',pipeline)
                    write(case/'api'/f'{sequence:05}-job.json',job)
                    timeline.append(dict(elapsed=time.monotonic()-start,job=job,workers=workers))
                    require(job['state'] != 'Failed','production job failed; see API/runtime evidence')
                    if fault is None and job['state'] == 'Running' and len(workers) == 1:
                        checkpoints = api(manifest['port'],f"pipelines/{pipeline['id']}/jobs/{job['id']}/checkpoints")
                        write(case/'api'/f'{sequence:05}-checkpoints.json',checkpoints)
                        completed = [c for c in checkpoints['data'] if c['finish_time'] is not None]
                        raw = case/'outputs/raw.jsonl'
                        if completed and raw.exists():
                            # Only flushed complete raw records are inspected; checkpoint ownership
                            # and every referenced immutable object must verify before the signal.
                            complete = raw.read_bytes()
                            complete = complete[:complete.rfind(b'\n')+1]
                            prefix = [json.loads(line) for line in complete.splitlines()]
                            if 0 < len(prefix) < manifest['rows']:
                                selected = max(completed,key=lambda c:c['epoch'])
                                proof = checkpoint((case/'checkpoints').resolve(),selected['epoch'],workers[0],manifest['protocol'])
                                committed = committed_prefix(case, proof, manifest['rows'])
                                proof['committed_prefix'] = committed
                                for record in proof['artifacts']:
                                    source = case/'checkpoints'/record['path']
                                    destination = case/'selected-checkpoint'/record['path']
                                    destination.parent.mkdir(parents=True,exist_ok=True)
                                    destination.write_bytes(source.read_bytes())
                                    require(sha(destination) == record['sha256'],'checkpoint changed while retaining evidence')
                                write(case/'checkpoint-before-loss.json',proof)
                                for name in ('raw.jsonl','totals.jsonl'):
                                    source = case/'outputs'/name
                                    if source.exists():
                                        (case/('before-loss.'+name)).write_bytes(source.read_bytes())
                                target = workers[0]
                                current = worker(process(target['pid']),binary)
                                require(current is not None and identity(current) == identity(target) and current['generation'] == target['generation'], 'worker identity changed before fault')
                                require(target['session'] == child.pid and target['group'] == child.pid,'worker escaped owned session/group')
                                require(child.poll() is None,'controller exited before worker fault')
                                require(terminate_identity(target,signal.SIGKILL),'worker exited before SIGKILL delivery; no fault injected')
                                fault = dict(worker=target,checkpoint=selected,observed_raw_rows=len(prefix),elapsed=time.monotonic()-start)
                                write(case/'fault.json',fault)
                    if fault is not None:
                        candidates = [w for w in workers if identity(w) != identity(fault['worker']) and w['job'] == fault['worker']['job']
                                      and w['pipeline'] == fault['worker']['pipeline'] and w['generation'] > fault['worker']['generation']]
                        if candidates and replacement is None:
                            replacement = candidates[0]
                            write(case/'replacement.json',replacement)
                        if job['state'] == 'Finished':
                            require(replacement is not None,'no observed replacement worker generation')
                            actual = list(rows(case/'outputs/raw.jsonl'))
                            expected = list(rows(case/'expected.raw.jsonl'))
                            require(len(actual) == len(expected),'raw output cardinality differs')
                            require(all(type(row) is dict and set(row) == {'counter','subtask_index'} and all(type(v) is int for v in row.values()) for row in actual),'raw output types/fields differ')
                            require(actual == expected,'raw counter/order/value oracle differs')
                            aggregate = totals(case/'outputs/totals.jsonl',manifest['rows'],True)
                            result.update(status='passed',raw_rows=len(actual),aggregate=aggregate,fault=fault,replacement=replacement)
                            break
                except (urllib.error.URLError,TimeoutError,ConnectionError) as error:
                    # Startup/transient restart connectivity retries remain inside one deadline.
                    result['last_api_error'] = f'{type(error).__name__}: {error}'
                time.sleep(0.25)
            require(result['status'] == 'passed','finite production qualification deadline exceeded')
        except BaseException as error:
            result['error'] = f'{type(error).__name__}: {error}'
            with (case/'runtime.log').open('rb') as runtime_log:
                runtime_log.seek(0,os.SEEK_END)
                runtime_log.seek(max(0,runtime_log.tell()-16384))
                result['runtime_log_tail'] = runtime_log.read().decode(errors='replace')
            raise
        finally:
            try:
                report = cleanup(child,owned)
            except BaseException as error:
                report = dict(complete=False,error=f'{type(error).__name__}: {error}')
            result['cleanup'] = report
            if not report['complete']:
                result.update(status='failed',cleanup_error='owned-session cleanup/reaping failed')
            result.update(elapsed_seconds=time.monotonic()-start,peak_combined_sampled_rss_bytes=peak,
                          rss_sampling_interval_seconds=0.25,top_level_exit=child.returncode,
                          output_hashes={p.name:sha(p) for p in (case/'outputs').iterdir() if p.is_file()})
            write(case/'timeline.json',timeline)
            write(case/'result.json',result)
    require(result['status'] == 'passed',result.get('error','qualification failed'))
    print(json.dumps(result,sort_keys=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary',type=Path,required=True)
    parser.add_argument('--case',type=Path,required=True)
    parser.add_argument('--source-free-root',type=Path,required=True)
    parser.add_argument('--provenance',type=Path,required=True)
    parser.add_argument('--backend',choices=('memory','rocksdb'),required=True)
    parser.add_argument('--protocol',choices=('controller','worker'),required=True)
    parser.add_argument('--fault-mode',choices=('worker','controller'),default='worker')
    parser.add_argument('--rows',type=int,default=20000)
    parser.add_argument('--rate',type=float,default=100)
    parser.add_argument('--port',type=int,default=19180)
    parser.add_argument('--timeout',type=int,default=600)
    parser.add_argument('--startup-timeout',type=int,default=90)
    parser.add_argument('--rss-limit-mib',type=int,default=1024)
    parser.add_argument('--prepare-only',action='store_true')
    args = parser.parse_args()
    require(0 < args.port < 65536 and 0 < args.startup_timeout < args.timeout and args.rss_limit_mib > 0,'invalid port/timeout/RSS limit')
    case,manifest = prepare(args)
    if not args.prepare_only:
        try:
            run(case,manifest)
        except BaseException as error:
            if not (case/'result.json').exists():
                write(case/'result.json',dict(status='failed',stage='preflight/startup',
                                            error=f'{type(error).__name__}: {error}'))
            raise


if __name__ == '__main__':
    main()
