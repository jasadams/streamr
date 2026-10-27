#!/usr/bin/env python3
"""Owned isolated real-Kafka transaction recovery qualification.

Run through scripts/rust-build; requires a scripts/build-pin source-free dev pin.
The frozen baseline directory is read-only evidence and is never changed."""
import argparse, json, os, re, subprocess, time
from pathlib import Path

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--case', type=Path, required=True)
    p.add_argument('--pin', type=Path, required=True)
    p.add_argument('--checkpoint-seconds', type=int, choices=[2], default=2)
    p.add_argument('--repo', type=Path, required=True)
    p.add_argument('--baseline', type=Path, required=True)
    p.add_argument('--fixture', type=Path, help='Optional generic fixture directory: config.json, query.sql, inputs.json, expected.json, manifest.json; query uses ${namespace} and ${broker}.')
    p.add_argument('--backend', choices=['memory', 'rocksdb'], default='rocksdb')
    p.add_argument('--scenario', choices=['liveness', 'before', 'after', 'expired', 'original-expired', 'checkpoint-expired', 'orphan'], default='liveness')
    p.add_argument('--supervisor', type=Path, default=Path(__file__).with_name('kafka-transaction-recovery-supervisor.py'))
    p.add_argument('--commit-delay-seconds', type=int, choices=[0, 3], default=0)
    p.add_argument('--broker-pause-seconds', type=int, choices=[0, 3], default=0)
    p.add_argument('--pyarrow', type=Path, required=True)
    p.add_argument('--image', default='localhost/streamr-str127-dev:latest')
    p.add_argument('--broker-image', default='docker.io/redpandadata/redpanda:v24.3.7')
    a = p.parse_args()
    assert os.environ.get('RUST_BUILD_QUEUE_HELD'), 'shared queue required'
    case = a.case.resolve()
    case.mkdir(parents=True, exist_ok=False)
    commands = []

    def write(name, obj):
        (case / name).write_text(json.dumps(obj, indent=2, sort_keys=True) + '\n')

    def run(argv, **kw):
        r = subprocess.run(argv, text=True, capture_output=True, **kw)
        commands.append(dict(command=argv, exit=r.returncode, stdout=r.stdout, stderr=r.stderr))
        write('host-commands.json', commands)
        assert r.returncode == 0, (argv, r.stderr)
        return r.stdout.strip()
    pin = a.pin.resolve()
    binary = pin / 'package/bin/arroyo'
    receipt = json.loads((pin / 'source-manifest.json').read_text())
    import hashlib
    with binary.open('rb') as stream:
        binary_digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    assert binary_digest == receipt['binary_sha256']
    assert receipt['build_profile'] == 'dev', 'qualification requires normal dev pin'
    for name, digest in receipt['package_files'].items():
        with (pin / 'package' / name).open('rb') as stream:
            assert hashlib.file_digest(stream, 'sha256').hexdigest() == digest
    for key, name in [('image_native_receipt_sha256', 'image-native-receipt.json'), ('runtime_linkage_receipt_sha256', 'runtime-linkage.json')]:
        assert hashlib.sha256((pin / name).read_bytes()).hexdigest() == receipt[key]
    image = json.loads(run(['podman', 'image', 'inspect', a.image]))[0]
    assert image['Id']==receipt['runtime_image'], 'runtime image differs from preserved pin'
    broker_image = json.loads(run(['podman', 'image', 'inspect', a.broker_image]))[0]
    write('source.json', dict(pin=receipt, image=image, broker_image=broker_image, supervisor_sha256=hashlib.sha256(a.supervisor.read_bytes()).hexdigest(), wrapper_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), helper_sha256=hashlib.sha256((a.repo / 'scripts/test-native-process-recovery.py').read_bytes()).hexdigest()))
    suffix = case.name.lower()
    assert re.fullmatch('[a-z0-9-]+', suffix)
    prefix = 'str146-' + suffix
    network = prefix + '-net'
    broker = prefix + '-broker'
    engine = prefix + '-engine'
    for kind, name in [('network', network), ('container', broker), ('container', engine)]:
        assert subprocess.run(['podman', kind, 'exists', name], capture_output=True).returncode == 1, 'preexisting resource ' + name
    network_id = broker_id = None
    broker_volumes = []
    cidfile = case / 'engine.cid'
    result = {'status': 'failed'}
    broker_paused = False
    engine_process = None
    try:
        created = run(['podman', 'network', 'create', network])
        ni = json.loads(run(['podman', 'network', 'inspect', created]))[0]
        network_id = ni.get('id', ni.get('Id'))
        assert network_id
        write('network.json', ni)
        broker_id = run(['podman', 'run', '-d', '--name', broker, '--network', network, '--network-alias', 'broker', '--memory', '2G', '--entrypoint', '/usr/bin/rpk', broker_image['Id'], 'redpanda', 'start', '--mode=dev-container', '--smp=1', '--memory=512M', '--reserve-memory=0M', '--overprovisioned', '--kafka-addr=internal://0.0.0.0:9092', '--advertise-kafka-addr=internal://broker:9092'])
        assert re.fullmatch('[a-f0-9]{64}', broker_id)
        broker_inspect=json.loads(run(['podman','container','inspect',broker_id]))[0]
        write('broker-container.json',broker_inspect)
        broker_volumes=[mount['Name'] for mount in broker_inspect.get('Mounts',[]) if mount.get('Type')=='volume']
        assert all(re.fullmatch('[a-f0-9]{64}', name) for name in broker_volumes), 'expected image-created anonymous broker volumes'
        write('owned-broker-volumes.json',dict(container_id=broker_id,volumes=broker_volumes))
        for _ in range(90):
            if subprocess.run(['podman', 'exec', broker_id, 'rpk', 'cluster', 'info', '-X', 'brokers=localhost:9092'], capture_output=True).returncode == 0:
                break
            time.sleep(1)
        else:
            raise RuntimeError('owned broker startup failed')
        run(['podman', 'cp', broker_id + ':/opt/redpanda/libexec/rpk', str(case / 'rpk')])
        with (case/'rpk').open('rb') as stream:
            write('broker-rpk.json',dict(broker_image_id=broker_image['Id'],binary_sha256=hashlib.file_digest(stream,'sha256').hexdigest(),describe_schema_source='https://github.com/redpanda-data/redpanda/blob/v24.3.7/src/go/rpk/pkg/cli/cluster/txn/describe.go'))
        argv = ['podman', 'run', '--rm', '--cidfile', str(cidfile), '--name', engine, '--network', network, '--security-opt', 'label=disable', '--memory', '2G', '-v', str(case) + ':' + str(case) + ':rw', '-v', str(pin) + ':' + str(pin) + ':ro', '-v', str(a.supervisor.resolve()) + ':/runner/supervisor.py:ro', '-v', str(a.repo.resolve() / 'scripts/test-native-process-recovery.py') + ':/runner/helper.py:ro', '-v', str(a.baseline.resolve()) + ':/runner/baseline:ro', '-v', str(a.pyarrow.resolve()) + ':' + str(a.pyarrow.resolve()) + ':ro', '-e', 'RUST_BUILD_QUEUE_HELD=' + os.environ['RUST_BUILD_QUEUE_HELD'], '-e', 'PYTHONDONTWRITEBYTECODE=1', '-e', 'PYTHONPATH=' + str(a.pyarrow.resolve())]
        if a.fixture:
            argv += ['-v', str(a.fixture.resolve()) + ':/runner/fixture:ro']
        libraries = [str(pin / 'package/lib')]
        argv += ['-e', 'LD_LIBRARY_PATH=' + ':'.join(libraries), image['Id'], 'python3', '/runner/supervisor.py', '--binary', str(binary), '--helper', '/runner/helper.py', '--baseline', '/runner/baseline', '--scenario', a.scenario, '--case', str(case), '--rpk', str(case / 'rpk'), '--namespace', prefix, '--checkpoint-seconds', str(a.checkpoint_seconds)]
        argv += ['--backend', a.backend]
        if a.fixture:
            argv += ['--fixture', '/runner/fixture']
        argv += ['--commit-delay-seconds', str(a.commit_delay_seconds), '--broker-pause-seconds', str(a.broker_pause_seconds)]
        write('inner-command.json', argv)
        with (case / 'container.log').open('w') as out:
            engine_process = subprocess.Popen(argv, stdout=out, stderr=subprocess.STDOUT)
            deadline = time.monotonic() + 1200
            pause_started = None
            while engine_process.poll() is None:
                assert time.monotonic() < deadline, 'finite engine timeout'
                if (case / 'broker-pause-request.json').exists() and pause_started is None:
                    run(['podman','pause',broker_id])
                    broker_paused = True
                    pause_started = time.monotonic()
                    write('broker-paused.json',dict(time_ns=time.time_ns(),monotonic_ns=time.monotonic_ns(),broker_id=broker_id,requested_seconds=a.broker_pause_seconds))
                if broker_paused and time.monotonic()-pause_started >= a.broker_pause_seconds:
                    run(['podman','unpause',broker_id])
                    broker_paused = False
                    write('broker-resumed.json',dict(time_ns=time.time_ns(),monotonic_ns=time.monotonic_ns(),broker_id=broker_id))
                time.sleep(.05)
            result['inner_exit']=engine_process.returncode
            assert engine_process.returncode==0,'inner proof failed; inspect container.log and runtime logs'
        result['status'] = 'passed'
    finally:
        cleanup = []
        if broker_paused:
            resumed=subprocess.run(['podman','unpause',broker_id],capture_output=True,text=True)
            cleanup.append(dict(time_ns=time.time_ns(),resource=broker_id,action='unpause',exit=resumed.returncode))
        if broker_id:
            logs = subprocess.run(['podman', 'logs', broker_id], capture_output=True, text=True)
            (case / 'broker.log').write_text(logs.stdout + logs.stderr)
        if cidfile.exists():
            cid = cidfile.read_text().strip()
            assert re.fullmatch('[a-f0-9]{64}', cid)
            if subprocess.run(['podman', 'container', 'exists', cid], capture_output=True).returncode == 0:
                r = subprocess.run(['podman', 'rm', '--force', cid], capture_output=True, text=True)
                cleanup.append(dict(time_ns=time.time_ns(), resource=cid, exit=r.returncode))
        if broker_id:
            r = subprocess.run(['podman', 'rm', '--force', '--volumes', broker_id], capture_output=True, text=True)
            cleanup.append(dict(time_ns=time.time_ns(), resource=broker_id, exit=r.returncode))
        if network_id:
            r = subprocess.run(['podman', 'network', 'rm', network_id], capture_output=True, text=True)
            cleanup.append(dict(time_ns=time.time_ns(), resource=network_id, exit=r.returncode))
        if engine_process and engine_process.poll() is None:
            try:engine_process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                engine_process.kill();engine_process.wait()
        for row in cleanup:
            if row.get('action')=='unpause':continue
            kind='network' if row['resource']==network_id else 'container'
            probe=subprocess.run(['podman',kind,'exists',row['resource']],capture_output=True)
            row['absence_exit']=probe.returncode
        volume_cleanup=[]
        for name in broker_volumes:
            probe=subprocess.run(['podman','volume','exists',name],capture_output=True,text=True)
            volume_cleanup.append(dict(time_ns=time.time_ns(),resource=name,kind='volume',absence_exit=probe.returncode,stderr=probe.stderr))
        result['volume_cleanup']=volume_cleanup
        result['cleanup'] = cleanup
        write('outer-result.json', result)
        assert all((row['exit']==0 and row.get('absence_exit',1)==1 for row in cleanup)), 'owned cleanup failed'
        assert all(row['absence_exit']==1 for row in volume_cleanup), 'owned broker volume cleanup failed'
    assert result['status'] == 'passed'
if __name__ == '__main__':
    main()
