# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017

import subprocess
import io
import json
import os
import tempfile
"""
IBM Streams utilities using `streamtool`.

Requires a local IBM Streams installation
located by the environment varible `STREAMS_INSTALL`.
"""

if 'STREAMS_INSTALL' in os.environ:
    _install = os.environ['STREAMS_INSTALL']
    _has_local_install = True
else:
    _has_local_install = False


def get_rest_api():
    """
    Get the root URL for the IBM Streams REST API.
    """
    assert _has_local_install

    url=[]
    ok = _run_st(['geturl', '--api'], lines=url)
    if not ok:
        raise ChildProcessError('streamtool geturl')
    return url[0]

def _submit_bundle(bundle, job_config=None, domain_id=None, instance_id=None):
    assert _has_local_install

    jo = tempfile.NamedTemporaryFile(delete=False)
    jo.close()

    args = ['submitjob']
    if domain_id:
        args.append('--domain-id')
        args.append(domain_id)
    if instance_id:
        args.append('--instance-id')
        args.append(instance_id)
    args.append('--outfile')
    args.append(jo.name)
    jcf = None
    if job_config:
        with tempfile.NamedTemporaryFile(mode='w+t', delete=False) as fp:
            json.dump(job_config.as_overlays(), fp)
            jcf = fp.name
        args.append('--jobConfig')
        args.append(jcf)

    args.append(bundle)
    rc = _run_st(args)
    if jcf:
        os.remove(jcf)
    if rc:
        with open(jo.name, 'r') as fp:
            job_id = str(fp.read()).strip()
        os.remove(jo.name)
        return job_id
    os.remove(jo.name)
    raise RuntimeError('streamtool submitjob failed for: ' + bundle)

def _cancel_job(job_id, force, domain_id=None, instance_id=None):
    assert _has_local_install

    args = ['canceljob']
    if domain_id:
        args.append('--domain-id')
        args.append(domain_id)
    if instance_id:
        args.append('--instance-id')
        args.append(instance_id)
    if force:
        args.append('--force')
    args.append(str(job_id))

    return _run_st(args)

def _run_st(args, lines=None):
    args.insert(0, os.path.join(_install, 'bin', 'streamtool'))
    process = subprocess.Popen(args, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
    process.stdin.close()
    while True:
        line = process.stdout.readline()
        if len(line) == 0:
            process.stdout.close()
            break
        line = line.decode("utf-8").strip()
        if lines is not None:
            lines.append(line)
    process.wait()
    return process.returncode == 0
