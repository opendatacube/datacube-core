"""
  This app launches workers for distributed work loads
"""

import click

KNOWN_WORKER_TYPES = ['distributed', 'dask', 'celery']


def parse_executor_opt(ctx, param, value):
    from datacube.ui.click import parse_endpoint
    (ex_type, host_port) = value
    if ex_type is None:
        ctx.fail('Need to provide valid --executor argument')

    try:
        host, port = parse_endpoint(host_port)
    except ValueError:
        ctx.fail('Expected host:port, got `{}`'.format(host_port))

    return ex_type, host, port


def launch_celery_worker(host, port, nprocs, password=''):
    from datacube import _celery_runner as cr
    cr.launch_worker(host, port, password=password, nprocs=nprocs)


def launch_distributed_worker(host, port, nprocs, nthreads=1):
    import subprocess

    addr = '{}:{}'.format(host, port)
    dask_worker = ['dask-worker', addr,
                   '--nthreads', str(nthreads)]

    if nprocs > 0:
        dask_worker.extend(['--nprocs', str(nprocs)])

    subprocess.check_call(dask_worker)


@click.command(name='worker')
@click.option('--executor', type=(click.Choice(KNOWN_WORKER_TYPES), str),  # type: ignore
              help="(distributed|dask(alias for distributed)|celery) host:port",
              default=(None, None),
              callback=parse_executor_opt)
@click.option('--nprocs', type=int, default=0, help='Number of worker processes to launch')
def main(executor, nprocs):
    launchers = dict(celery=launch_celery_worker,
                     dask=launch_distributed_worker,
                     distributed=launch_distributed_worker)
    ex_type, host, port = executor
    return launchers[ex_type](host, port, nprocs)


if __name__ == '__main__':
    main()
