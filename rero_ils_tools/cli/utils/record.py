import click
from flask.cli import with_appcontext

from rero_ils.modules.utils import get_record_class_from_schema_or_pid_type


@click.command('resolve_uuid')
@click.option('-i', '--id',  default=None)
@click.option('-f', '--file', type=click.File('r'), default=None)
@click.option('-t', '--pid-type', default='doc')
@click.option('-v', '--verbose', is_flag=True, default=False)
@with_appcontext
def resolve_uuid(id, file, pid_type, verbose):
    """Resolve Persistent identifier."""
    try:
        record_class = get_record_class_from_schema_or_pid_type(
            pid_type=pid_type)
        if verbose:
            print(f'record_class: {pid_type}')
        if file:
            click.secho('Reading file to resolve uuid ...', fg='green')
            pids = []
            for id_ in file:
                id_ = id_.strip()
                pid = record_class.get_pid_by_id(id_.strip())
                pids.append(pid)
                if verbose:
                    click.secho(f'{id_}\t{pid}', fg='yellow')
        if id:
            pid = record_class.get_pid_by_id(id)
            click.secho(f'{id}\t{pid}', fg='yellow')
    except Exception as e:
        raise e
