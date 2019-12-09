#!/usr/bin/python3
import sys
import os
import re
from urllib.parse import unquote_plus
from pathlib import Path

KEYS = 'hostname port username password database'.split()

def parse_connect_url(url):
    """
    Valid inputs:

    postgresql://user:password@host:port/db
    postgresql://user@host/db
    postgresql://@/db

    password section is expected to be url encoded.
    """
    mm = re.match(("([^:]+)://([^:@/]+){0,1}(?::([^@]*)){0,1}@"
                   "(?:([^/:]+)(?::([0-9]+)){0,1}){0,1}/(.+)"), url)
    if mm is None:
        raise ValueError("Not a valid url: {}".format(url))

    _, user, password, host, port, db = mm.groups()
    oo = dict(hostname=host, database=db)

    if port is not None:
        oo['port'] = int(port)
    if password is not None:
        oo['password'] = unquote_plus(password)
    if user is not None:
        oo['username'] = user
    return oo


def render_dc_config(params, section_name='default'):
    oo = '[{}]\n'.format(section_name)
    for k in KEYS:
        v = params.get(k, None)
        if v is not None:
            oo += 'db_{k}: {v}\n'.format(k=k, v=v)
    return oo


def dump_help():
    print("""Usage:

   For automatic configuration do:
   > dc_config_render.py auto

   To generate and dump config to stdout do this:

   > dc_config_render.py 'postgresql://user:password@hostname:port/db'
   Examples:
        postgresql://@/db1                     -- local db via Unix socket
        postgresql://user:secret@somehost/db2  -- remote db""",
          file=sys.stderr)


def auto_config():
    """
    Render config to $DATACUBE_CONFIG_PATH or ~/.datacube.conf, but only if doesn't exist.

    option1:
      DATACUBE_DB_URL  postgresql://user:password@host/database

    option2:
      DB_{HOSTNAME|PORT|USERNAME|PASSWORD|DATABASE}

    option3:
       default config
    """
    cfg_path = os.environ.get('DATACUBE_CONFIG_PATH', None)
    cfg_path = Path(cfg_path) if cfg_path else Path.home()/'.datacube.conf'

    if cfg_path.exists():
        return cfg_path

    db_url = os.environ.get('DATACUBE_DB_URL', None)
    if db_url is None:
        params = {k: os.environ.get('DB_{}'.format(k.upper()), None)
                                    for k in KEYS}
        params = {k:v for k,v in params.items() if v is not None}
        if len(params) == 0:
            params = {'database' : 'datacube'}
    else:
        params = parse_connect_url(db_url)

    cfg_text = render_dc_config(params)
    with open(str(cfg_path), 'wt') as f:
        f.write(cfg_text)

    return cfg_path


if __name__ == '__main__':
    """ Two ways to run this:
         > dc_config_render.py auto
         ::: will write to file

         > dc_config_render.py postgresql://user:password@host/database
         :::: will output to stdout
    """

    if len(sys.argv) != 2:
        dump_help()
        sys.exit(2)

    _, url = sys.argv
    try:
        if url == 'auto':
            auto_config()
        else:
            print(render_dc_config(parse_connect_url(url)))
        sys.exit(0)
    except ValueError as e:
        print(str(e), file=sys.stderr)
        dump_help()
        sys.exit(1)
