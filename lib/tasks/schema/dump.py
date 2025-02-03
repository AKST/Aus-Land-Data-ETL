from datetime import datetime
import logging
import os
import subprocess
import sys

from lib.service.docker.service import DockerService
from lib.service.database import *
from lib.tasks.ctrl_docker import run_controller, DockerStart

_DIR = '_out_pgdump'
_LOGGER = logging.getLogger(__name__)

def dump_file_name() -> tuple[str, str]:
    try:
        git_hash = subprocess\
            .check_output(['git', 'rev-parse', 'HEAD'])\
            .strip().decode('utf-8')[:7]
    except subprocess.CalledProcessError:
        git_hash = "nogit"

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dump_name = f'{timestamp}_{git_hash}.dump'
    return f'/home/dumps/{dump_name}', f'{_DIR}/{dump_name}'

def run_export(container_name: str,
               db_cfg: DatabaseConfig,
               workers: int):
    container_fname, local_fname = dump_file_name()
    command: list[str] = [
        'docker', 'exec', '-t', container_name,
        'pg_dump', '-U', db_cfg.user, '-d', db_cfg.dbname,
        '-F', 'c', '-f', container_fname,
    ]

    env = { **os.environ.copy(), 'PGPASSWORD': db_cfg.password }

    try:
        subprocess.run(command, check=True, env=env, stdout=sys.stdout, stderr=sys.stderr)
        _LOGGER.info(f"Backup successful: {local_fname}")
    except subprocess.CalledProcessError as e:
        _LOGGER.error(f"Backup export failed")
        _LOGGER.exception(e)

async def run_import(backup_name: str,
                     container_name: str,
                     db_cfg: DatabaseConfig,
                     docker_start: DockerStart,
                     workers: int) -> None:

    async with DockerService.create() as docker:
        db = DatabaseServiceImpl.create(db_cfg, 1)
        await run_controller(docker_start, db, docker)
        await db.close()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    container_backup_path = f"/tmp/{timestamp}-{os.path.basename(backup_name)}"
    subprocess.run(['docker', 'cp', backup_name, f"{container_name}:{container_backup_path}"], check=True)

    command: list[str] = [
        'docker', 'exec', '-t', container_name,
        'pg_restore', '-U', db_cfg.user, '-d', db_cfg.dbname,
        '--clean', '--if-exists', container_backup_path, '-j', str(workers),
    ]

    env = { **os.environ.copy(), 'PGPASSWORD': db_cfg.password }

    try:
        subprocess.run(command, check=True, env=env, stdout=sys.stdout, stderr=sys.stderr)
        _LOGGER.info(f"Restore successful: {backup_name} -> {db_cfg.dbname}")
    except subprocess.CalledProcessError as e:
        _LOGGER.error(f"Backup import failed")
        _LOGGER.exception(e)

if __name__ == '__main__':
    import argparse
    import asyncio

    from lib.defaults import INSTANCE_CFG
    from lib.utility.logging import config_vendor_logging, config_logging

    parser = argparse.ArgumentParser(description="export/import")
    parser.add_argument('--debug', action='store_true', default=False)
    parser.add_argument('--workers', type=int, default=8)

    command = parser.add_subparsers(dest='command')
    e_parser = command.add_parser('export')
    e_parser.add_argument('--instance', type=int, required=True)

    i_parser = command.add_parser('import')
    i_parser.add_argument("--backup", required=True)
    i_parser.add_argument('--instance', type=int, required=True)

    args = parser.parse_args()

    config_vendor_logging(set())
    config_logging(worker=None, debug=args.debug)

    match args.command:
        case 'export':
            instance = INSTANCE_CFG[args.instance]
            run_export(
                instance.docker_container.container_name,
                instance.database,
                workers=args.workers,
            )

        case 'import':
            instance = INSTANCE_CFG[args.instance]
            asyncio.run(run_import(
                args.backup,
                instance.docker_container.container_name,
                instance.database,
                DockerStart(
                    image=instance.docker_image,
                    container=instance.docker_container,
                    wait_for_database=True,
                    reinitialise_image=False,
                    reinitialise_container=True,
                ),
                workers=args.workers,
            ))
