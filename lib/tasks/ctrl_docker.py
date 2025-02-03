import abc
from dataclasses import dataclass
import logging
from typing import Tuple

from lib.service.docker.config import ImageConfig, ContainerConfig
from lib.service.docker.service import DockerService
from lib.service.database import DatabaseService, DatabaseServiceImpl, DatabaseConfig

@dataclass
class DockerCtrlInstruction(abc.ABC):
    image: ImageConfig
    container: ContainerConfig

@dataclass
class DockerRestart(DockerCtrlInstruction):
    wait_for_database: bool

@dataclass
class DockerStart(DockerCtrlInstruction):
    wait_for_database: bool
    reinitialise_image: bool
    reinitialise_container: bool

@dataclass
class DockerStop(DockerCtrlInstruction):
    throw_if_not_found: bool
    drop_image: bool
    drop_container: bool

async def run_controller(instruction: DockerCtrlInstruction, db: DatabaseService, docker: DockerService):
    match instruction:
        case DockerRestart(container=c_conf, image=i_conf):
            image = docker.create_image(i_conf)
            container = docker.create_container(image, c_conf)
            await container.stop()
            await container.start()
            if instruction.wait_for_database:
                await db.wait_till_running()
        case DockerStart(container=c_conf, image=i_conf):
            if instruction.reinitialise_image:
                await docker.create_image(i_conf).nuke()

            image = docker.create_image(i_conf)
            await image.prepare()

            container = docker.create_container(image, c_conf)
            if not instruction.reinitialise_container:
               await container.clean()

            await container.prepare(db.config)
            await container.start()

            if instruction.wait_for_database:
                await db.wait_till_running()
        case DockerStop(container=c_conf, image=i_conf):
            image = docker.create_image(i_conf)
            container = docker.create_container(image, c_conf)

            await container.stop(throw_if_not_found=instruction.throw_if_not_found)

            if instruction.drop_container:
                await container.clean()

            if instruction.drop_image:
                await image.nuke()
        case other:
            raise TypeError(f'Not implemented {other}')


if __name__ == '__main__':
    import asyncio
    import argparse
    from lib.defaults import INSTANCE_CFG

    parser = argparse.ArgumentParser(description="db start stop tool")
    parser.add_argument('--nuke-image', action='store_true', default=False)
    parser.add_argument("--instance", type=int, required=True)
    parser.add_argument("--debug", action='store_true', default=False)

    subparsers = parser.add_subparsers(help='sub command help',
                                       dest='command',
                                       required=True)

    start_parser = subparsers.add_parser('start')
    start_parser.add_argument('--nuke-image', action='store_true', default=False)
    start_parser.add_argument('--nuke-container', action='store_true', default=False)
    start_parser.add_argument('--wait-for-database', action='store_true', default=False)

    start_parser = subparsers.add_parser('restart')
    start_parser.add_argument('--wait-for-database', action='store_true', default=False)

    stop_parser = subparsers.add_parser('stop')
    stop_parser.add_argument('--nuke-image', action='store_true', default=False)
    stop_parser.add_argument('--nuke-container', action='store_true', default=False)
    stop_parser.add_argument('--throw-if-not-found', action='store_true', default=False)

    args = parser.parse_args()


    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    instance_cfg = INSTANCE_CFG[args.instance]

    async def main(instruction: DockerCtrlInstruction):
        async with DockerService.create() as docker:
            db = DatabaseServiceImpl.create(instance_cfg.database, 1)
            await run_controller(instruction, db, docker)

    match args.command:
        case 'start':
            asyncio.run(main(DockerStart(
                image=instance_cfg.docker_image,
                container=instance_cfg.docker_container,
                wait_for_database=args.wait_for_database,
                reinitialise_image=args.nuke_image,
                reinitialise_container=args.nuke_container,
            )))
        case 'stop':
            asyncio.run(main(DockerStop(
                image=instance_cfg.docker_image,
                container=instance_cfg.docker_container,
                drop_image=args.nuke_image,
                drop_container=args.nuke_container,
                throw_if_not_found=args.throw_if_not_found
            )))
        case 'restart':
            asyncio.run(main(DockerRestart(
                image=instance_cfg.docker_image,
                container=instance_cfg.docker_container,
                wait_for_database=args.wait_for_database,
            )))

