from os.path import basename
from lib.service.database import DatabaseService
from lib.service.io import IoService

from .controller import SchemaController
from .discovery import SchemaDiscovery

_ROOT_DIR = './sql'

def create(io: IoService, db: DatabaseService) -> SchemaController:
    discovery = SchemaDiscovery.create(io, basename(_ROOT_DIR))
    controller = SchemaController(io, db, discovery)
    return controller


