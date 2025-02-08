from os.path import basename
from lib.service.database import DatabaseService
from lib.service.io import IoService

from .controller import SchemaController
from .reader import SchemaReader

_ROOT_DIR = './sql'

def create(io: IoService, db: DatabaseService) -> SchemaController:
    reader = SchemaReader.create(io, basename(_ROOT_DIR))
    controller = SchemaController(io, db, reader)
    return controller


