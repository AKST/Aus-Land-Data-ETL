from os.path import basename
import re
from lib.service.database import DatabaseService
from lib.service.io import IoService

from .controller import SchemaController
from .file_discovery import FileDiscovery, create_file_regex
from .reader import SchemaReader

_ROOT_DIR = './sql'

def create(io: IoService, db: DatabaseService) -> SchemaController:
    pattern = create_file_regex(basename(_ROOT_DIR))
    file_discovery = FileDiscovery(io, pattern, basename(_ROOT_DIR))
    return SchemaController(io, db, SchemaReader(file_discovery, io))

