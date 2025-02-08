from os.path import basename
import re
from lib.service.database import DatabaseService
from lib.service.io import IoService

from .controller import SchemaController
from .file_discovery import FileDiscovery
from .reader import SchemaReader

_ROOT_DIR = './sql'

def create_file_regex(root_dir: str) -> re.Pattern:
    path_root = re.escape(root_dir)
    path_ns = r'(?P<ns>[_a-zA-Z][_a-zA-Z0-9]*)'
    path_file = r'(?P<step>\d{3})_APPLY(_(?P<name>[_a-zA-Z][_a-zA-Z0-9]*))?.sql'
    return re.compile(rf'^{path_root}/{path_ns}/schema/{path_file}$')

def create(io: IoService, db: DatabaseService) -> SchemaController:
    pattern = create_file_regex(basename(_ROOT_DIR))
    file_discovery = FileDiscovery(io, pattern, basename(_ROOT_DIR))
    reader = SchemaReader(file_discovery, io)
    controller = SchemaController(io, db, reader)
    return controller


