import logging
from imessage_extractor.src.helpers.verbosity import path, bold
from imessage_extractor.src.helpers.utils import listfiles, ensurelist, duplicated
from os.path import dirname, basename, join, isdir, isfile, splitext


class Attribute():
    pass


class WorkflowConfig(object):
    """
    Store configuration variables for iMessage Extractor workflow. `params` is a dictionary
    of configuration variables, such that each key: value pair will be stored as a class
    attribute in an instantiation of WorkflowConfig.
    """
    def __init__(self, params: dict, logger: logging.Logger) -> None:
        self.logger = logger
        self.__dict__.update(params)

        self.sqlite_schema = 'main'

        self.dir = Attribute()
        self.file = Attribute()

        self.dir.home = dirname(dirname(__file__))
        assert basename(self.dir.home) == 'src'
        self.logger.debug(f'Home directory set at {path(self.dir.home)}')

        self.dir.chatdb = join(self.dir.home, 'chatdb')
        self.file.chatdb_table_info = join(self.dir.chatdb, 'chatdb_table_info.json')

        self.dir.static_tables = join(self.dir.home, 'static_tables')
        self.file.static_table_info = join(self.dir.static_tables, 'static_table_info.json')
        self.dir.static_table_data = join(self.dir.static_tables, 'data')
        self.file.static_table_csv = listfiles(path=self.dir.static_table_data, full_names=True, ext='.csv')

        self.dir.helpers = join(self.dir.home, 'helpers')
        self.dir.qc = join(self.dir.home, 'quality_control')
        self.dir.qc_views = join(self.dir.qc, 'views')

        self.dir.staging = join(self.dir.home, 'staging')
        self.dir.staging_python = join(self.dir.staging, 'python_definitions')
        self.dir.staging_sql = join(self.dir.staging, 'sql_definitions')

        self.file.staging_sql = listfiles(path=self.dir.staging_sql, full_names=True, ext='.sql')
        self.file.staging_sql_info = join(self.dir.staging, 'staging_sql_info.json')

        self.file.staging_python = listfiles(path=self.dir.staging_python, full_names=True, ext='.py')
        self.file.staging_python_info = join(self.dir.staging, 'staging_python_info.json')

        self._files_and_directories_exist()
        self._no_duplicate_object_names()

    def _files_and_directories_exist(self):
        """
        Make sure each directory and file exists.
        """
        for item in [d for d in dir(self.dir) if not d.startswith('_')]:
            thing = getattr(self.dir, item)
            thing = ensurelist(thing)
            for subthing in thing:
                if subthing != self.dir.home:
                    if not isdir(subthing):
                        raise FileNotFoundError(f'Directory {path(subthing)} is expected but does not exist')
                    else:
                        self.logger.debug(f'Validated existence of directory {path(subthing.replace(self.dir.home + "/", ""))}')

        for item in [f for f in dir(self.file) if not f.startswith('_')]:
            thing = getattr(self.file, item)
            thing = ensurelist(thing)
            for subthing in thing:
                if not isfile(subthing):
                    raise FileNotFoundError(f'File {path(subthing)} is expected but does not exist')
                else:
                    self.logger.debug(f'Validated existence of file {path(subthing.replace(self.dir.home + "/", ""))}')

        self.logger.info('All required configured paths exist', arrow='black')
        self.logger.info('Loaded workflow configuration', arrow='black')

    def _no_duplicate_object_names(self):
        """
        Make sure there are no duplicate table or view names.
        """
        tables_and_views_fpaths = self.file.static_table_csv + self.file.staging_sql + self.file.staging_python

        tables_and_views = [splitext(basename(f))[0] for f in tables_and_views_fpaths]

        dup_ind = duplicated(tables_and_views)

        table_and_view_dups = [x for i, x in enumerate(tables_and_views) if dup_ind[i] == True]
        table_and_view_dup_fpaths = [x for x in tables_and_views if splitext(basename(x))[0] in table_and_view_dups]

        if len(table_and_view_dups):
            raise ValueError(f'Found duplicate table/view files: {bold(table_and_view_dup_fpaths)}')

        self.logger.debug('No duplicate table/view definitions found')
