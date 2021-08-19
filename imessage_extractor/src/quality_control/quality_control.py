import logging
from pydoni import listfiles, Postgres
from ..helpers.config import WorkflowConfig
from os.path import splitext, basename
from ..helpers.verbosity import bold


def create_qc_views(pg: Postgres, cfg: WorkflowConfig, logger: logging.Logger) -> None:
    """
    Execute quality control view definitions.
    """
    view_fpaths = listfiles(cfg.dir.qc_views, ext='.sql', full_names=True)

    for view_fpath in view_fpaths:
        vw_name = splitext(basename(view_fpath))[0]
        sql = open(view_fpath, 'r').read().format(pg_schema=cfg.pg_schema)
        pg.execute(sql)
        logger.info(f'Defined view "{bold(vw_name)}"', arrow='green')
