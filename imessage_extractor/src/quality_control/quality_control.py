import logging
from imessage_extractor.src.helpers.config import WorkflowConfig
from imessage_extractor.src.helpers.utils import listfiles
from imessage_extractor.src.helpers.verbosity import bold, code
from os.path import splitext, basename
from sql_query_tools import Postgres


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


def run_quality_control(pg: Postgres, cfg: WorkflowConfig, logger: logging.Logger) -> None:
    """
    Query each QC view and check for any data integrity issues.
    """
    vw_names = [splitext(basename(f))[0] for f in listfiles(cfg.dir.qc_views, ext='.sql')]

    for vw_name in vw_names:
        # Validate the view was successfully defined
        if not pg.view_exists(cfg.pg_schema, vw_name):
            raise Exception(f'View "{bold(vw_name)}" expected, but does not exist in schema "{cfg.pg_schema}"')

        qc_df = pg.read_table(cfg.pg_schema, vw_name)

        if len(qc_df):
            # Some QC issues to report
            logger.warning(f'QC issues found in "{bold(vw_name)}"!')

            if vw_name == 'qc_duplicate_chat_identifier_defs':
                logger.warning("""The following `chat_identifier` values
                are mapped to multiple names/sources, and only 1 is allowed. Please
                check the `chat_identifier` in each source, and make sure it is only
                mapped to one value of `contact_name` in one source.""")

                chat_ids = qc_df['chat_identifier'].unique()
                for chat_id in chat_ids:
                    mapped_names = qc_df[qc_df['chat_identifier'] == chat_id]['contact_name'].tolist()
                    mapped_sources = qc_df[qc_df['chat_identifier'] == chat_id]['source'].tolist()

                    mappings = [f'name: "{name}" (source: "{source}")' for name, source in zip(mapped_names, mapped_sources)]
                    mappings_str = ' | '.join(mappings)

                    logger.warning(f'Chat Identifier {bold(chat_id)} mapped to: {mappings_str}', arrow='yellow')

            elif vw_name == 'qc_missing_contact_names':
                logger.warning('Unmapped `chat_identifier` values:')
                for chat_id in qc_df['chat_identifier'].unique():
                    logger.warning(chat_id, arrow='yellow')

            elif vw_name == 'qc_null_flags':
                logger.warning(
                    f"""{len(qc_df)} records found with one or more flag columns as
                    null (should be either True or False). Check {bold(vw_name)} for
                    more information.
                    """)

            elif vw_name == 'qc_duplicate_message_id':
                logger.warning(
                    f"""{len(qc_df)} duplicate {code('message_id')} values found in {bold('message_vw')}
                    """)

            elif vw_name == 'qc_message_special_types':
                logger.warning(
                    f"""{len(qc_df)} missing {code('message_special_type')} values found in {bold('message_vw')}
                    """)

        else:
            # No QC issues to report
            logger.info(f'No QC issues found in "{bold(vw_name)}"')
