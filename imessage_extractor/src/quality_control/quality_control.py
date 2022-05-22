import logging
from imessage_extractor.src.chatdb.chatdb import ChatDb
from imessage_extractor.src.helpers.config import WorkflowConfig
from imessage_extractor.src.helpers.utils import listfiles
from imessage_extractor.src.helpers.verbosity import code, path
from os.path import splitext, basename


def create_qc_views(chatdb: 'ChatDb', cfg: WorkflowConfig, logger: logging.Logger) -> None:
    """
    Execute quality control view definitions.
    """
    view_fpaths = listfiles(cfg.dir.qc_views, ext='.sql', full_names=True)

    for view_fpath in view_fpaths:
        view_name = splitext(basename(view_fpath))[0]
        logger.debug(f'Defining view {code(view_name)}')
        sql = open(view_fpath, 'r').read()
        chatdb.execute(sql)
        logger.info(f'Defined view {code(view_name)}', arrow='black')


def run_quality_control(chatdb: 'ChatDb', cfg: WorkflowConfig, logger: logging.Logger) -> None:
    """
    Query each QC view and check for any data integrity issues.
    """
    logger.info('Checking data integrity...', bold=True, arrow='black')

    vw_names = [splitext(basename(f))[0] for f in listfiles(cfg.dir.qc_views, ext='.sql')]
    total_warnings = 0

    for view_name in vw_names:
        # Validate the view was successfully defined
        if not chatdb.view_exists(view_name):
            raise Exception(f'View {code(view_name)} expected, but does not exist in SQLite')

        qc_df = chatdb.read_table(view_name)

        if len(qc_df):
            # At least one data integrity issue to report
            total_warnings += 1

            if view_name == 'qc_duplicate_chat_identifier_defs':
                logger.warning(f"""The following {code('chat_identifier')} values
                are mapped to multiple names/sources, and only 1 is allowed. Please
                check the {code('chat_identifier')} in each source, and make sure it is only
                mapped to one value of {code('contact_name')} in one source.""", arrow='black')

                chat_ids = qc_df['chat_identifier'].unique()
                for chat_id in chat_ids:
                    mapped_names = qc_df[qc_df['chat_identifier'] == chat_id]['contact_name'].tolist()
                    mapped_sources = qc_df[qc_df['chat_identifier'] == chat_id]['source'].tolist()

                    mappings = [f'name: "{name}" (source: "{source}")' for name, source in zip(mapped_names, mapped_sources)]
                    mappings_str = ' | '.join(mappings)

                    logger.warning(f'Chat Identifier {code(chat_id)} mapped to: {mappings_str}', arrow='yellow', indent=1)

            elif view_name == 'qc_missing_contact_names':
                logger.warning(f'Unmapped {code("chat_identifier")} value(s) found, please check {code(view_name)}', arrow='yellow')

            elif view_name == 'qc_null_flags':
                logger.warning(
                    f"""{len(qc_df)} records found with one or more flag columns as
                    null (should be either True or False). Check {code(view_name)} for
                    more information.""", arrow='yellow', indent=1)

            elif view_name == 'qc_duplicate_message_id':
                logger.warning(f"{len(qc_df)} duplicate {code('message_id')} values found in {code('message_user')}", arrow='yellow', indent=1)

            elif view_name == 'qc_message_special_types':
                logger.warning(f"{len(qc_df)} missing {code('message_special_type')} values found in {code('message_user')}", arrow='yellow', indent=1)

        else:
            # No QC issues to report
            logger.info(f'No QC issues found in {code(view_name)}', arrow='black')

    return total_warnings