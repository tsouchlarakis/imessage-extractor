import click
import logging
import pandas as pd
import re
from .helpers.verbosity import logger_setup, path, code
from os.path import expanduser, abspath, join, dirname
from send2trash import send2trash


target_fpath = abspath(join(dirname(__file__), '..', 'custom_tables', 'contacts.csv'))


@click.option('--exported-contacts-csv-fpath', type=click.Path(exists=True), required=True,
              default=expanduser('~/Desktop/contacts_export.csv'),
              help='Filepath of contacts exported .csv.')
@click.option('--delete-input-csv', is_flag=True, default=False,
              help='Move contacts .csv to the trash when upload to Postgres is complete.')
@click.option('-v', '--verbose', is_flag=True, default=False,
              help='Print messages to console.')

@click.command()
def refresh_contacts(exported_contacts_csv_fpath, delete_input_csv, verbose) -> None:
    """
    Refresh contacts.csv located in the 'custom_tables' directory with an exported
    .csv file via the macOS app Exporter for Contacts:

    App Link:
    https://itunes.apple.com/us/app/exporter-for-contacts/id959289998?mt=8

    This simple script accepts the raw .csv output of Exporter for Contacts, and massages
    it into a dataframe of key: value pairs in the form:

    contact_name: chat_identifier

    Where 'contact_name' is the contact's full name (i.e. 'John Smith'), and 'chat_identifier'
    can be either a phone number or an email address. Split rows in the raw export that are
    semicolon-separated (the app's default setting).

    NOTE: It is not necessary to use this script to refresh contacts.csv. That file can
    of course be refreshed manually, or however else the user wishes. This script simply
    provides a convenient way to refresh the file using the output of Exporter for Contacts.
    """
    logging_level = logging.INFO if verbose else logging.ERROR
    logger = logger_setup(name='refresh-contact', level=logging_level)
    logger.info(f'Processing {path(exported_contacts_csv_fpath)}')

    # Read datafile
    df = pd.read_csv(exported_contacts_csv_fpath)
    assert 'Full Name' in df.columns, 'Exported .csv file must contain a column named "Full Name"'
    logger.info(f'Read CSV, shape {df.shape}')

    # Drop defined values of "Full Name"
    drop_full_names = ['Error']
    df = df[~df['Full Name'].isin(drop_full_names)]
    df = df.drop_duplicates()

    # Create contact map in format `chat_identifier`: `contact_name`, where
    # `chat_identifier` is the contact's phone number or email, and `contact_name`
    # is the contact's display name in clear text.
    contact_map_df = pd.DataFrame(columns=['chat_identifier', 'contact_name'])

    #
    # Iterate over each row in the exported .csv file and process them sequentially
    #

    for i, row in df.iterrows():
        name = row['Full Name']

        for col, value in row[~row.isnull()].to_dict().items():
            if (col != 'Full Name') and ('phone' in col.lower() or 'email' in col.lower()):
                value_lst = []  # Convert to list to append alternative values

                if 'phone' in col.lower():
                    values = str(value).split(';')

                    for v in values:
                        phone_no_spaces = str(v).replace(' ', '')
                        phone_no_chars = str(v).replace(' ', '').replace('(', '').replace(')', '').replace('-', '')

                        # Add raw values as contender values
                        if phone_no_chars.isdigit():
                            value_lst.append(v)
                            value_lst.append(phone_no_spaces)
                            value_lst.append(phone_no_chars)

                        if re.match(r'^\d{10}$', phone_no_chars):
                            # Example: 4155954380
                            phone_10_no_chars_w_plus_one = '+1' + phone_no_chars
                            value_lst.append(phone_10_no_chars_w_plus_one)

                        if re.match(r'^\d{11}$', phone_no_chars) and phone_no_chars.startswith('1'):
                            # Example: 14155954380
                            phone_11_no_chars_w_plus = '+' + phone_no_chars
                            value_lst.append(phone_11_no_chars_w_plus)

                if 'email' in col.lower():
                    values = str(value).split(';')
                    for v in values:
                        value_lst.append(v)

                for v_val in list(set(value_lst)):
                    contact_map_df.loc[len(contact_map_df)] = [v_val, name]

    # Ensure only populated contact names remain
    contact_map_df = contact_map_df[~contact_map_df['contact_name'].isnull()]
    contact_map_df = contact_map_df.drop_duplicates()
    logger.info(f'Extracted {len(contact_map_df)} {code("(chat_identifier : contact_name)")}, pairs')

    # Output to target destination
    contact_map_df.to_csv(target_fpath, index=False)
    logger.info(f'Wrote {path(target_fpath)}')

    # Move exported CSV to trash if specified
    if delete_input_csv:
        send2trash(exported_contacts_csv_fpath)
        logger.info('Moved CSV to trash')
