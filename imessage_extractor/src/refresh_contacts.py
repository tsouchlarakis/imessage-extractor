import click
import logging
import numpy as np
import pandas as pd
import phonenumbers
from .helpers.verbosity import logger_setup, path, code
from os.path import expanduser, abspath, join, dirname
from send2trash import send2trash


target_fpath = abspath(join(dirname(__file__), 'custom_tables', 'data', 'contacts.csv'))


def extract_phone_number(raw_string: str, sep: str=';') -> dict:
    """
    Extract all phone number(s) in a given row. Input format will be...

        home: (XXX) XXX-XXXX; mobile: (XXX) XXX-XXXX; work: (XXX) XXX-XXXX; work fax: (XXX) XXX-XXXX

    ...with an arbitrary number of phone numbers. Return each phone number (in its native format)
    as a key: value pair in the form...

        phone_number: phone_number_subtype

    ...where `phone_number` is the phone number in its native format, and `phone_number_subtype` is
    the type of phone number (i.e. 'home', 'mobile', 'work', 'work fax').
    """
    values_dct = {}

    if isinstance(raw_string, str):
        for s in raw_string.split(sep):
            value_subtype = s.split(':')[0].strip()
            value = s.split(':')[1].strip()
            values_dct[value] = value_subtype

        return values_dct

    elif np.isnan(raw_string):
        return {}

    else:
        return {}


def extract_email(raw_string: str, sep:str=';') -> dict:
    """
    Apply the same extractions as extract_phone_number().
    """
    return extract_phone_number(raw_string, sep)


def variate_phone_number(phone_number: str) -> list:
        """
        Use the phonenumbers package to return a set of phone number variations
        that can be used to match iMessages with, since the iMessage app can
        record the contact's phone number in a variety of formats.
        """
        pn_obj = phonenumbers.parse(phone_number, 'US')

        return [
            phonenumbers.format_number(pn_obj, phonenumbers.PhoneNumberFormat.E164),
            phonenumbers.format_number(pn_obj, phonenumbers.PhoneNumberFormat.NATIONAL),
            phonenumbers.format_number(pn_obj, phonenumbers.PhoneNumberFormat.INTERNATIONAL),
        ]



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

    # Read datafile
    exported_contacts_csv_fpath = expanduser(exported_contacts_csv_fpath)
    logger.info(f'Processing {path(exported_contacts_csv_fpath)}')
    df = pd.read_csv(exported_contacts_csv_fpath).drop_duplicates()
    assert 'Full Name' in df.columns, 'Exported .csv file must contain a column named "Full Name"'
    logger.info(f'Read CSV, shape {df.shape}')

    # Validate dataframe structure
    assert list(df.columns) == ['Full Name', 'Phone', 'Email']

    # Create contact map (output dataframe) in format `chat_identifier`: `contact_name`,
    # where `chat_identifier` is the contact's phone number or email, and `contact_name`
    # is the contact's display name in clear text.
    contact_map_cols = ['contact_name', 'chat_identifier', 'identifier_type', 'identifier_subtype']
    contact_map_df = pd.DataFrame(columns=contact_map_cols)

    #
    # Iterate over each row in the exported .csv file and process them sequentially
    #

    # Filter out these values of "Full Name"
    ignore_full_names = ['Error']

    n_unique_contacts = 0
    n_invalid_contacts = 0
    for i, row in df.iterrows():
        name = row['Full Name']

        if name in ignore_full_names:
            n_invalid_contacts += 1

        else:
            n_unique_contacts += 1
            pn_dct_original = extract_phone_number(row['Phone'])
            pn_dct_variated = {}

            for pn_string, pn_subtype in pn_dct_original.items():
                pn_dct_variated[pn_string] = pn_subtype

                # Get all possible variations of this phone number
                pn_variations = variate_phone_number(pn_string)

                # Append each variation to a recording dictionary
                for variation in pn_variations:
                    if variation not in pn_dct_variated.keys():
                        pn_dct_variated[variation] = pn_subtype

            phone_number_df = pd.DataFrame(pn_dct_variated, index=[None]).T.reset_index()
            phone_number_df.columns = ['chat_identifier', 'identifier_subtype']
            phone_number_df['contact_name'] = name
            phone_number_df['identifier_type'] = 'phone'
            phone_number_df = phone_number_df[contact_map_cols]

            email_dct = extract_email(row['Email'])
            email_df = pd.DataFrame(email_dct, index=[None]).T.reset_index()
            email_df.columns = ['chat_identifier', 'identifier_subtype']
            email_df['contact_name'] = name
            email_df['identifier_type'] = 'email'
            email_df = email_df[contact_map_cols]

            contact_df = pd.concat([phone_number_df, email_df], axis=0)[contact_map_cols]
            contact_map_df = pd.concat([contact_map_df, contact_df], axis=0)


    # Ensure only populated contact names remain
    contact_map_df = contact_map_df[~contact_map_df['contact_name'].isnull()]
    contact_map_df = contact_map_df.drop_duplicates().sort_values('contact_name')
    logger.info(f'Extracted {len(contact_map_df)} {code("(chat_identifier : contact_name)")}, pairs from {n_unique_contacts} unique contacts ({n_invalid_contacts} invalid)')

    # Output to target destination
    contact_map_df.to_csv(target_fpath, index=False)
    logger.info(f'Wrote {path(target_fpath)}')

    # Move exported CSV to trash if specified
    if delete_input_csv:
        send2trash(exported_contacts_csv_fpath)
        logger.info('Moved CSV to trash')
