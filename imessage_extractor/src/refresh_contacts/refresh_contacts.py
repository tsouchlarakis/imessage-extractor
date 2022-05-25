import logging
import numpy as np
import pandas as pd
import phonenumbers
import sqlite3
from imessage_extractor.src.helpers.verbosity import logger_setup, path, code
from os import walk
from os.path import expanduser, abspath, join, dirname, splitext


target_fpath = abspath(join(dirname(dirname(__file__)), 'static_tables', 'data', 'contacts.csv'))


def list_address_book_db_fpaths(dpath: str) -> list:
    """
    Search the address book home filepath for .abcddb files and return the filepaths
    as a list.
    """
    db_fpaths = []
    for root, dpaths, filenames in walk(dpath):
        for f in filenames:
            if splitext(f)[1] == '.abcddb':
                db_fpaths.append(join(root, f).replace('./', ''))

    return db_fpaths


def get_contacts_from_db(db_con: sqlite3.Connection, sep: str=';') -> pd.DataFrame:
    """
    Get a dataframe with columns first_name, last_name, email, phone number
    from a given database connection.
    """
    db_record = pd.read_sql('select * from ZABCDRECORD', con=db_con)
    db_record = db_record.fillna('').set_index('Z_PK')
    db_phone_number = pd.read_sql('select * from ZABCDPHONENUMBER', con=db_con)
    db_email = pd.read_sql('select * from ZABCDEMAILADDRESS', con=db_con)

    emails = (
        db_record
        [['ZFIRSTNAME', 'ZLASTNAME']]
        .merge(db_email
               .fillna('')
               .set_index('ZOWNER')[['ZADDRESS']],
               left_index=True,
               right_index=True)
        .groupby(['ZFIRSTNAME','ZLASTNAME'])
        ['ZADDRESS']
        .apply(lambda x: sep.join(x))
        .reset_index()
    )

    phone_numbers = (
        db_record
        [['ZFIRSTNAME', 'ZLASTNAME']]
        .merge(db_phone_number
               .fillna('')
               .set_index('ZOWNER')[['ZFULLNUMBER']],
               left_index=True,
               right_index=True)
        .groupby(['ZFIRSTNAME','ZLASTNAME'])
        ['ZFULLNUMBER']
        .apply(lambda x: sep.join(x))
        .reset_index()
    )

    contacts_compact = (
        emails
        .merge(phone_numbers, on=['ZFIRSTNAME', 'ZLASTNAME'], how='outer')
        .rename(columns={
            'ZFIRSTNAME': 'first_name',
            'ZLASTNAME': 'last_name',
            'ZADDRESS': 'email',
            'ZFULLNUMBER': 'phone_number',
        })
    )

    return contacts_compact


def extract_phone_number(raw_string: str, sep: str=';') -> dict:
    """
    Extract all phone number(s) in a given row. Input format will be...

        (XXX) XXX-XXXX;(XXX) XXX-XXXX;...

    ...with an arbitrary number of phone numbers. Return all phone numbersas a list.
    """
    if isinstance(raw_string, str):
        return raw_string.split(sep)
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


def refresh_contacts(logger: logging.Logger) -> None:
    """
    Refresh contacts.csv located in the 'static_tables' directory from the Address Book
    local database.

    contact_name: chat_identifier

    Where 'contact_name' is the contact's full name (i.e. 'John Smith'), and 'chat_identifier'
    can be either a phone number or an email address. Split rows in the raw export that are
    semicolon-separated.
    """
    sep=';'

    # Get address boook contacts (first/last name, email, phone number). Where the user
    # has multiple emails or phone numbers, concatenate into a separated string
    address_book_home_dpath = expanduser('~/Library/Application Support/AddressBook')
    address_book_db_fpaths = list_address_book_db_fpaths(address_book_home_dpath)

    contacts_compact_lst = []
    for db_fpath in address_book_db_fpaths:
        logger.debug(f'Reading db {path(db_fpath)}', arrow='black')
        db_con = sqlite3.connect(db_fpath)
        df = get_contacts_from_db(db_con, sep=sep)
        contacts_compact_lst.append(df)
        db_con.close()

    if not len(contacts_compact_lst):
        raise Exception('Executing application does not have full disk access. Remedy this in System Preferences > Security > Full Disk Access.')

    df = pd.concat(contacts_compact_lst, axis=0)[['first_name', 'last_name', 'email', 'phone_number']].drop_duplicates()
    n_unique_contacts = df[['first_name', 'last_name']].drop_duplicates().shape[0]
    logger.info(f'Read {n_unique_contacts} records from Contacts app', arrow='black')

    # Create contact map (output dataframe) in format `chat_identifier`: `contact_name`,
    # where `chat_identifier` is the contact's phone number or email, and `contact_name`
    # is the contact's display name in clear text.
    contact_map_cols = ['contact_name', 'chat_identifier', 'identifier_type']
    contact_map_df = pd.DataFrame(columns=contact_map_cols)

    #
    # Iterate over each row in the exported .csv file and process them sequentially
    #

    # Filter out these values of "Full Name"
    ignore_full_names = ['Error']

    n_unique_contacts = 0
    n_invalid_contacts = 0
    for i, row in df.iterrows():
        name = row['first_name'] + ' ' + row['last_name']

        if name in ignore_full_names:
            n_invalid_contacts += 1

        else:
            n_unique_contacts += 1
            pn_lst_original = extract_phone_number(row['phone_number'], sep=sep)
            pn_lst_variated = []

            for pn_string in pn_lst_original:
                # Get all possible variations of this phone number
                pn_variations = variate_phone_number(pn_string)

                # Append each variation to a recording dictionary
                for variation in pn_variations:
                    if variation not in pn_lst_variated:
                        pn_lst_variated.append(variation)

            phone_number_df = pd.DataFrame(pn_lst_variated)
            if len(phone_number_df):
                phone_number_df.columns = ['chat_identifier']
            else:
                phone_number_df = pd.DataFrame(columns=['chat_identifier'])
            phone_number_df['contact_name'] = name
            phone_number_df['identifier_type'] = 'phone'
            phone_number_df = phone_number_df[contact_map_cols]

            email_lst = extract_email(row['email'], sep=sep)
            email_df = pd.DataFrame(email_lst)
            if len(email_df):
                email_df.columns = ['chat_identifier']
            else:
                email_df = pd.DataFrame(columns=['chat_identifier'])
            email_df['contact_name'] = name
            email_df['identifier_type'] = 'email'
            email_df = email_df[contact_map_cols]

            contact_df = pd.concat([phone_number_df, email_df], axis=0)[contact_map_cols]
            contact_map_df = pd.concat([contact_map_df, contact_df], axis=0)


    # Ensure only populated contact names remain
    contact_map_df = contact_map_df[~contact_map_df['contact_name'].isnull()]
    contact_map_df = contact_map_df.drop_duplicates().sort_values('contact_name')

    # Output to target destination
    contact_map_df.to_csv(target_fpath, index=False)
    logger.info(f'Saved {path(target_fpath)}', arrow='black')
