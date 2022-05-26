=========
Changelog
=========

All notable changes to this project will be documented in this file.

The format is based on `Keep a Changelog <https://keepachangelog.com/en/1.0.0/>`_\ ,
and this project adheres to `Semantic Versioning <https://semver.org/spec/v2.0.0.html>`_.


.. raw:: html


.. V.V.V (YYYY-MM-DD)
.. ------------------
.. **Added**

.. **Changed**

.. **Deprecated**

.. **Removed**

.. **Fixed**

.. **Security**

1.0.9 (2022-05-25)
------------------
**Added**
- Grpahics folder to manifest

**Changed**

**Deprecated**

**Removed**

**Fixed**

1.0.8 (2022-05-24)
------------------
**Added**
- CHANGELOG to manifest

**Changed**

**Deprecated**

**Removed**

**Fixed**

1.0.7 (2022-05-24)
------------------
**Added**
- ``imessage-extractor run-app`` command

**Changed**
- Finalized initial version of **Pick a Contact** page

**Deprecated**

**Removed**

**Fixed**
- Thread identification logic in ``message_user`` table

**Security**

1.0.6 (2022-05-22)
------------------
**Added**

**Changed**

**Deprecated**

**Removed**

**Fixed**
- Security dependencies detailed `here <https://github.com/tsouchlarakis/imessage-extractor/security/dependabot/15>`_ and `here <https://github.com/tsouchlarakis/imessage-extractor/security/dependabot/16>`_

**Security**

1.0.5 (2022-05-21)
------------------
**Added**
- Automatic contact list refreshing
- My Stats infrastructure in analytic app
- Stopwords table
- ``Visuals`` class

**Changed**
- Overhaul to migrate Postgres dependency to SQLite
- Requirements
- Restructured staging table/view definitions
- Enhanced logging output

**Deprecated**

**Removed**
- ``psycopg2-binary`` requirement

**Fixed**

**Security**

1.0.4 (2022-01-16)
------------------
**Added**
- ``message_tokens`` view
- Additional ``message_special_type`` values and null text values
- Pick a Contact app enhancements

**Changed**
- Requirements
- Additional data added to contacts.csv
  - Now use ``phonenumbers`` packages to parse phone numbers from exported contacts

**Deprecated**
- ``message_tokens`` table

**Removed**

**Fixed**
- Join to ``thread_origins``
- Handle carriage return in message text

**Security**

1.0.3 (2021-09-08)
------------------
**Added**
- View of ``message_user`` filtered for just messages that are text ``message_user_text_vw``
- Renamed column 'is_thread' to 'is_threaded_reply' for a more accurate description

1.0.2 (2021-08-22)
------------------
**Added**
- STDOUT log handler

**Fixed**
- Hard-coded ``imessage_test`` schema

1.0.1 (2021-08-21)
------------------
**Added**
- Repository image

**Changed**
- Updated README and CHANGELOG documentation
- Updated requirements.txt

1.0.0 (2021-08-21)
------------------
**Added**
- ``imessage-extractor`` launch! 🚀

.. image:: graphics/rocket.gif
