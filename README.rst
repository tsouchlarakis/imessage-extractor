.. image:: graphics/imessage_extractor_logo.png

.. role:: raw-html(raw)
    :format: html

Extract chat data from iMessage and much, much more!

:raw-html:`<br />`

.. image:: https://img.shields.io/pypi/v/imessage_extractor.svg
        :target: https://pypi.python.org/pypi/imessage_extractor

:raw-html:`<br />`

🏁 Getting Started
==================

``imessage-extractor`` is a pip-installable Python package that extracts iMessage chat data stored locally in a SQLite database by macOS, and saves it to an set of flat .csv files or a Postgres database, or both. If saving to a Postgres database the workflow will create a series of staging tables, views, and other database objects designed to make querying your iMessage chat data as simple and straightforward as possible.

🧿 Prerequisites
----------------

* macOS
* Python 3.X
* pip


⚙️ Installation
---------------

.. code-block:: bash

   pip install imessage-extractor

   # Vanilla usage (just save chat.db tables to .csv files)
   imessage-extractor go --save-csv "~/Desktop/imessage_chatdb_extract" -v

It really is that simple ✨ but this vanilla workflow call merely scratches the surface of what **iMessage Extractor** can do.

⚡️ Usage
---------

First, you'll want to make sure you have the iMessage SQLite database, called **chat.db**, stored locally. By default on macOS, this database is stored in the **~/Library/Messages** directory. You can verify that it's there by runnning the following command:

.. code-block:: bash

   FILE=~/Library/Messages/chat.db
   if [ -f "$FILE" ]; then
       echo "You're good\!"
   else
       echo "Sadly, no file exists at $FILE - is it possible chat.db lives elsewhere on your system?"
   fi

Or by just navigating to that directory in Finder, and seeing whether a file called **chat.db** exists 🙂

A call to the extractor can involve the following options (examples below):

.. list-table:: Commandline Options
   :header-rows: 1

   * - Option
     - Data Type
     - Default Value
     - Required
     - Description
   * - --chat-db-path
     - string, path
     - ~/Library/Messages/chat.db
     - Required
     - Path to working chat.db.
   * - --save-csv
     - string, path
     -
     - Optional
     - Path to folder to save chat.db tables to.
   * - --pg-schema
     - string
     -
     - Optional
     - Name of Postgres schema to save tables to.
   * - --pg-credentials
     - string, path
     -
     - Optional
     - EITHER the path to a local Postgres credentials file 'i.e. ~/.pgpass', OR a string with the    connection credentials. Must be in format ``hostname:port:db_name:user:pg_pass``.
   * - -r, --rebuild
     - bool
     - False
     - Optional
     - Wipe target Postgres schema and rebuild from scratch.
   * - -s, --stage
     - bool
     - False
     - Optional
     - Build staging tables and views after the chat.db tables have been loaded
   * - -v, --verbose
     - bool
     - False
     - Optional
     - Set logging level to INFO.
   * - -d, --debug
     - bool
     - False
     - Optional
     - Set logging level to DEBUG.
   * - -h, --help
     - bool
     - False
     - Optional
     - Show the help message and exit.

*Note that while --save-csv and pg-schema are both optional, at least one of them must be specified to run the extraction beacuse the program must have an output destination (either flat .csv files, a Postgres schema, or both).*

Now that we have our chat.db file and we understand the preceding options, we're ready to make a call to the extractor. To do this, we need to call the ``go`` command within the extractor.

To save the chat.db tables to a set of flat .csv files at a target folder:

.. code-block:: bash

   imessage-extractor go --save-csv "~/Desktop/imessage-extractor/imessage_chatdb_extract"

To save the chat.db tables to a Postgres schema, we need to supply two things:

1.  The credentials to establish a Postgres connection
2.  The name of the schema to save the tables to

For (1), we can supply this either using a **.pgpass** file, which is generally stored in your home directory (**/Users/<username>**), or by passing the desired Postgres credentials in a connection string using.

If we supply a **.pgpass** file, that file's contents **must** be in the format ``hostname:port:db_name:user_name:password``. Alternatively, if we supply those credentials by commandline string, they must be in the same format.

For example, our **.pgpass** file might be a text file with one line: ``127.0.0.1:5432:<your_database_name>:<your_user_name>:<your_password>``. We can then supply the option ``--pg-credentials "~/.pgpass"`` to the ``go`` command.

Alternatively, we can supply the same credentials to the ``go`` command with ``--pg-credentials "127.0.0.1:5432:<your_database_name>:<your_user_name>:<your_password>"``.

It's totally your choice how you choose to supply the Postgres credentials (they are used identically in establishing a database connection no matter how they're supplied to ``go``, but using **.pgpass** is generally preferred for security).

For (2), this can be any Postgres schema name, but ideally it would be a non-existent or unused one, the reason being that if the pipeline is run with the ``rebuild`` option set to ``True``, then the schema will be dropped and recreated before the extraction.

Here are a few ways we can tell the extractor to load data into Postgres:

.. code-block:: bash

   # Using a .pgpass file
   imessage-extractor go --pg-credentials "~/.pgpass" --pg-schema "imessage"

   # Or by passing the connection string
   imessage-extractor go --pg-credentials "<hostname>:<port>:<db_name>:<user_name>:<password>" --pg-schema "imessage"

Lastly, we can append the ``--verbose`` option to get feedback printed to the console as the extraction is happening!

🌈 Releasing
------------

``imessage-extractor`` utilizes `versioneer <https://pypi.org/project/versioneer/>`_ for versioning. This requires the ``versioneer.py`` in the project's top-level directory, as well as some lines in the package's ``setup.cfg`` and ``__init__.py``.

1. Make your changes locally and push to ``develop`` or a different feature branch.

2. Tag the new version. This will be the version of the package once publication to PyPi is complete.

   .. code-block:: bash

      git tag {major}.{minor}.{patch}

3. Publish to PyPi.

   .. code-block:: bash

      rm -rf ./dist && python3 setup.py sdist && twine upload -r pypi dist/*

4. Install the new version of ``imessage-extractor``.

   .. code-block:: bash

      pip install imessage-extractor=={major}.{minor}.{patch}

5. Create a `pull request <https://github.com/tsouchlarakis/imessage-extractor/pulls>`_.

⚒ Customization
================

Here's where the fun begins! Because the use case for each user's iMessage history is slightly different, making custom changes to your local installation of ``imessage-extractor`` is encouraged and easy.

⚓️ Changelog
=============

See `changelog <CHANGELOG.rst>`_.

📜 License
==========

See `license <LICENSE>`_.

🙏 Credits
----------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
