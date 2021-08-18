==================
iMessage Extractor
==================


.. image:: https://img.shields.io/pypi/v/imessage_extractor.svg
        :target: https://pypi.python.org/pypi/imessage_extractor

.. image:: https://img.shields.io/travis/tsouchlarakis/imessage_extractor.svg
        :target: https://travis-ci.com/tsouchlarakis/imessage_extractor

.. image:: https://readthedocs.org/projects/imessage-extractor/badge/?version=latest
        :target: https://imessage-extractor.readthedocs.io/en/latest/?version=latest
        :alt: Documentation Status

Extract local iMessage data to a Postgres database or flat textfiles on a Mac.

🏁 Getting Started
=================

``imessage-extractor`` is a pip-installable python package designed to be run on macOS that extracts iMessage chat data stored locally in a SQLite database by macOS, and saves it to an array of flat .csv files or a Postgres database, or both.

🧿 Prerequisites
----------------

* macOS
* Python 3.X
* pip


⚒ Installation
--------------

.. code-block:: bash

   pip install imessage-extractor

It really is that simple ✔️

⚡️ Usage
-------

First, you'll want to make sure you have the iMessage SQLite database, called ``chat.db``, stored locally. By default on macOS, this database is stored in the ``~/Library/Messages`` directory. You can verify that it's there by runnning the following command:

.. code-block:: bash

   FILE=~/Library/Messages/chat.db
   if [ -f "$FILE" ]; then
       echo "You're good!"
   else
       echo "Sadly, no file exists at $FILE - is it possible chat.db lives elsewhere on your system?"
   fi

Or by just navigating to that directory in Finder, and seeing whether a file called ``chat.db`` exists 🙂

Options
~~~~~~~

.. list-table:: Commandline Options
   :widths: 40 15 18 9 50
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
     - EITHER the path to a local Postgres credentials file 'i.e. ~/.pgpass', OR a string with the    connection credentials. Must be in format 'hostname:port:db_name:user:pg_pass'.
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

⚙️ Customization
===============

Here's where the fun begins! Because the use case for each user's iMessage history is slightly different, making custom changes to your local installation of ``imessage-extractor`` is encouraged and easy.

⤴️ Changelog
============

See `changelog <Changelog.rst>`_.

📜 License
==========

See `license <LICENSE>`_.

🙏 Credits
----------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
