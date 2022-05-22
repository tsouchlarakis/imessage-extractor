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

``imessage-extractor`` is a pip-installable Python package that extracts iMessage chat data stored locally in a SQLite database by macOS and joins it with local Contacts data and saves the transformed data in a local SQLite database. There is also a Streamlit app baked into this package designed to display visual analytics on the user's iMessage history.

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

So that your iMessage history can be automatically joined with your contacts, you'll also want to make sure you have one one or more .aclcddb database files at **~/Library/Application Support/AddressBook/ABAssistantChangelog**. You can also check the **Sources/** subdirectories.

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
   * - -- outputdb-path
     - string, path
     - ~/Desktop/imessage_extractor_chat.db
     - Required
     - Desired path to output .db SQLite database file..
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

Now that we have our chat.db file and we understand the preceding options, we're ready to make a call to the extractor. To do this, we need to call the ``go`` command within the extractor:

.. code-block:: bash

   imessage-extractor go

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
