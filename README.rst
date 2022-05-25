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
================

* macOS
* Python 3.X
* pip

⚙️ Installation
===============

.. code-block:: bash

   pip install imessage-extractor

⚡️ Usage
=========

First, you'll want to make sure you have the local iMessage SQLite database, called **chat.db**. By default on macOS, this database is stored in the **~/Library/Messages** directory. You can verify that it's there by runnning the following command:

.. code-block:: bash

   FILE=~/Library/Messages/chat.db
   if [ -f "$FILE" ]; then
       echo "You're good\!"
   else
       echo "Sadly, no file exists at $FILE - is it possible chat.db lives elsewhere on your system?"
   fi

You can also navigate to that directory in Finder and check whether a file called **chat.db** exists.

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
   * - --output-db-path
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
   # Vanilla call. Saves transformed iMessage chat data to ~/Desktop/imessage_extractor_chat.db.
   imessage-extractor go -v

🌈 Releasing
============

1. Make your changes locally and push to ``develop`` or a different feature branch.

2. Create a `pull request <https://github.com/tsouchlarakis/imessage-extractor/pulls>`_.

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
