.. image:: ../../graphics/source_code.png

iMessage Extractor workflow split into multiple sub-components. In order:

Step 1: Extract chat.db Tables
==============================

The first step is to extract tables from **chat.db** and save them as .csv files to a temporary database. If saving data to Postgres (``--pg-schema`` is specified) and ``rebuild`` is not specified, then we'll attempt to only query and save any *new* data in **chat.db**.

We can control which tables are fully rebuilt and which are simply updated with new data in the **chatdb_table_info.json** file. Those with ``write_mode: "replace"`` will be fully replaced with the corresponding **chat.db** table, but those with ``write_mode: "append"`` will have only new data appended to them

(For the nerds out there, the workflow logic figures out what exactly is *new* data by looking at the primary key values for each table specified in the ``"primary_key"`` JSON attribute).

*Once this point has been reached, chat.db table data has been saved to a folder on disk (controlled with the --save-csv-dpath argument.) Therefore, all following information ONLY applies if saving data to a Postgres schema.* 😊

After the source tables are loaded into Postgres, we can now define **chatdb views** that are only dependent on those **chat.db** tables. These views were developed by yours truly, and exist purely for your convenience of querying the data. For example, there really isn't a super intutive way on first glance to query the source tables to simply pull a list of all messages sent to/from you, and the contacts associated with those messages. ✨ **message_vw** will be your friend ✨

You're encouraged to add more useful views to the **views**/ folder as you see fit. If you do, be sure to add an entry in **chatdb_view_info.json** with that view's name, and the tables/views it depends on. The workflow will fold any new view into the definition of chatdb views automatically, and will a bit of magic to figure out the right order to define the views given their dependencies, and will notify you if anything has gone wrong.

📂 `chatdb`_
---------

* **chatdb.py**: custom objects designed for interacting with the **chat.db** database
* **chatdb_table_info.json**: configure handling of source data tables
* **chatdb_view_info.json**:
* **views**/
    * all views whose only dependencies are the source **chat.db** tables, or other views in this folder