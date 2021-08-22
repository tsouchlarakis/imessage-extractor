.. image:: ../../graphics/source_code.png

The **iMessage Extractor** workflow is split into multiple steps...

Step 1: Extract chat.db Tables
==============================

The first step is to extract tables from **chat.db** and save them as .csv files to a temporary database. If saving data to Postgres (``--pg-schema`` is specified) and ``rebuild`` is not specified, then we'll attempt to only query and save any *new* data in **chat.db**.

*Once this point has been reached, chat.db table data has been saved to a folder on disk (controlled with the --save-csv-dpath argument.) Therefore, all following information ONLY applies if saving data to a Postgres schema.* 😊

We can control which tables are fully rebuilt and which are simply updated with new data in the **chatdb_table_info.json** file. Those with ``"write_mode": "replace"`` will be fully replaced with the corresponding **chat.db** table, but those with ``"write_mode": "append"`` will have only new data appended to them

(For the nerds out there, the workflow logic figures out what exactly is *new* data by looking at the primary key values for each table specified in the ``"primary_key"`` JSON attribute).

📂 chatdb/
----------

- **chatdb.py**: custom objects designed for interacting with the **chat.db** database
- **chatdb_table_info.json**: configure handling of source data tables
- **chatdb_view_info.json**: list references, if any, for chat.db views
- **views**/
    - all views whose only dependencies are the source **chat.db** tables, or other views in this folder


Step 2: Custom Tables
=====================

With **iMessage Extractor**, we have the option of defining custom tables by adding .csv files to the **custom_tables/** folder. The workflow will automatically pick up on them and **will fully replace them each time the workflow is run**. These custom tables can be any .csv file of your choosing, and only require that an item be appended to **custom_table_info.json** with the name of the table, the columns it contains and the Postgres datatypes of those columns.

Take the simple example of maintaining a list of contacts. Because the iMessage app does not actually store contacts, and instead looks them up on the fly, contact names are not actually stored in **chat.db**. Instead, a ``chat_identifier`` is stored, that's generally the phone number or Apple ID of the contact. If we'd like to include contact names in our database (always a nice touch, so you don't have to rely on your ability to recognize phone numbers/Apple IDs), we need to define a custom table that will store the contact names, that we then use as a mapping from ``chat_identifier`` to ``contact_name``.

What do you know, **iMessage Extractor** ships with a few of custom table template files in the custom_tables/ folder that help with just this. How convenient!

Just add entries to the **contacts_manual.csv** file, and they'll automagically appear in **message_vw**, the main entry point for querying your iMessage history 🪄

📂 custom_tables/
-----------------

- **data**/
    - add any new custom table .csv data files here
    - **contacts_ignored.csv**: maintain list of contacts to ignore in the workflow-final QC step (i.e. spam texts, one-time passwords, etc.)
    - **contacts_manual.csv**: maintain running list of contacts that you'd like to manually add to the database
    - **contacts.csv**: exported contact list using a contacts exporter app like `Contacts Exporter <https://apps.apple.com/us/app/exporter-for-contacts-2/id1526043062?mt=12>`_
- **custom_table_info.json**: store column specification (name and datatype) for each custom table
- **custom_tables.py**: python objects responsible for maintaining custom tables in the Postgres database

Step 3: Define "chat.db" Views
================================

After the source **chat.db** tables and custom tables are loaded into Postgres, we can now define **chatdb views**, which are simply Postgres views that are dependent on the source **chat.db** tables and/or the custom tables defined in Step 2.

These views were developed by yours truly, and exist purely for your convenience of querying your iMessage history data. For example, there really isn't a super intutive way on first glance to query the source tables to simply pull a list of all messages sent to/from you, and the contacts associated with those messages.

✨ **message_vw** will be your friend ✨

You're encouraged to add more useful views to the **views**/ folder as you see fit. If you do, be sure to add an entry in **chatdb_view_info.json** with that view's name, and the tables/views it depends on. The workflow will fold any new view into the definition of chatdb views automatically, and will use a bit of magic to figure out the right order to define the views given their dependencies, and will notify you of any issues encountered along the way.

Step 4: Staging Tables and Views
=================================

"Staging" tables and views refers to tables/views that are dependent on any of the following types of objects:

1. **custom tables** (Step 2)
2. **chat.db views** (Step 3)
3. other staging tables or views (this step)

Because of this condition (3) staging tables/views can be dependent on another staging table/view, which, in turn, may be depenent on another, up to an arbitrary depth. As a result, this workflow contains logic to intelligently determine the correct order to define the staging tables/views, depending on the tables/views that they, in turn, reference.

📂 staging/
-----------

- **tables**/
    - refresh functions responsible for maintaining each individual staging table
    - refresh functions can fully rebuild staging tables on each run, or only update them with new data (user customizable)
    - each staging table must have a refresh function defined here, and each refresh function must take the same parameters
- **views**/
    - view definitions for staging views
- **common.py**: common library for objects referenced used across refresh functions
- **staging_table_info.json**: store column specification (name and datatype), primary key column name (if present), and a list of references for each staging table
- **staging_view_info.json**: list references for staging views
- **staging.py**: python objects designed for staging table and view interaction

Step 5: Quality Control
========================

Save a couple of views in the destination Postgres database that report on the integrity of the data finally loaded into Postgres. Check each view and report the results to the user.

📂 quality_control/
-------------------

- **views**/
    - view definitions that report on integrity of the data loaded into Postgres
- **quality_control.py**: python objects designed for reporting quality control to the user
