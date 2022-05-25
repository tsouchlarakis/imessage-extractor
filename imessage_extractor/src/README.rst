.. image:: ../../graphics/source_code.png

The **iMessage Extractor** workflow (called with ``imessage-extractor go```) is split into multiple steps...

Step 1: Extract chat.db Tables
==============================

The first step is to read the local **chat.db**, apply transformations and save a new **chat.db**.

📂 chatdb/
----------

- **chatdb.py**: custom objects designed for interacting with the **chat.db** database
- **chatdb_table_info.json**: configure handling of source data tables
- **chatdb_view_info.json**: list references, if any, for chat.db views
- **views**/
    - all views whose only dependencies are the source **chat.db** tables, or other views in this folder

Step 2: Static Tables
=====================

With **iMessage Extractor**, we have the option of defining static tables by adding .csv files to the **static_tables/** folder. The workflow will automatically pick them up and **will fully replace them each time the workflow is run**. These static tables can be any .csv file of your choosing, and only require that a key be appended to **static_table_info.json** with the name of the table, the columns it contains and the SQLite datatypes of those columns.

Take the simple example of maintaining a list of contacts. Because the iMessage app does not actually store contacts, and instead looks them up on the fly, contact names are not actually stored in **chat.db**. Instead, a ``chat_identifier`` is stored, that's generally the phone number or Apple ID of the contact. If we'd like to include contact names in our database (always a nice touch, so you don't have to rely on your ability to recognize phone numbers/Apple IDs), we need to define a static table that will store the contact names, that we then use as a mapping from ``chat_identifier`` to ``contact_name``.

What do you know, **iMessage Extractor** ships with a few of static table template files in the static_tables/ folder that help with just this. How convenient!

Just add entries to the **contacts_manual.csv** file, and they'll automagically appear in **message_user**, the main entry point for querying your iMessage history 🪄

📂 static_tables/
-----------------

- **data**/
    - add any new static table .csv data files here
    - **contacts_ignored.csv**: maintain list of contacts to ignore in the workflow-final QC step (i.e. spam texts, one-time passwords, etc.)
    - **contacts_manual.csv**: maintain running list of contacts that you'd like to manually add to the database
    - **contacts.csv**: exported contact list using a contacts exporter app like `Contacts Exporter <https://apps.apple.com/us/app/exporter-for-contacts-2/id1526043062?mt=12>`_
- **static_table_info.json**: store column specification (name and datatype) for each static table
- **static_tables.py**: python objects responsible for maintaining static tables in the transformed SQLite database

Step 3: Define "chat.db" Views
================================

After the source **chat.db** tables and static tables are loaded into SQLite, we can now define **chatdb views**, which are simply SQLite views that are dependent on the source **chat.db** tables and/or the static tables defined in Step 2.

These views were developed by yours truly, and exist purely for your convenience of querying your iMessage history data. For example, there really isn't a super intutive way on first glance to query the source tables to simply pull a list of all messages sent to/from you, and the contacts associated with those messages.

✨ **message_user** will be your friend ✨

You're encouraged to add more useful views to the **views**/ folder as you see fit. If you do, be sure to add an entry in **chatdb_view_info.json** with that view's name, and the tables/views it depends on. The workflow will fold any new view into the definition of chatdb views automatically, and will use a bit of magic to figure out the right order to define the views given their dependencies, and will notify you of any issues encountered along the way.

Step 4: Staging Tables and Views
=================================

"Staging" tables and views refers to tables/views that are dependent on any of the following types of objects:

1. **static tables** (Step 2)
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
- **staging_sql_info.json**: list references for staging views
- **staging.py**: python objects designed for staging table and view interaction

Step 5: Quality Control
========================

Save a couple of views in the destination SQLite database that report on the integrity of the data finally loaded into SQLite. Check each view and report the results to the user.

📂 quality_control/
-------------------

- **views**/
    - view definitions that report on integrity of the data loaded into SQLite
- **quality_control.py**: python objects designed for reporting quality control to the user
