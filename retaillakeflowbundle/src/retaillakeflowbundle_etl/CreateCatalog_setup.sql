-- SQL statements for the setup-unity-catalog job (sql_task file).
-- Same statements as the CreateCatalog.sql notebook in this folder.

create catalog retaildevBronze;

use catalog retaildevBronze;
create schema retaildevnew;

create catalog dev_catalog;

use catalog dev_catalog;
create schema dev_schema;
