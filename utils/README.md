# Data Source Script Migration

The script `data_source_migration.py` provides mechanisms to migrate data sources from one Lemonade instance to another (i.e., from one database to another). The tool allows you to specify the ID of the data source you want to copy, but it also supports multiple IDs (as a comma-separated list) or the option to specify all IDs. 

## Parameters:

 * `--source-db`: Connection string for the source database (e.g., 'mysql://user:pass@host:port');
 * `--target-db`: Connection string for the target database (e.g., 'mysql://user:pass@host:port');
 * `--storage`: The storage id to link all datasources or a mapping like `4:1,1:2` meaning: all datasources from origin storage_id 4 will be mapped to storage_id 1 in target;
 * `--id`: ID of the datasources to migrate. Supported options are: a specific ID (e.g., `1`), a list of IDs (e.g., `1,2,3`), or `all` to migrate all datasources;
 * `--user-login`: (Optional) When set, it will change the current user owner of the workflow/pipeline.

### Usage Example

```bash
$ python3 datasource_migration.py  --source-db mysql://root:PASSWORD@<HOSTNAME1>:3306 \
    --target-db mysql://root:PASSWORD@<HOSTNAME2>:3306 \
    --user-login admin@lemonade.org.br \
    --id 34,37,38,39,40,41 \
    --storage 6:4,5:2,3:1
```
