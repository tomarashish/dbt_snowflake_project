# Open Target Platform Data in Snowflake using dbt

This project provides a data pipeline to download Open Target Platform parquet data and load it into Snowflake using dbt (data build tool).

## Overview

The [Open Target Platform](https://platform.opentargets.org/) provides valuable data on target-disease associations, which can be used for drug discovery and research. This project automates the process of:

1. Downloading Open Target Platform data in parquet format
2. Loading the data into Snowflake
3. Transforming the data using dbt models
4. Creating a comprehensive data mart for analysis

## Project Structure

```
opentarget_snowflake_dbt/
├── dbt_project.yml          # Main dbt project configuration
├── dbt_project_vars.yml     # Project variables
├── profiles.yml             # Snowflake connection configuration
├── requirements.txt         # Python dependencies
├── download_opentarget_data.py  # Script to download parquet data
├── macros/                  # dbt macros
│   ├── create_external_stage.sql  # Macro to create Snowflake external stage
│   └── load_parquet_to_table.sql  # Macro to load parquet data into tables
├── models/                  # dbt models
│   ├── schema.yml           # Model documentation
│   ├── staging/             # Staging models (raw data)
│   │   ├── stg_targets.sql
│   │   ├── stg_diseases.sql
│   │   ├── stg_evidence.sql
│   │   └── stg_associations.sql
│   ├── intermediate/        # Intermediate models
│   │   └── int_target_disease.sql
│   └── marts/               # Final presentation models
│       └── mart_target_disease_associations.sql
├── seeds/                   # Seed data files (if needed)
└── tests/                   # Custom tests
```

## Prerequisites

- Python 3.8+
- dbt 1.5+
- Snowflake account with appropriate permissions
- Access to Open Target Platform data

## Setup Instructions

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Snowflake Connection

Edit the `profiles.yml` file with your Snowflake connection details:

```yaml
opentarget_snowflake:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <your_snowflake_account>
      user: <your_snowflake_username>
      password: <your_snowflake_password>
      role: <your_snowflake_role>
      database: OPENTARGET_DB
      warehouse: <your_snowflake_warehouse>
      schema: PUBLIC
      threads: 4
```

### 3. Configure Project Variables

Edit the `dbt_project_vars.yml` file to customize settings:

```yaml
vars:
  opentarget_stage: 'OPENTARGET_STAGE'
  opentarget_version: '23.09'  # Update to the latest version as needed
  local_data_dir: 'data'
  cloud_storage_path: 'file:///path/to/data/'  # Update with your storage path
```

### 4. Download Open Target Platform Data

Run the download script to fetch the parquet data:

```bash
python download_opentarget_data.py --version 23.09 --output-dir data
```

Options:
- `--version`: Open Target Platform data version (default: 23.09)
- `--output-dir`: Directory to store downloaded data (default: data)
- `--data-types`: Data types to download (choices: targets, diseases, evidence, associations, all)
- `--list-only`: Only list available files without downloading

### 5. Create Snowflake External Stage

Use the provided macro to create an external stage pointing to your data:

```sql
-- Run this in dbt
{{ create_external_stage(
    stage_name=var('opentarget_stage'),
    url_path=var('cloud_storage_path')
) }}
```

### 6. Run dbt Models

Execute the dbt models to load and transform the data:

```bash
# Run all models
dbt run

# Run specific models
dbt run --select staging
dbt run --select marts
```

## Data Models

### Staging Models

- `stg_targets`: Raw target data from Open Target Platform
- `stg_diseases`: Raw disease data from Open Target Platform
- `stg_evidence`: Raw evidence data linking targets and diseases
- `stg_associations`: Raw association data between targets and diseases

### Intermediate Models

- `int_target_disease`: Combines target and disease data with associations

### Mart Models

- `mart_target_disease_associations`: Final presentation model with target-disease associations and evidence

## Usage Examples

### Query Target-Disease Associations

```sql
SELECT
    target_symbol,
    disease_name,
    association_score,
    evidence_count
FROM marts.mart_target_disease_associations
WHERE association_score > 0.7
ORDER BY association_score DESC
LIMIT 100;
```

### Find Targets for a Specific Disease

```sql
SELECT
    target_id,
    target_symbol,
    target_name,
    association_score,
    evidence_count
FROM marts.mart_target_disease_associations
WHERE disease_name ILIKE '%diabetes%'
ORDER BY association_score DESC;
```

## Maintenance

### Updating to a New Data Release

1. Update the `opentarget_version` variable in `dbt_project_vars.yml`
2. Run the download script with the new version
3. Re-run the dbt models

```bash
python download_opentarget_data.py --version 24.01
dbt run
```

## Troubleshooting

### Common Issues

1. **Snowflake Connection Issues**
   - Verify your credentials in `profiles.yml`
   - Ensure your Snowflake account is active and accessible

2. **Data Download Issues**
   - Check your internet connection
   - Verify the Open Target Platform version is valid
   - Ensure you have sufficient disk space

3. **dbt Model Failures**
   - Check the error messages in the dbt logs
   - Verify that the parquet files were downloaded correctly
   - Ensure the Snowflake stage is properly configured

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [Open Target Platform](https://platform.opentargets.org/) for providing the data
- [dbt](https://www.getdbt.com/) for the data transformation framework
- [Snowflake](https://www.snowflake.com/) for the data warehouse platform
