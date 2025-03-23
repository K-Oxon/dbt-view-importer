# dbt-view-importer

<!-- [![PyPI version](https://badge.fury.io/py/dbt-view-importer.svg)](https://badge.fury.io/py/dbt-view-importer) -->
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)

A tool to automatically convert BigQuery views to dbt models.

## Overview

This tool retrieves views from a specified BigQuery dataset and converts them into appropriate dbt models (SQL files and YAML files). It also allows you to include view dependencies in the retrieval. Key features include:

- Comprehensive view retrieval from specified BigQuery datasets
- Dependency retrieval for views (using Data Catalog Lineage API)
- Automatic generation of dbt models (SQL and YAML)
- Model generation with customizable templates
- View name filtering for imports

## Important Note

> ⚠️ In some cases, lineage information may not be retrievable immediately. If you encounter issues, waiting a short time and trying again may resolve the problem.

## Installation

### Prerequisites

- Python 3.9 or higher
- Authenticated Google Cloud environment (access to BigQuery and Data Catalog Lineage API)

### Installation Options

```bash
# Install from GitHub
pip install git+https://github.com/K-Oxon/dbt-view-importer.git
```

## Usage

### Basic Command Structure

```bash
bq2dbt [command] [options]
```

### Main Commands

#### Importing Views

##### Basic Usage (Required Options Only)

```bash
bq2dbt import views \
  --project-id <PROJECT_ID> \
  --dataset <DATASET_ID> \
  --output-dir <OUTPUT_DIR>
```

##### Example with All Options

```bash
bq2dbt import views \
  --project-id <PROJECT_ID> \
  --dataset <DATASET_ID> \
  --output-dir <OUTPUT_DIR> \
  --naming-preset full \
  --include-views "report_*,mart_*" \
  --exclude-views "temp_*,test_*" \
  --include-dependencies \
  --max-depth 3 \
  --sql-template <template_file> \
  --yml-template <template_file> \
  --yml-prefix <prefix_string> \
  --location asia-northeast1 \
  --non-interactive \
  --dry-run \
  --debug
```

##### Option Details

###### Required Options

- `--project-id <PROJECT_ID>`
  - BigQuery project ID
  - Example: `--project-id my-gcp-project`

- `--dataset <DATASET_ID>`
  - Target BigQuery dataset for import
  - Example: `--dataset my_dataset`

- `--output-dir <OUTPUT_DIR>`
  - Output directory for dbt models
  - Example: `--output-dir models/staging`

###### Naming Options

- `--naming-preset <PRESET>`
  - Model naming convention preset
  - Choices: `full` (default), `table_only`, `dataset_without_prefix`
  - `table_only`: Uses only the table name (e.g., `sales.revenue` → `revenue.sql`)
  - `full`: Uses dataset and table name (e.g., `sales.revenue` → `sales__revenue.sql`)
  - `dataset_without_prefix`: Removes prefix from dataset and combines with table name using `__` (e.g., `dm_sales.revenue` → `sales__revenue.sql`)

###### Filtering Options

- `--include-views <PATTERNS>`
  - View name patterns to include (comma-separated)
  - Example: `--include-views "*.sample_dataset.*,mart_*"`

- `--exclude-views <PATTERNS>`
  - View name patterns to exclude (comma-separated)
  - Example: `--exclude-views "*.temp_dataset.*,test_*"`

- `--non-interactive`
  - Flag to skip interactive confirmations
  - When specified, all views are converted without user confirmation

###### Dependency Options

- `--include-dependencies`
  - Flag to include dependent views in the import
  - When specified, other views that selected views depend on are automatically imported

- `--max-depth <DEPTH>`
  - Maximum depth of dependencies (when using `--include-dependencies`)
  - Default: 3
  - Example: `--max-depth 5` (retrieves deeper dependencies)

###### Template Options

- `--sql-template <TEMPLATE_FILE>`
  - Jinja2 template file for SQL models
  - Uses default template if not specified
  - Example: `--sql-template templates/custom_model.sql`

- `--yml-template <TEMPLATE_FILE>`
  - Jinja2 template file for YAML models
  - Uses default template if not specified
  - Example: `--yml-template templates/custom_model.yml`

- `--yml-prefix <PREFIX_STRING>`
  - Specifies a prefix for yml files
  - Example: `--yml-prefix _` -> generates `_model_name.yml`

###### Execution Options

- `--dry-run`
  - Flag to run without creating files
  - When specified, only displays target views and output destinations

- `--location <LOCATION>`
  - BigQuery location
  - Default: `asia-northeast1`
  - Example: `--location us-central1`

- `--debug`
  - Flag to enable debug mode
  - When specified, displays more detailed log information

#### Viewing Logs

```bash
# Display a list of recent logs
bq2dbt logs list

# Display the most recent log
bq2dbt logs show --last
```

### Interactive Mode

By default, the tool runs in interactive mode, which includes the following confirmations:

1. Display of detected views
2. Analysis and display of dependencies
3. Confirmation for importing each view
4. Confirmation for overwriting existing files

To use non-interactive mode, specify the `--non-interactive` option:

```bash
bq2dbt import views \
  --project-id <PROJECT_ID> \
  --dataset <DATASET_ID> \
  --output-dir <OUTPUT_DIR> \
  --non-interactive
```

### Template Customization

dbt-view-importer allows you to customize templates used for generating SQL and YAML models. This enables you to apply organization-specific naming conventions and dbt settings.

#### Default Templates

##### SQL Model Template (model.sql)

```sql
-- dbt model configuration
{{ "{{" }}
    config(
        materialized='view'
    )
{{ "}}" }}

-- Original BigQuery view: {{ source_view }}
-- Generated by bq2dbt at {{ timestamp }}

{{ sql_definition }}
```

##### YAML Model Template (model.yml)

```yaml
version: 2

models:
  - name: {{ model_name }}
    description: |
      {{ description | default('') }}
    columns:
{%- for column in columns %}
      - name: {{ column.name }}
        description: |
          {{ column.description | default('') }}
{%- endfor %}
```

#### Creating Custom Templates

When creating your own templates, you can use the following variables:

##### Variables Available in SQL Templates

- `{{ source_view }}`: Fully qualified name of the original BigQuery view (e.g., `project.dataset.view_name`)
- `{{ timestamp }}`: Timestamp of generation
- `{{ sql_definition }}`: SQL definition of the BigQuery view

##### Variables Available in YAML Templates

- `{{ model_name }}`: dbt model name
- `{{ description }}`: View description
- `{{ columns }}`: List of column information (each column has `name` and `description` attributes)

#### Custom Template Examples

##### Custom SQL Template Example

```sql
-- dbt model configuration
{{ "{{" }}
    config(
        enabled=true,
        dataset="{{ source_view.split('.')[1] }}",
        alias="{{ source_view.split('.')[2] }}",
        materialized="view"
    )
{{ "}}" }}

-- Original BigQuery view: {{ source_view }}
-- Generated by bq2dbt at {{ timestamp }}
-- This model is part of the data warehouse

{{ sql_definition }}
```

##### Custom YAML Template Example

```yaml
version: 2

models:
  - name: {{ model_name }}
    description: "{{ description | default('') }}"
    config:
      persist_docs:
        relation: true
        columns: true
      meta:
        source: "BigQuery"
        source_view: "{{ source_view }}"
    columns:
{%- for column in columns %}
      - name: {{ column.name }}
        description: "{{ column.description | default('') }}"
{%- endfor %}
```

#### Using Templates

To use custom templates, specify the paths to the template files with the `--sql-template` and `--yml-template` options:

```bash
bq2dbt import views \
  --project-id <PROJECT_ID> \
  --dataset <DATASET_ID> \
  --output-dir <OUTPUT_DIR> \
  --sql-template path/to/custom_model.sql \
  --yml-template path/to/custom_model.yml
```

### Debug Mode

You can use debug mode to display more detailed information:

```bash
bq2dbt import views \
  --project-id <PROJECT_ID> \
  --dataset <DATASET_ID> \
  --output-dir <OUTPUT_DIR> \
  --debug
```

## Developer Information

### Clone and Installation

```bash
# Clone the repository
git clone https://github.com/K-Oxon/dbt-view-importer.git
cd dbt-view-importer

# Install in development mode using uv
uv sync
```

### Running Tests

```bash
# Run all tests
./scripts/run_tests.sh

# Run tests with BigQuery integration (environment variables required)
export BQ_PROJECT_ID=your-project
export BQ_DATASET_ID=your_dataset
./scripts/run_tests.sh --with-bq

# Run specific tests
./scripts/run_tests.sh "" tests/converter/test_generator.py
```

### Debug Import

```bash
# Set environment variables
export BQ_PROJECT_ID=your-project
export BQ_DATASET_ID=your_dataset

# Run import in debug mode
./scripts/debug_import.sh output_dir
```

## Project Structure

```
src/bq2dbt/
├── cli.py                # CLI entry point
├── commands/             # Command definitions
│   ├── importer.py       # Import command group
│   ├── import_views.py   # View import command
│   └── logs.py           # Log command
├── converter/            # Conversion logic
│   ├── bigquery.py       # BigQuery client
│   ├── dependency.py     # Dependency analysis
│   ├── generator.py      # Model generation
│   ├── importer.py       # Import business logic
│   └── lineage.py        # Lineage API integration
├── templates/            # Templates
│   ├── model.sql         # SQL model template
│   └── model.yml         # YAML model template
└── utils/                # Utilities
    ├── logging.py        # Logging
    └── naming.py         # Naming conventions
```

## Architecture

This tool consists of the following components:

### Command Layer
- `commands/importer.py`: Defines the import command group
- `commands/import_views.py`: Defines the view import command
- `commands/logs.py`: Defines the log display command

### Business Logic Layer
- `converter/importer.py`: Implements import processing business logic
- `converter/bigquery.py`: Provides integration with BigQuery
- `converter/lineage.py`: Provides integration with Data Catalog Lineage API
- `converter/dependency.py`: Provides dependency analysis functionality
- `converter/generator.py`: Provides dbt model generation functionality

### Utility Layer
- `utils/naming.py`: Provides naming convention utilities
- `utils/logging.py`: Provides logging functionality

This layered architecture clearly separates business logic from the user interface, improving code maintainability and extensibility.

## License

This project is released under the MIT License.
