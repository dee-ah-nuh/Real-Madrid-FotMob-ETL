# dbt YAML Files Reference Guide

A comprehensive reference for working with YAML files in dbt projects.

---

## 📁 Core YAML Files Overview

### **1. `dbt_project.yml`**
**Location:** Project root directory  
**Purpose:** Main project configuration file

**Key Sections:**
```yaml
name: 'project_name'           # Project name
version: '1.0.0'               # Project version
config-version: 2              # dbt config version
profile: 'profile_name'        # Profile to use (MUST match profiles.yml)

model-paths: ["models"]        # Where models are located
seed-paths: ["seeds"]          # Where seed files are located
test-paths: ["tests"]          # Where custom tests are located
analysis-paths: ["analyses"]   # Where analyses are located

vars:                          # Global variables
  my_variable: 'default_value'
  start_date: '2024-01-01'

models:                        # Model-level configurations
  project_name:
    staging:
      +materialized: view
    marts:
      +materialized: table
      
on-run-start:                  # Hooks to run at start
  - "{{ create_secret() }}"
  
on-run-end:                    # Hooks to run at end
  - "{{ log('Run completed') }}"
```

**Important Notes:**
- `profile` name MUST match a profile in `profiles.yml`
- `vars` defined here can be overridden with `--vars` CLI flag
- You can only pass `vars` via CLI in this file (not use `{{ var() }}`)

---

### **2. `profiles.yml`**
**Location:** `~/.dbt/` (default) or project root with `--profiles-dir .`  
**Purpose:** Database connection credentials and settings

**Basic Structure:**
```yaml
profile_name:                  # Must match dbt_project.yml
  target: dev                  # Default target
  outputs:
    dev:
      type: duckdb            # Adapter type
      path: 'dev.duckdb'      # Database path/connection
      threads: 4              # Parallel threads
      extensions:             # DuckDB extensions
        - httpfs
        - json
    
    prod:
      type: duckdb
      path: 'prod.duckdb'
      threads: 8
```

**Using Environment Variables:**
```yaml
profile_name:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('DB_HOST') }}"
      user: "{{ env_var('DB_USER') }}"
      password: "{{ env_var('DB_PASSWORD') }}"  # Use DBT_ENV_SECRET_ prefix to mask in logs
      port: 5432
      dbname: analytics
      schema: public
      threads: 4
```

**Important Notes:**
- Can use `env_var()` for credentials
- Use `DBT_ENV_SECRET_` prefix for sensitive values (automatically masked in logs)
- Can be in `~/.dbt/` globally or project root with `--profiles-dir` flag
- Cannot use `{{ var() }}` - only CLI `--vars` and `env_var()`

---

### **3. `schema.yml` (or any `.yml` in models/)**
**Location:** Anywhere in `models/` directory  
**Purpose:** Model documentation, tests, and metadata

**Full Example:**
```yaml
version: 2

sources:
  - name: raw_data
    description: "Raw data from S3"
    database: my_database
    schema: raw
    tables:
      - name: customers
        description: "Customer data"
        columns:
          - name: customer_id
            description: "Unique customer ID"
            tests:
              - unique
              - not_null
        
        freshness:
          warn_after: {count: 12, period: hour}
          error_after: {count: 24, period: hour}

models:
  - name: stg_customers
    description: "Staged customer data"
    config:
      materialized: view
      tags: ['staging']
    
    columns:
      - name: customer_id
        description: "Unique customer identifier"
        tests:
          - unique
          - not_null
      
      - name: email
        description: "Customer email"
        tests:
          - not_null
          - relationships:
              to: ref('stg_users')
              field: email
    
    tests:
      - dbt_utils.expression_is_true:
          expression: "customer_id > 0"
```

**Important Notes:**
- ✅ Supports full Jinja (including `{{ var() }}`)
- Can define tests, documentation, and configs
- Can be split across multiple files

---

### **4. `packages.yml`**
**Location:** Project root  
**Purpose:** Declare external dbt packages

**Example:**
```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
  
  - package: dbt-labs/codegen
    version: 0.12.1
  
  - git: "https://github.com/dbt-labs/dbt-audit-helper.git"
    revision: 0.9.0
  
  - local: ../local-package  # Local package
```

**Installing Packages:**
```bash
dbt deps  # Install all packages
```

**Important Notes:**
- ⚠️ Can use `env_var()` for secure values (with `DBT_ENV_SECRET_` prefix)
- Cannot use `{{ var() }}` - only CLI `--vars`
- Supports git repos and local paths

---

### **5. `dependencies.yml`**
**Location:** Project root  
**Purpose:** Alternative to `packages.yml` (newer format)

**Example:**
```yaml
projects:
  - name: dbt_utils
    version: 1.1.1
```

**Important Notes:**
- ❌ Does NOT support Jinja at all
- No variables, no `env_var()`, no macros
- Simpler than `packages.yml` but less flexible

---

## 🔧 Variable Usage Matrix

| File | `{{ var() }}` | `--vars` CLI | `{{ env_var() }}` | `DBT_ENV_SECRET_` |
|------|---------------|--------------|-------------------|-------------------|
| `dbt_project.yml` | ❌ | ✅ | ✅ | ❌ |
| `profiles.yml` | ❌ | ✅ | ✅ | ✅ |
| `schema.yml` | ✅ | ✅ | ✅ | ❌ |
| `packages.yml` | ❌ | ✅ | ✅ | ✅ |
| `dependencies.yml` | ❌ | ❌ | ❌ | ❌ |
| Model `.sql` files | ✅ | ✅ | ✅ | ❌ |

---

## 📝 Common Patterns

### **Setting Variables**

**1. Define in `dbt_project.yml`:**
```yaml
vars:
  start_date: '2024-01-01'
  environment: 'dev'
```

**2. Override via CLI:**
```bash
dbt run --vars '{"start_date": "2024-06-01", "environment": "prod"}'
```

**3. Use in models:**
```sql
SELECT *
FROM events
WHERE event_date >= '{{ var("start_date") }}'
```

---

### **Using Environment Variables**

**1. Set environment variables:**
```bash
export AWS_ACCESS_KEY_ID='your_key'
export AWS_SECRET_ACCESS_KEY='your_secret'
export DBT_ENV_SECRET_DB_PASSWORD='secret_password'  # Masked in logs
```

**2. Reference in YAML:**
```yaml
# profiles.yml
my_profile:
  outputs:
    dev:
      user: "{{ env_var('DB_USER') }}"
      password: "{{ env_var('DBT_ENV_SECRET_DB_PASSWORD') }}"  # Auto-masked
```

---

### **Configuring Models**

**Option 1: In `dbt_project.yml` (applies to all models in folder):**
```yaml
models:
  my_project:
    staging:
      +materialized: view
      +tags: ['staging']
    marts:
      +materialized: table
      +tags: ['marts']
```

**Option 2: In model file (applies to single model):**
```sql
{{ config(
    materialized='incremental',
    unique_key='id',
    tags=['daily']
) }}

SELECT * FROM ...
```

**Option 3: In `schema.yml` (applies to single model):**
```yaml
models:
  - name: my_model
    config:
      materialized: table
      tags: ['important']
```

---

## 🚀 Quick Reference Commands

### **Running dbt**
```bash
# Run all models
dbt run

# Run specific model
dbt run --select model_name

# Run models in folder
dbt run --select staging

# Run model and upstream dependencies
dbt run --select +model_name

# Run model and downstream dependencies
dbt run --select model_name+

# Exclude specific models
dbt run --exclude model_name

# Full refresh incremental models
dbt run --full-refresh

# Use different profile
dbt run --profiles-dir /path/to/profiles

# Override variables
dbt run --vars '{"key": "value"}'

# Run specific target
dbt run --target prod
```

### **Testing**
```bash
# Run all tests
dbt test

# Test specific model
dbt test --select model_name

# Test sources
dbt test --select source:*
```

### **Other Useful Commands**
```bash
# Install packages
dbt deps

# Compile SQL (don't run)
dbt compile

# Generate docs
dbt docs generate
dbt docs serve

# Debug connection
dbt debug

# Load seed files
dbt seed

# Snapshot data
dbt snapshot
```

---

## 🎯 Best Practices

### **1. Profile Management**
- ✅ Keep `profiles.yml` in `~/.dbt/` for personal credentials
- ✅ Use environment variables for sensitive data
- ✅ Use `DBT_ENV_SECRET_` prefix for passwords/keys
- ❌ Never commit credentials to git

### **2. Project Structure**
```
my_dbt_project/
├── dbt_project.yml          # Project config
├── profiles.yml             # Optional: local profiles
├── packages.yml             # Package dependencies
├── models/
│   ├── staging/
│   │   ├── schema.yml       # Staging docs/tests
│   │   └── stg_*.sql
│   └── marts/
│       ├── schema.yml       # Marts docs/tests
│       └── *.sql
├── seeds/                   # CSV files
├── tests/                   # Custom tests
├── macros/                  # Reusable SQL
└── analyses/                # Ad-hoc queries
```

### **3. Variable Organization**
- Define global defaults in `dbt_project.yml`
- Override per environment via `--vars` or target-specific configs
- Use `env_var()` for credentials and environment-specific values

### **4. Documentation**
- Document all models in `schema.yml`
- Add column descriptions
- Include business logic in descriptions
- Use tests to enforce data quality

---

## 🔍 Troubleshooting

### **"Could not find profile named 'X'"**
- Check `profile:` in `dbt_project.yml` matches profile name in `profiles.yml`
- Ensure you're running from project root
- Try `dbt debug` to diagnose connection issues

### **Variables not working**
- In `dbt_project.yml`/`profiles.yml`: Use `--vars` CLI, not `{{ var() }}`
- In `schema.yml`/models: Can use both `{{ var() }}` and `--vars`
- In `dependencies.yml`: No variables supported at all

### **Environment variables not found**
- Ensure variables are exported: `export VAR_NAME=value`
- Check spelling and case sensitivity
- Use `DBT_ENV_SECRET_` prefix for sensitive values

---

## 📚 Additional Resources

- [dbt Documentation](https://docs.getdbt.com)
- [dbt Discourse Community](https://discourse.getdbt.com)
- [dbt Packages Hub](https://hub.getdbt.com)
- [dbt Context Docs](https://github.com/dbt-labs/dbt-core/blob/main/core/dbt/context/README.md)

---

**Created:** February 1, 2026  
**Last Updated:** February 1, 2026