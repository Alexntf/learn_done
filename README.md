# Getting started
## Setup
**DuckDB**
Create DB - `duckdb tennis.db`
**dbt**
`dbt init`
Edit `~ .dbt/.profile`
```yaml
break:
  outputs:
    dev:
      type: duckdb
      path: /Users/your_profile/learn_done/tennis.db
      threads: 1
  target: dev
```
Add `sources` folder in dbt project (cf. break)
**streamlit**
