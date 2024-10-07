# Getting started
## Setup
### DuckDB 
Create DB - `duckdb tennis.db`
### dbt 
**Init** `dbt init`
**DB**
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
**Sources** 
Add `sources` folder in dbt project (cf. break)

## Todo 
- League EL 🏗️
- Season EL 
- Matchs EL
