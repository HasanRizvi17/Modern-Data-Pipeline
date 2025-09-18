# Modern-Data-Pipeline

# Initial:
1. Create GitHub repo & connect locally
2. Create folder structure (dbt_project/, airflow_dags/, docker-compose.yml, README.md)

## Environment Setup
1. Create GCP projects: dev and prod
2. Create datasets on BigQuery (in both projects): `mdp_raw`, `mdp_stg`, `mdp_int`, `mdp_fct`, `mdp_dim`, `mdp_rep` 
3. Create service accounts fror both projects
    - give relevant access to the account while creating it
4. Generate JSON keys and download them
5. Create `.env` file with paths and IDs
5. Create `.gitignore` file