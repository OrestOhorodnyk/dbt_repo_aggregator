# dbt_repo_aggregator

dbt_repo_aggregator/
├── dags/
│   ├── dbt_repo_1/      ← submodule
│   └── dbt_repo_2/      ← submodule
├── Dockerfile
├── requirements.txt
└── .gitmodules


# add submodules

git submodule add https://github.com/OrestOhorodnyk/dbt_repo_1.git dags/dbt_repo_1
git submodule add https://github.com/OrestOhorodnyk/dbt_repo_2.git dags/dbt_repo_2


# Initialize and fetch submodules

git submodule update --init --recursive
