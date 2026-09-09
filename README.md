ecommerce-pipeline/
├── dbt_project/                  # All dbt models, tests, macros
│   ├── models/
│   ├── tests/
│   ├── macros/
│   └── dbt_project.yml
├── airflow/
│   ├── dags/
│   │   └── ecommerce_dag.py
│   ├── plugins/                  # Custom Airflow plugins
│   └── Dockerfile
├── scripts/
│   ├── generate_clickstream.py
│   ├── pyspark_loader.py
│   └── data_quality_check.py
├── docker-compose.yml
├── requirements.txt
└── README.md