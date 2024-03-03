This document outlines the process for configuring Directed Acyclic Graphs (DAGs) for various data pipelines using the Apache Airflow ECSOperator. It includes details on selecting machine sizes for task execution, configuring Docker images, and setting up environment variables.

Machine Size Selection
For task execution, select an appropriate machine size from the following options, defined in the AWS ECS task definitions:

pipelines-small: 1 CPU, 2GB RAM
pipelines-medium: 1 CPU, 8GB RAM
pipelines-large: 2 CPUs, 16GB RAM
pipelines-xl: 8 CPUs, 32GB RAM
pipelines-huge: 8 CPUs, 64GB RAM
pipelines-highcpu: 16 CPUs, 32GB RAM
Docker Image Configuration
Each task utilizes a Docker image built and deployed automatically by the CI pipeline, available at 823557601923.dkr.ecr.us-east-2.amazonaws.com/chainverse/pipelines:latest. Refer to this image as data-pipelines in the ECSOperator.

DAG Configuration
The core of DAG configuration involves deploying a container into FARGATE for execution. The process is highly efficient, requiring resources only when needed and eliminating the need for manual intervention.

DAG Definition
Define the DAG object with Airflow parameters such as start_date, interval, title, and description. Customize these parameters based on your requirements.

```
dag = DAG(
    "DAG title",
    description="DAG Description.",
    default_args={
        "start_date": days_ago(2),
        "owner": "Owner name",
        "email": ["Owner Email"],
        "schedule_interval": "@daily",  # Options: @hourly, @daily, @weekly, @monthly
        "retries": 3  # Number of retries before failure
    },
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10080)  # Set a large number for long DAGs
)
```


Task Configuration
For tasks requiring different machine sizes, redefine ecs_task_definition and ecs_awslogs_group before each task. The primary setup involves selecting the ecs_task_definition from the provided options and configuring environment variables specific to the DAG.

ECSOperator Task Definition
Modify the task ID and command line in the ECSOperator task definition to reflect the specific action required. The command line instructs the Docker image on which action to execute from the pipelines module.

```
example_ingest_task = ECSOperator(
    task_id="example_ingesting",
    dag=dag,
    aws_conn_id="aws_ecs",
    cluster=ecs_cluster,
    task_definition=ecs_task_definition,
    region_name="us-east-2",
    launch_type="FARGATE",
    overrides={
        "containerOverrides": [
            {
                "name": ecs_task_image,
                "command": ["python3", "-m", "pipelines.ingestion.ens.ingest"],
                "environment": env_vars
            },
        ],
    },
    network_configuration=network_configuration,
    awslogs_group=ecs_awslogs_group,
    awslogs_stream_prefix=ecs_awslogs_stream_prefix
)
```

Creating and Activating a DAG
To create a DAG, define configurations, create ECSOperator tasks, and chain them using the Airflow >> operator. For activation, after pushing the code to GitLab, activate the DAG in the AirFlow interface by clicking the "switch" icon next to the DAG.

Environment Variables
Include essential environment variables in the DAG's environment definitions, such as ETHERSCAN_API_KEY, ALCHEMY_API_KEY, and AWS credentials. These variables must be declared in the AirFlow interface under Admin > Variables.

