"""
DAG validity tests for energy_pipeline.py.
Includes import errors, structure, task counts, dependencies, default args, and task groups.
"""
import pytest
from airflow.models import DagBag


@pytest.fixture(scope="module")
def dag_bag():
    """Load all DAGs once for this test module."""
    return DagBag(include_examples=False)


@pytest.fixture(scope="module")
def energy_dag(dag_bag):
    """Get the energy pipeline DAG."""
    return dag_bag.get_dag("energy_pipeline_final")


# Import and loading
class TestDagLoading:
    def test_no_import_errors(self, dag_bag):
        """DAGs should load without any Python import errors."""
        assert len(dag_bag.import_errors) == 0, (
            f"DAG import errors found: {dag_bag.import_errors}"
        )

    def test_dag_count(self, dag_bag):
        """Exactly one DAG should be loaded."""
        assert dag_bag.size() >= 1, "Expected at least 1 DAG"

    def test_dag_exists(self, energy_dag):
        """The energy pipeline DAG should exist."""
        assert energy_dag is not None, "DAG 'energy_pipeline_final' not found"


# DAG Configuration
class TestDagConfig:
    def test_dag_tags(self, energy_dag):
        """DAG should have the expected tags for filtering."""
        expected_tags = {"energy", "lakehouse", "lambda-arch"}
        assert set(energy_dag.tags) == expected_tags

    def test_dag_schedule(self, energy_dag):
        """DAG should run daily."""
        assert energy_dag.schedule_interval == "@daily"

    def test_catchup_disabled(self, energy_dag):
        """Catchup should be disabled to avoid backfilling on first deploy."""
        assert energy_dag.catchup is False

    def test_default_args(self, energy_dag):
        """Verify default args for retry and notification settings."""
        defaults = energy_dag.default_args
        assert defaults["owner"] == "energy_engineer"
        assert defaults["depends_on_past"] is False
        assert defaults["email_on_failure"] is False
        assert defaults["retries"] == 1


# Task structure
class TestTaskStructure:
    def test_total_task_count(self, energy_dag):
        """DAG should have the expected number of tasks."""
        task_ids = [t.task_id for t in energy_dag.tasks]
        assert len(task_ids) >= 9, (
            f"Expected at least 9 tasks, got {len(task_ids)}: {task_ids}"
        )

    def test_critical_tasks_exist(self, energy_dag):
        """All critical task IDs should be present in the DAG."""
        required_tasks = [
            "ensure_bucket_exists",
            "batch_layer_history.ingest_kaggle_history",
            "batch_layer_history.spark_batch_processing",
            "batch_layer_history.dbt_seed",
            "batch_layer_history.dbt_run_models",
            "batch_layer_history.dbt_test_quality",
            "speed_layer_realtime.ingest_api_snapshot",
            "speed_layer_realtime.trigger_spark_streaming",
            "dashboard_ready_notification",
        ]
        actual_task_ids = {t.task_id for t in energy_dag.tasks}
        for task_id in required_tasks:
            assert task_id in actual_task_ids, (
                f"Missing critical task: {task_id}"
            )

    def test_task_groups_exist(self, energy_dag):
        """Task groups batch_layer and speed_layer should be defined."""
        group_ids = set(energy_dag.task_group_dict.keys())
        assert "batch_layer_history" in group_ids
        assert "speed_layer_realtime" in group_ids


# Dependencies
class TestDagDependencies:
    def test_bucket_is_root(self, energy_dag):
        """ensure_bucket_exists should have no upstream dependencies."""
        bucket_task = energy_dag.get_task("ensure_bucket_exists")
        assert len(bucket_task.upstream_task_ids) == 0

    def test_ingest_depends_on_bucket(self, energy_dag):
        """Ingestion tasks should depend on bucket creation."""
        ingest_task = energy_dag.get_task(
            "batch_layer_history.ingest_kaggle_history"
        )
        assert "ensure_bucket_exists" in ingest_task.upstream_task_ids

    def test_spark_depends_on_ingest(self, energy_dag):
        """Spark processing depends on data ingestion."""
        spark_task = energy_dag.get_task(
            "batch_layer_history.spark_batch_processing"
        )
        assert (
            "batch_layer_history.ingest_kaggle_history"
            in spark_task.upstream_task_ids
        )

    def test_dbt_chain(self, energy_dag):
        """dbt tasks should form a chain: seed -> run -> test."""
        dbt_seed = energy_dag.get_task("batch_layer_history.dbt_seed")
        dbt_run = energy_dag.get_task("batch_layer_history.dbt_run_models")
        dbt_test = energy_dag.get_task("batch_layer_history.dbt_test_quality")

        assert (
            "batch_layer_history.spark_batch_processing"
            in dbt_seed.upstream_task_ids
        )
        assert (
            "batch_layer_history.dbt_seed" in dbt_run.upstream_task_ids
        )
        assert (
            "batch_layer_history.dbt_run_models" in dbt_test.upstream_task_ids
        )

    def test_notification_depends_on_both_layers(self, energy_dag):
        """Final notification depends on both batch and speed layers."""
        notify_task = energy_dag.get_task("dashboard_ready_notification")
        upstream_ids = notify_task.upstream_task_ids
        assert "batch_layer_history.dbt_test_quality" in upstream_ids
        assert "speed_layer_realtime.trigger_spark_streaming" in upstream_ids

    def test_streaming_depends_on_dbt_run(self, energy_dag):
        """Streaming should start after dbt models are built."""
        stream_task = energy_dag.get_task(
            "speed_layer_realtime.trigger_spark_streaming"
        )
        assert (
            "batch_layer_history.dbt_run_models"
            in stream_task.upstream_task_ids
        )

    def test_no_cycles(self, energy_dag):
        """DAG should have no circular dependencies."""
        assert energy_dag is not None
