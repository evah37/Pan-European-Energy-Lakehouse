"""
Validation tests for dbt model files and project configuration.
"""
import pytest
import yaml
from pathlib import Path

DBT_ROOT = Path(__file__).parent.parent / "dbt_energy"
MODELS_DIR = DBT_ROOT / "models"


# SQL File Existence
class TestSqlFilesExist:
    """Verify all expected dbt model SQL files are present."""

    @pytest.mark.parametrize("sql_file", [
        "staging/stg_spark__energy.sql",
        "staging/stg_spark__price_stats.sql",
    ])
    def test_staging_models_exist(self, sql_file):
        path = MODELS_DIR / sql_file
        assert path.exists(), f"Missing staging model: {sql_file}"

    @pytest.mark.parametrize("sql_file", [
        "marts/core/dim_country.sql",
        "marts/core/dim_date.sql",
        "marts/core/dim_energy_metrics.sql",
        "marts/core/fct_energy_hourly.sql",
        "marts/core/fct_price_baselines.sql",
    ])
    def test_core_models_exist(self, sql_file):
        path = MODELS_DIR / sql_file
        assert path.exists(), f"Missing core model: {sql_file}"

    @pytest.mark.parametrize("sql_file", [
        "marts/analytics/mrt_country_green_performance.sql",
        "marts/analytics/mrt_duck_curve_monthly.sql",
        "marts/analytics/mrt_financial_risk.sql",
        "marts/analytics/mrt_price_anomaly_detection.sql",
    ])
    def test_analytics_models_exist(self, sql_file):
        path = MODELS_DIR / sql_file
        assert path.exists(), f"Missing analytics model: {sql_file}"


# YAML Schema Files
class TestYamlSchemas:
    """Verify YAML schema files exist and are well-formed."""

    @pytest.mark.parametrize("yaml_file", [
        "staging/_sources.yml",
        "staging/stg_spark__energy.yml",
        "marts/core/core.yml",
        "marts/analytics/analytics.yml",
    ])
    def test_yaml_files_exist(self, yaml_file):
        path = MODELS_DIR / yaml_file
        assert path.exists(), f"Missing YAML schema: {yaml_file}"

    @pytest.mark.parametrize("yaml_file", [
        "staging/_sources.yml",
        "staging/stg_spark__energy.yml",
        "marts/core/core.yml",
        "marts/analytics/analytics.yml",
    ])
    def test_yaml_parseable(self, yaml_file):
        """YAML files should parse without errors."""
        path = MODELS_DIR / yaml_file
        with open(path, "r") as f:
            data = yaml.safe_load(f)
        assert data is not None

    def test_sources_define_tables(self):
        """Source YAML should define at least one source with tables."""
        path = MODELS_DIR / "staging" / "_sources.yml"
        with open(path, "r") as f:
            data = yaml.safe_load(f)
        assert "sources" in data
        source = data["sources"][0]
        assert "tables" in source
        assert len(source["tables"]) >= 2  # silver_features + silver_price_profile

    def test_sources_have_external_location(self):
        """Each source table should have external_location for S3 access."""
        path = MODELS_DIR / "staging" / "_sources.yml"
        with open(path, "r") as f:
            data = yaml.safe_load(f)
        for table in data["sources"][0]["tables"]:
            meta = table.get("meta", {})
            assert "external_location" in meta, (
                f"Table '{table['name']}' missing external_location"
            )
            assert "s3://energy-lake" in meta["external_location"]


# dbt_project.yml
class TestDbtProjectConfig:
    @pytest.fixture
    def project_config(self):
        with open(DBT_ROOT / "dbt_project.yml", "r") as f:
            return yaml.safe_load(f)

    def test_project_name(self, project_config):
        assert project_config["name"] == "energy_project"

    def test_config_version(self, project_config):
        assert project_config["config-version"] == 2

    def test_model_paths(self, project_config):
        assert "models" in project_config["model-paths"]

    def test_marts_materialization(self, project_config):
        """Marts should be materialized as external (Parquet export)."""
        marts_config = project_config["models"]["energy_project"]["marts"]
        assert marts_config["+materialized"] == "external"
        assert marts_config["+format"] == "parquet"

    def test_staging_materialization(self, project_config):
        """Staging should be materialized as views."""
        staging = project_config["models"]["energy_project"]["staging"]
        assert staging["+materialized"] == "view"


# profiles.yml
class TestDbtProfiles:
    @pytest.fixture
    def profiles(self):
        with open(DBT_ROOT / "profiles.yml", "r") as f:
            return yaml.safe_load(f)

    def test_profile_exists(self, profiles):
        assert "energy_profile" in profiles

    def test_duckdb_adapter(self, profiles):
        """Should use DuckDB adapter."""
        dev_output = profiles["energy_profile"]["outputs"]["dev"]
        assert dev_output["type"] == "duckdb"

    def test_s3_extensions_loaded(self, profiles):
        """httpfs and parquet extensions should be configured."""
        dev_output = profiles["energy_profile"]["outputs"]["dev"]
        extensions = dev_output.get("extensions", [])
        assert "httpfs" in extensions
        assert "parquet" in extensions

    def test_minio_endpoint_configured(self, profiles):
        """S3 settings should point to MinIO."""
        dev_output = profiles["energy_profile"]["outputs"]["dev"]
        settings = dev_output.get("settings", {})
        assert "minio" in settings.get("s3_endpoint", "")
