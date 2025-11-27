from vearch import Config, BackupConfig
import pytest

def test_config_initialization():
    backup_config = BackupConfig(
        cluster_name="test_cluster",
        cluster_id=10,
        db_name="test_db",
        db_id=2,
        space_name="test_space",
        partition_num=3,
        replica_num=2,
        backup_id=42
    )
    config = Config(path="data", log_dir="logs", space_name =backup_config.space_name, backup_config=backup_config)
    assert config.path == "data"
    assert config.log_dir == "logs"
    assert config.backup.cluster_name == "test_cluster"
    assert config.backup.cluster_id == 10
    assert config.backup.db_name == "test_db"
    assert config.backup.db_id == 2
    assert config.backup.space_name == "test_space"
    assert config.backup.partition_num == 3
    assert config.backup.replica_num == 2
    assert config.backup.backup_id == 42

def test_config_to_dict():
    backup_config = BackupConfig(
        cluster_name="test_cluster",
        cluster_id=10,
        db_name="test_db",
        db_id=2,
        space_name="test_space",
        partition_num=3,
        replica_num=2,
        backup_id=42
    )
    config = Config(path="data", log_dir="logs", backup_config=backup_config)
    config_dict = config.to_dict()
    assert config_dict == {
        "path": "data",
        "log_dir": "logs",
        "space_name": "test_space",
        "backup": {
            "cluster_name": "test_cluster",
            "cluster_id": 10,
            "db_name": "test_db",
            "db_id": 2,
            "space_name": "test_space",
            "backup_id": 42,
            "partition_num": 3,
            "replica_num": 2
        }
    }

def test_config_with_empty_log_dir():
    with pytest.raises(ValueError):
        Config(path="data", log_dir="")

def test_config_space_name_inheritance():
    backup_config = BackupConfig(
        cluster_name="test_cluster",
        cluster_id=10,
        db_name="test_db",
        db_id=2,
        space_name="backup_space",
        partition_num=3,
        replica_num=2,
        backup_id=42
    )
    # Config without explicit space_name, should inherit from BackupConfig
    config = Config(path="data", log_dir="logs", backup_config=backup_config)
    assert config.space_name == "backup_space"

    # Config with explicit space_name, should override BackupConfig
    config = Config(path="data", log_dir="logs", space_name="explicit_space", backup_config=backup_config)
    assert config.space_name == "explicit_space"
    assert config.backup.space_name == "explicit_space"
