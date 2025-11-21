from vearch import Config
import pytest

def test_config_initialization():
    config = Config(path="data", log_dir="logs", space_name="test_space", db_name="test_db")
    assert config.path == "data"
    assert config.log_dir == "logs"
    assert config.space_name == "test_space"
    assert config.db_name == "test_db"

def test_config_to_dict():
    config = Config(path="data", log_dir="logs", space_name="test_space", db_name="test_db")
    config_dict = config.to_dict()
    assert config_dict == {
        "path": "data",
        "log_dir": "logs",
        "cluster_name": "default",
        "space_name": "test_space",
        "db_name": "test_db",
        "backup_id": 1,
        "is_backup_import": False
    }

def test_config_with_empty_log_dir():
    with pytest.raises(ValueError):
        Config(path="data", log_dir="")
