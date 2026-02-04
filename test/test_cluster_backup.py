import requests
import json
import os
import subprocess
import time
import pytest
from minio import Minio
from minio.error import S3Error
from utils.vearch_utils import *
from utils.data_utils import *


__description__ = """ test case for cluster level backup and restore """


class TestClusterBackup:
    @pytest.fixture(autouse=True)
    def setup_class(self):
        sift10k = DatasetSift10K()
        self.xb = sift10k.get_database()
        self.xq = sift10k.get_queries()
        self.gt = sift10k.get_groundtruth()
        
        self.db_name = "cluster_backup_db"
        self.space_names = ["space1", "space2", "space3"]
        
        self.endpoint = os.getenv("S3_ENDPOINT", "127.0.0.1:10000")
        self.server_endpoint = self._resolve_server_endpoint()
        self.access_key = os.getenv("S3_ACCESS_KEY", "minioadmin")
        self.secret_key = os.getenv("S3_SECRET_KEY", "minioadmin")
        self.use_ssl_str = os.getenv("S3_USE_SSL", "False")
        self.secure = self.use_ssl_str.lower() in ['true', '1']
        self.region = os.getenv("S3_REGION", "")
        self.cluster_name = os.getenv("CLUSTER_NAME", "vearch")
        self.bucket_name = os.getenv("S3_BUCKET_NAME", "test")

        # Log S3 configuration for debugging
        logger.info(
            f"S3 Configuration: endpoint={self.endpoint}, server_endpoint={self.server_endpoint}, "
            f"bucket={self.bucket_name}, use_ssl={self.secure}, region={self.region}"
        )

        # Ensure S3 bucket exists before running tests
        self.ensure_bucket_exists()
        self.verify_server_s3_connectivity()

    def ensure_bucket_exists(self):
        """Ensure the S3 bucket exists, create it if it doesn't"""
        try:
            client = Minio(
                self.endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=self.secure,
                region=self.region if self.region else None,
            )
            
            if not client.bucket_exists(self.bucket_name):
                logger.info(f"Bucket '{self.bucket_name}' does not exist, creating it...")
                client.make_bucket(self.bucket_name)
                logger.info(f"Bucket '{self.bucket_name}' created successfully")
            else:
                logger.info(f"Bucket '{self.bucket_name}' already exists")
        except S3Error as err:
            logger.error(f"Error ensuring bucket exists: {err}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error ensuring bucket exists: {e}")
            raise

    def _detect_vearch_container(self):
        candidates = [
            os.getenv("VEARCH_ROUTER_CONTAINER"),
            "vearch",
            "vearch-router1",
            "vearch-standalone",
            "vearch-router2",
        ]
        for name in candidates:
            if not name:
                continue
            result = subprocess.run(
                ["docker", "inspect", "-f", "{{.State.Running}}", name],
                capture_output=True,
                text=True,
                check=False,
            )
            if result.returncode == 0 and result.stdout.strip() == "true":
                return name
        return None

    def verify_server_s3_connectivity(self):
        if os.getenv("SKIP_S3_SERVER_CHECK", "").lower() in ["1", "true", "yes"]:
            logger.info("Skip server S3 connectivity check (SKIP_S3_SERVER_CHECK set)")
            return

        docker_check = subprocess.run(
            ["docker", "ps"],
            capture_output=True,
            text=True,
            check=False,
        )
        if docker_check.returncode != 0:
            logger.warning("Docker not available; skip server S3 connectivity check")
            return

        container = self._detect_vearch_container()
        if not container:
            logger.warning("No running Vearch container found; skip server S3 connectivity check")
            return

        url = f"http://{self.server_endpoint}/minio/health/live"
        logger.info(f"Checking S3 connectivity from container {container}: {url}")
        result = subprocess.run(
            ["docker", "exec", container, "curl", "-sf", url],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            message = (
                "S3 connectivity check failed from Vearch container. "
                f"container={container}, url={url}, stdout={result.stdout.strip()}, "
                f"stderr={result.stderr.strip()}"
            )
            logger.error(message)
            raise AssertionError(message)
        logger.info(f"S3 connectivity check passed from container {container}: {url}")

    def _dump_vearch_logs_on_failure(self, reason):
        if os.getenv("SKIP_VEARCH_LOG_DUMP", "").lower() in ["1", "true", "yes"]:
            logger.info("Skip Vearch log dump (SKIP_VEARCH_LOG_DUMP set)")
            return

        logger.error(f"Dumping Vearch logs due to failure: {reason}")
        ps_result = subprocess.run(
            ["docker", "ps", "-a"],
            capture_output=True,
            text=True,
            check=False,
        )
        if ps_result.returncode == 0:
            logger.error(f"==== docker ps -a ====\n{ps_result.stdout.strip()}")
        else:
            logger.error(f"==== docker ps -a failed ====\n{ps_result.stderr.strip()}")
        containers = [
            "minio",
            "vearch",
            "vearch-router1",
            "vearch-router2",
            "vearch-master1",
            "vearch-master2",
            "vearch-master3",
            "vearch-ps1",
            "vearch-ps2",
            "vearch-ps3",
        ]
        for name in containers:
            inspect = subprocess.run(
                ["docker", "inspect", "-f", "{{.State.Status}}", name],
                capture_output=True,
                text=True,
                check=False,
            )
            if inspect.returncode != 0:
                logger.error(f"==== docker inspect {name} failed ====\n{inspect.stderr.strip()}")
                continue
            logger.error(f"==== docker inspect {name} status ====\n{inspect.stdout.strip()}")
            result = subprocess.run(
                ["docker", "logs", "--tail", "200", name],
                capture_output=True,
                text=True,
                check=False,
            )
            if result.returncode == 0:
                logger.error(f"==== docker logs {name} ====\n{result.stdout.strip()}")
            else:
                logger.error(f"==== docker logs {name} failed ====\n{result.stderr.strip()}")

            if name == "minio":
                continue
            # Also try to dump in-container log files if present
            exec_result = subprocess.run(
                [
                    "docker",
                    "exec",
                    name,
                    "sh",
                    "-c",
                    "if [ -d /vearch/logs ]; then ls -la /vearch/logs; "
                    "for f in /vearch/logs/*; do echo \"==== $f ====\"; tail -n 200 \"$f\"; done; "
                    "else echo \"no /vearch/logs\"; fi",
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            if exec_result.returncode == 0:
                logger.error(f"==== docker exec {name} /vearch/logs ====\n{exec_result.stdout.strip()}")
            else:
                logger.error(f"==== docker exec {name} /vearch/logs failed ====\n{exec_result.stderr.strip()}")

    def _resolve_server_endpoint(self):
        """
        Resolve the S3 endpoint used by Vearch server.
        Priority:
          1) S3_ENDPOINT_SERVER env
          2) If client endpoint is localhost/127.0.0.1, assume server is in Docker -> minio:9000
          3) Fallback to client endpoint
        """
        explicit = os.getenv("S3_ENDPOINT_SERVER")
        if explicit:
            return explicit

        endpoint = os.getenv("S3_ENDPOINT", "127.0.0.1:10000")
        lower = endpoint.lower()
        if lower.startswith("127.0.0.1") or lower.startswith("localhost"):
            return "minio:9000"
        return endpoint

    def backup_db(self, router_url, command, version_id=None):
        url = router_url + "/backup/dbs/" + self.db_name

        data = {
            "command": command,
            "backup_id": 0,
            "s3_param": {
                "access_key": self.access_key,
                "secret_key": self.secret_key,
                "bucket_name": self.bucket_name,
                "endpoint": self.server_endpoint,
                "region": self.region,
                "use_ssl": self.secure
            },
        }
        
        if command == "restore" and version_id:
            data["version_id"] = version_id

        url = router_url + "/backup/dbs/" + self.db_name + "?timeout=100000"
        try:
            response = requests.post(url, auth=(username, password), json=data)
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error during database {command}: {e}")
            logger.error(f"URL: {url}")
            self._dump_vearch_logs_on_failure(f"ConnectionError during database {command}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during database {command}: {e}")
            self._dump_vearch_logs_on_failure(f"Unexpected error during database {command}")
            raise

        assert response.json()["code"] == 0
        result = response.json()["data"]
        version_id = result.get("version_id")
        backup_id = result.get("backup_id", 0)
        
        if version_id:
            logger.info(f"Cluster backup/restore started: version_id={version_id}, backup_id={backup_id}")
            return version_id
        else:
            logger.warning("No version_id in response, using backup_id")
            return backup_id

    def get_backup_progress_for_space(self, router_url, space_name, version_id):
        url = router_url + f"/backup/dbs/{self.db_name}/spaces/{space_name}/versions/{version_id}/progress"
        response = requests.get(url, auth=(username, password))
        
        if response.status_code != 200:
            logger.error(f"Failed to get backup progress for {space_name}: {response.text}")
            return None
        
        result = response.json()
        if result.get("code") != 0:
            logger.error(f"Failed to get backup progress for {space_name}: {result}")
            return None
        
        return result.get("data", {})

    def get_restore_progress_for_space(self, router_url, space_name):
        url = router_url + f"/restore/dbs/{self.db_name}/spaces/{space_name}/progress"
        response = requests.get(url, auth=(username, password))
        
        if response.status_code != 200:
            logger.error(f"Failed to get restore progress for {space_name}: {response.text}")
            return None
        
        result = response.json()
        if result.get("code") != 0:
            logger.error(f"Failed to get restore progress for {space_name}: {result}")
            return None
        
        return result.get("data", {})

    def waiting_backup_finish_for_all_spaces(self, router_url, version_id, timewait=5, max_wait_time=600):
        start_time = time.time()
        completed_spaces = set()
        
        while len(completed_spaces) < len(self.space_names):
            for space_name in self.space_names:
                if space_name in completed_spaces:
                    continue
                    
                progress = self.get_backup_progress_for_space(router_url, space_name, version_id)
                
                if progress is None:
                    logger.warning(f"Failed to get backup progress for {space_name}, using fallback method")
                    url = router_url + "/dbs/" + self.db_name + "/spaces/" + space_name
                    response = requests.get(url, auth=(username, password))
                    if response.json()["code"] == 0:
                        partitions = response.json()["data"]["partitions"]
                        backup_status = 0
                        for p in partitions:
                            if p.get("backup_status", 0) != 0:
                                backup_status = p["backup_status"]
                        if backup_status == 0:
                            logger.info(f"Backup finished for {space_name} (fallback method)")
                            completed_spaces.add(space_name)
                else:
                    status = progress.get("status", "")
                    total_tasks = progress.get("total_tasks", 0)
                    completed_tasks = progress.get("completed_tasks", 0)
                    success_ratio = progress.get("success_ratio", 0.0)
                    
                    logger.info(f"Backup progress for {space_name}: {completed_tasks}/{total_tasks} tasks completed, "
                              f"success_ratio={success_ratio:.2f}, status={status}")
                    
                    if status == "completed":
                        logger.info(f"Backup finished successfully for {space_name} (status: completed)")
                        completed_spaces.add(space_name)
                    elif status == "failed":
                        logger.error(f"Backup failed for {space_name} (status: failed)")
                        raise Exception(f"Backup failed for {space_name}")
                    elif total_tasks > 0 and completed_tasks >= total_tasks and success_ratio >= 1.0:
                        logger.info(f"Backup finished successfully for {space_name}")
                        completed_spaces.add(space_name)
                    elif total_tasks > 0 and completed_tasks >= total_tasks and success_ratio == 0.0:
                        logger.error(f"Backup failed for {space_name}: all tasks completed but success_ratio is 0")
                        raise Exception(f"Backup failed for {space_name}: success_ratio is 0")
            
            if time.time() - start_time > max_wait_time:
                logger.error(f"Backup timeout after {max_wait_time} seconds. Completed: {len(completed_spaces)}/{len(self.space_names)}")
                raise Exception(f"Backup timeout after {max_wait_time} seconds")
            
            if len(completed_spaces) < len(self.space_names):
                time.sleep(timewait)
        
        logger.info(f"All spaces backup completed: {completed_spaces}")

    def waiting_restore_finish_for_all_spaces(self, router_url, timewait=5, max_wait_time=600):
        start_time = time.time()
        completed_spaces = set()
        
        while len(completed_spaces) < len(self.space_names):
            for space_name in self.space_names:
                if space_name in completed_spaces:
                    continue
                    
                progress = self.get_restore_progress_for_space(router_url, space_name)
                
                if progress is None:
                    logger.warning(f"Failed to get restore progress for {space_name}")
                else:
                    status = progress.get("status", "")
                    total_tasks = progress.get("total_tasks", 0)
                    completed_tasks = progress.get("completed_tasks", 0)
                    success_ratio = progress.get("success_ratio", 0.0)
                    
                    logger.info(f"Restore progress for {space_name}: {completed_tasks}/{total_tasks} tasks completed, "
                              f"success_ratio={success_ratio:.2f}, status={status}")
                    
                    if status == "completed":
                        logger.info(f"Restore finished successfully for {space_name} (status: completed)")
                        completed_spaces.add(space_name)
                    elif status == "failed":
                        logger.error(f"Restore failed for {space_name} (status: failed)")
                        raise Exception(f"Restore failed for {space_name}")
                    elif total_tasks > 0 and completed_tasks >= total_tasks and success_ratio >= 1.0:
                        logger.info(f"Restore finished successfully for {space_name}")
                        completed_spaces.add(space_name)
                    elif total_tasks > 0 and completed_tasks >= total_tasks and success_ratio == 0.0:
                        logger.error(f"Restore failed for {space_name}: all tasks completed but success_ratio is 0")
                        raise Exception(f"Restore failed for {space_name}: success_ratio is 0")
            
            if time.time() - start_time > max_wait_time:
                logger.error(f"Restore timeout after {max_wait_time} seconds. Completed: {len(completed_spaces)}/{len(self.space_names)}")
                raise Exception(f"Restore timeout after {max_wait_time} seconds")
            
            if len(completed_spaces) < len(self.space_names):
                time.sleep(timewait)
        
        logger.info(f"All spaces restore completed: {completed_spaces}")

    def wait_for_partitions_ready(self, router_url, max_wait_time=300, timewait=5):
        """Wait for partitions to be ready after restore by checking if they can be queried"""
        logger.info("Waiting for partitions to be ready after restore")
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            all_ready = True
            for space_name in self.space_names:
                url = router_url + "/dbs/" + self.db_name + "/spaces/" + space_name
                try:
                    response = requests.get(url, auth=(username, password))
                    response_json = response.json()
                    if response_json.get("code") != 0:
                        logger.debug(f"Space {space_name} not ready yet (code != 0): {response_json}")
                        all_ready = False
                        break
                    
                    if "data" not in response_json:
                        logger.debug(f"Space {space_name} response missing 'data' field: {response_json}")
                        all_ready = False
                        break
                    
                    partitions = response_json["data"].get("partitions", [])
                    if len(partitions) == 0:
                        logger.debug(f"Space {space_name} has no partitions yet")
                        all_ready = False
                        break
                    
                    # Check if partitions have documents (indicates they're loaded)
                    total_docs = sum(p.get("doc_num", 0) for p in partitions)
                    if total_docs == 0:
                        logger.debug(f"Space {space_name} has no documents yet (total_docs=0)")
                        all_ready = False
                        break
                    
                    logger.debug(f"Space {space_name} has {total_docs} documents, partitions ready")
                except Exception as e:
                    logger.debug(f"Error checking space {space_name}: {e}")
                    all_ready = False
                    break
            
            if all_ready:
                logger.info("All partitions are ready after restore")
                # Additional wait to ensure partitions are fully opened
                time.sleep(5)
                return True
            
            time.sleep(timewait)
        
        logger.warning(f"Timeout waiting for partitions to be ready after {max_wait_time} seconds")
        # Even if timeout, try to continue (partitions might still be loading)
        logger.warning("Continuing despite timeout - partitions may still be loading")
        time.sleep(10)
        return False

    def create_db(self, router_url):
        response = create_db(router_url, self.db_name)
        logger.info(f"Create db response: {response.json()}")

    def create_space(self, router_url, space_name, embedding_size, store_type="MemoryOnly"):
        properties = {}
        properties["fields"] = [
            {
                "name": "field_int",
                "type": "integer",
            },
            {
                "name": "field_vector",
                "type": "vector",
                "dimension": embedding_size,
                "store_type": store_type,
                "index": {
                    "name": "gamma",
                    "type": "FLAT",
                    "params": {
                        "metric_type": "L2",
                        "training_threshold": 1,
                    }
                },
            }
        ]

        space_config = {
            "name": space_name,
            "partition_num": 3,
            "replica_num": 3,
            "fields": properties["fields"]
        }

        response = create_space(router_url, self.db_name, space_config)
        logger.info(f"Create space {space_name} response: {response.json()}")

    def add_data_to_space(self, router_url, space_name, embedding_size, batch_size=100):
        total = self.xb.shape[0]
        total_batch = int(total / batch_size)
        logger.info(f"Adding data to {space_name}: dataset num: {total}, total_batch: {total_batch}, dimension: {embedding_size}")
        
        add(total_batch, batch_size, self.xb, with_id=True, db_name=self.db_name, space_name=space_name)

    def query_space(self, router_url, space_name, k=100, max_retries=10, retry_delay=5):
        query_dict = {
            "vectors": [],
            "index_params": {
                "parallel_on_queries": 0
            },
            "vector_value": False,
            "fields": ["field_int"],
            "limit": k,
            "db_name": self.db_name,
            "space_name": space_name,
        }

        for batch in [True, False]:
            retry_count = 0
            while retry_count < max_retries:
                try:
                    average, recalls = evaluate(self.xq, self.gt, k, batch, query_dict)
                    result = f"Space {space_name}, batch: {batch}, parallel_on_queries: 0, average time: {average:.2f} ms, "
                    for recall in recalls:
                        result += f"recall@{recall} = {recalls[recall] * 100:.2f}% "
                    logger.info(result)

                    assert recalls[1] >= 0.95
                    assert recalls[10] >= 1.0
                    break  # 成功则退出重试循环
                except KeyError as e:
                    # KeyError 通常是因为响应中没有 'data' 字段，可能是 partition 未完全打开
                    retry_count += 1
                    if retry_count < max_retries:
                        logger.warning(f"Search failed with KeyError (attempt {retry_count}/{max_retries}): {e}")
                        logger.warning("This usually means partition is not fully ready. Waiting for partition to open...")
                        logger.info(f"Waiting {retry_delay} seconds before retry...")
                        time.sleep(retry_delay)
                    else:
                        logger.error(f"Failed after {max_retries} retries. Partition may not be fully recovered.")
                        raise
                except Exception as e:
                    error_msg = str(e)
                    error_type = type(e).__name__
                    # 检查是否是 partition_is_closed 相关的错误
                    if ("partition_is_closed" in error_msg.lower() or 
                        "search engine is null" in error_msg.lower() or
                        "'data'" in error_msg or
                        "KeyError" in error_type):
                        retry_count += 1
                        if retry_count < max_retries:
                            logger.warning(f"Partition may not be fully ready (attempt {retry_count}/{max_retries}): {error_msg}")
                            logger.info(f"Waiting {retry_delay} seconds before retry...")
                            time.sleep(retry_delay)
                        else:
                            logger.error(f"Failed after {max_retries} retries. Partition may not be fully recovered.")
                            raise
                    else:
                        # 其他错误直接抛出
                        raise

    def verify_space_data(self, router_url, space_name, sample_size=100):
        for i in range(sample_size):
            query_dict_partition = {
                "document_ids": [str(i)],
                "limit": 1,
                "db_name": self.db_name,
                "space_name": space_name,
                "vector_value": True,
            }
            query_url = router_url + "/document/query"
            response = requests.post(
                query_url, auth=(username, password), json=query_dict_partition
            )
            assert response.json()["code"] == 0
            assert response.json()["data"]["total"] == 1
            doc = response.json()["data"]["documents"][0]
            origin_doc = {}
            origin_doc["_id"] = str(i)
            origin_doc["field_int"] = i
            origin_doc["field_vector"] = self.xb[i].tolist()
            assert doc["_id"] == origin_doc["_id"]
            assert doc["field_int"] == origin_doc["field_int"]
            assert doc["field_vector"] == origin_doc["field_vector"]

    def remove_oss_files(self, prefix, recursive=False):
        client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
            region=self.region,
        )

        try:
            found = client.bucket_exists(self.bucket_name)
            logger.info(f"Bucket {self.bucket_name} exists: {found}")
            
            if recursive:
                objects = client.list_objects(self.bucket_name, prefix=prefix, recursive=True)
                object_names = []
                for obj in objects:
                    object_names.append(obj.object_name)
                    
                if not object_names:
                    logger.info(f"No objects found with prefix '{prefix}' in bucket '{self.bucket_name}'")
                    return

                for name in object_names:
                    try:
                        client.remove_object(self.bucket_name, name)
                        logger.debug(f"Deleted object '{name}'")
                    except Exception as e:
                        logger.error(f"Error removing object '{name}': {e}")
                
                logger.info(f"Successfully processed {len(object_names)} objects with prefix '{prefix}'")
            else:
                client.remove_object(self.bucket_name, prefix)
                logger.info(f"Object '{prefix}' deleted successfully")
                
        except S3Error as err:
            logger.error(f"Error occurred: {err} bucket_name {self.bucket_name} secure {self.secure} endpoint {self.endpoint}")

    def restore_space(self, router_url, space_name, version_id):
        url = router_url + "/backup/dbs/" + self.db_name + "/spaces/" + space_name

        data = {
            "command": "restore",
            "backup_id": 0,
            "version_id": version_id,
            "s3_param": {
                "access_key": self.access_key,
                "secret_key": self.secret_key,
                "bucket_name": self.bucket_name,
                "endpoint": self.server_endpoint,
                "region": self.region,
                "use_ssl": self.secure
            },
        }

        url = router_url + "/backup/dbs/" + self.db_name + "/spaces/" + space_name + "?timeout=100000"
        try:
            response = requests.post(url, auth=(username, password), json=data)
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error during restore for {space_name}: {e}")
            logger.error(f"URL: {url}")
            self._dump_vearch_logs_on_failure(f"ConnectionError during restore {space_name}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during restore for {space_name}: {e}")
            self._dump_vearch_logs_on_failure(f"Unexpected error during restore {space_name}")
            raise

        assert response.json()["code"] == 0
        result = response.json()["data"]
        restore_version_id = result.get("version_id")
        logger.info(f"Restore started for {space_name}: version_id={restore_version_id}")
        return restore_version_id

    @pytest.mark.parametrize(["command", "corrupted_data"], [
        ["backup", False],
    ])
    def test_cluster_backup_restore(self, command: str, corrupted_data: bool):
        embedding_size = self.xb.shape[1]
        batch_size = 100
        k = 100

        total = self.xb.shape[0]
        total_batch = int(total / batch_size)
        logger.info(f"Cluster backup test: dataset num: {total}, total_batch: {total_batch}, "
                   f"dimension: {embedding_size}, spaces: {len(self.space_names)}")


        logger.info("Step 1: Creating database and spaces")
        self.create_db(router_url)
        
        # 等待数据库创建完成
        time.sleep(2)
        
        # 设置全局变量，确保 waiting_index_finish 使用正确的 db_name
        global db_name, space_name
        original_db_name = db_name
        original_space_name = space_name
        db_name = self.db_name  # 在循环开始前设置全局变量
        
        for space_name in self.space_names:
            self.create_space(router_url, space_name, embedding_size)
            # 等待 space 创建完成
            time.sleep(1)
            self.add_data_to_space(router_url, space_name, embedding_size, batch_size)
            waiting_index_finish(total, space_name=space_name)

        logger.info("Step 2: Starting cluster backup")
        version_id = self.backup_db(router_url, "backup")
        if not version_id:
            logger.error("Failed to start cluster backup")
            return
        
        logger.info(f"Waiting for cluster backup to finish, version_id: {version_id}")
        self.waiting_backup_finish_for_all_spaces(router_url, version_id)

        logger.info("Step 3: Destroying original database and spaces")
        for space_name in self.space_names:
            destroy(router_url, self.db_name, space_name)
        time.sleep(10)
        self.create_db(router_url)

        logger.info("Step 4: Starting cluster restore")
        for space_name in self.space_names:
            self.restore_space(router_url, space_name, version_id)
        
        logger.info("Waiting for cluster restore to finish")
        self.waiting_restore_finish_for_all_spaces(router_url)
        
        # 等待 partition 打开（恢复后 partition 需要时间打开）
        self.wait_for_partitions_ready(router_url)
        
        # 确保全局变量已设置
        db_name = self.db_name
        for space_name in self.space_names:
            waiting_index_finish(total, space_name=space_name)
        
        
        logger.info("Waiting for indexes to stabilize after restore...")
        time.sleep(20)
        

        logger.info("Double-checking index status before verification...")
        for space_name in self.space_names:
            url = router_url + "/dbs/" + self.db_name + "/spaces/" + space_name
            response = requests.get(url, auth=(username, password))
            if response.json()["code"] == 0:
                partitions = response.json()["data"]["partitions"]
                total_indexed = sum(p.get("index_num", 0) for p in partitions)
                total_docs = sum(p.get("doc_num", 0) for p in partitions)
                logger.info(f"Space {space_name}: indexed={total_indexed}, docs={total_docs}, expected={total}")
                if total_indexed < total:
                    logger.warning(f"Space {space_name} index count ({total_indexed}) is less than expected ({total}), waiting more...")
                    waiting_index_finish(total, space_name=space_name, timewait=3)
                    time.sleep(10)  

        logger.info("Step 5: Verifying restored data")
        for space_name in self.space_names:
            logger.info(f"Verifying space: {space_name}")
            self.query_space(router_url, space_name, k)
            self.verify_space_data(router_url, space_name, sample_size=100)

        logger.info("Step 6: Cleaning up")
        for space_name in self.space_names:
            destroy(router_url, self.db_name, space_name)
        self.remove_oss_files(f"{self.cluster_name}/backup/{self.db_name}/", recursive=True)
        
        db_name = original_db_name
        space_name = original_space_name
