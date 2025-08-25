from lithops.config import load_config


def cmd_remove_minio(bucket: str):
    lithops_config = load_config()
    minio_config = lithops_config.get("minio", {})
    access_key = minio_config.get("access_key_id", "")
    secret_key = minio_config.get("secret_access_key", "")
    endpoint = minio_config.get("endpoint", "")

    return (
        "mc alias set myminio %s %s %s && "
        "mc rm --recursive --force myminio/%s"
    ) % (endpoint, access_key, secret_key, bucket)
