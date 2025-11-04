import requests
import sys
import logging


def main() -> int:
    result = 0
    namespace = sys.argv[1]
    command = sys.argv[2]

    log_level = "DEBUG"
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s: %(message)s",
        stream=sys.stdout,
    )

    if command == "ls":
        http_code = requests.get(
            f"http://hdfs-namenode-default-0.hdfs-namenode-default.{namespace}.svc.cluster.local:9870/webhdfs/v1/testdata.txt?user.name=stackable&op=LISTSTATUS"
        ).status_code
        if http_code != 200:
            result = 1
    elif command == "create":
        files = {
            "file": (
                "testdata.txt",
                open("/tmp/testdata.txt", "rb"),
                "text/plain",
                {"Expires": "0"},
            )
        }
        http_code = requests.put(
            f"http://hdfs-namenode-default-0.hdfs-namenode-default.{namespace}.svc.cluster.local:9870/webhdfs/v1/testdata.txt?user.name=stackable&op=CREATE",
            files=files,
            allow_redirects=True,
        ).status_code
        if http_code != 201:
            result = 1
    else:
        result = 1

    return result


if __name__ == "__main__":
    sys.exit(main())
