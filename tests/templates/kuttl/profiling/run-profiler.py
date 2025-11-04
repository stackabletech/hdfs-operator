import re
import requests
import time

EVENT_TYPE = "itimer"
PROFILING_DURATION_IN_SEC = 1


def start_profiling_and_get_refresh_header(service_url):
    prof_page = requests.get(
        f"{service_url}/prof?event={EVENT_TYPE}&duration={PROFILING_DURATION_IN_SEC}"
    )

    assert prof_page.ok, f"""Profiling could not be started.
        URL: {prof_page.request.url}
        Status Code: {prof_page.status_code}"""

    return prof_page.headers["Refresh"]


def parse_refresh_header(refresh_header):
    refresh_time_in_sec, refresh_path = refresh_header.split(";", 1)
    refresh_time_in_sec = int(refresh_time_in_sec)

    assert (
        refresh_time_in_sec == PROFILING_DURATION_IN_SEC
    ), f"""Profiling duration and refresh time should be equal.
        expected: {PROFILING_DURATION_IN_SEC}
        actual:   {refresh_time_in_sec}"""

    expected_refresh_path_pattern = (
        r"/prof-output-hadoop/async-prof-pid-\d+-itimer-\d+.html"
    )
    assert re.fullmatch(
        expected_refresh_path_pattern, refresh_path
    ), f"""The path to the flamegraph contains an unexpected pattern.
        expected pattern: {expected_refresh_path_pattern}"
        actual path:      {refresh_path}"""

    return refresh_time_in_sec, refresh_path


def wait_for_profiling_to_finish(refresh_time_in_sec):
    additional_sleep_time_in_sec = 2
    time.sleep(refresh_time_in_sec + additional_sleep_time_in_sec)


def fetch_flamegraph(service_url, refresh_path):
    flamegraph_page = requests.get(f"{service_url}{refresh_path}")

    assert flamegraph_page.ok, f"""The flamegraph could not be fetched.
        URL: {flamegraph_page.request.url}
        Status Code: {flamegraph_page.status_code}"""


def test_profiling(role, port):
    service_url = f"http://test-hdfs-{role}-default-0.test-hdfs-{role}-default:{port}"

    print(f"Test profiling on {service_url}")

    refresh_header = start_profiling_and_get_refresh_header(service_url)

    refresh_time_in_sec, refresh_path = parse_refresh_header(refresh_header)

    wait_for_profiling_to_finish(refresh_time_in_sec)

    fetch_flamegraph(service_url, refresh_path)


test_profiling(role="namenode", port=9870)
test_profiling(role="datanode", port=9864)
test_profiling(role="journalnode", port=8480)
