import os
import subprocess

import requests


def start_server():
    return subprocess.Popen(
        ["functions-framework", "--target", "main"],
        cwd=os.path.dirname(__file__),
        stdout=subprocess.PIPE,
    )


def test_auto():
    process = start_server()
    try:
        with requests.get("http://localhost:8080") as r:
            res = r.json()
        for i in res["results"]:
            assert i["output_rows"] > 0
    except:
        raise AssertionError
    finally:
        process.kill()
        process.wait()
