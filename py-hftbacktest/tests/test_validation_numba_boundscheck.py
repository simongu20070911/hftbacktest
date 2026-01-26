import os
import subprocess
import sys
import textwrap
import unittest


class TestCorrectEventOrderNumbaBoundscheck(unittest.TestCase):
    def test_correct_event_order_boundscheck(self) -> None:
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
        script = textwrap.dedent(
            """
            import os
            import sys
            import types

            pkg_path = os.path.join(os.getcwd(), "hftbacktest")
            pkg = types.ModuleType("hftbacktest")
            pkg.__path__ = [pkg_path]
            sys.modules["hftbacktest"] = pkg

            data_pkg_path = os.path.join(pkg_path, "data")
            data_pkg = types.ModuleType("hftbacktest.data")
            data_pkg.__path__ = [data_pkg_path]
            sys.modules["hftbacktest.data"] = data_pkg

            import numpy as np
            from hftbacktest.data.validation import correct_event_order, validate_event_order
            from hftbacktest.types import EXCH_EVENT, LOCAL_EVENT, event_dtype

            data = np.zeros(1, dtype=event_dtype)
            data[0]["exch_ts"] = 1
            data[0]["local_ts"] = 1

            sorted_exch_index = np.array([0], dtype=np.int64)
            sorted_local_index = np.array([0], dtype=np.int64)

            out = correct_event_order(data, sorted_exch_index, sorted_local_index)
            validate_event_order(out)
            assert out.shape[0] == 1
            assert (out[0]["ev"] & (EXCH_EVENT | LOCAL_EVENT)) == (EXCH_EVENT | LOCAL_EVENT)
            """
        )
        env = dict(os.environ)
        env["NUMBA_BOUNDSCHECK"] = "1"
        if env.get("PYTHONPATH"):
            env["PYTHONPATH"] = repo_root + os.pathsep + env["PYTHONPATH"]
        else:
            env["PYTHONPATH"] = repo_root
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=repo_root,
            env=env,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            self.fail(
                "NUMBA_BOUNDSCHECK=1 subprocess failed:\n"
                f"stdout:\n{result.stdout}\n"
                f"stderr:\n{result.stderr}"
            )
