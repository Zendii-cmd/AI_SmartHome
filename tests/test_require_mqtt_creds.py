import os
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PY = sys.executable


def test_require_creds_env_blocking(tmp_path):
    # Run a short Python snippet which sets REQUIRE_MQTT_CREDS=1 and unsets MQTT_USER/PASS
    env = os.environ.copy()
    env.pop("MQTT_USER", None)
    env.pop("MQTT_PASS", None)
    env["REQUIRE_MQTT_CREDS"] = "1"

    # Run Python importing the module; should exit with non-zero code
    cmd = [PY, "-c", "import os, sys; os.chdir(r'{}'); os.environ['REQUIRE_MQTT_CREDS']='1'; os.environ.pop('MQTT_USER', None); os.environ.pop('MQTT_PASS', None); import ai_mqtt".format(str(PROJECT_ROOT))]
    p = subprocess.run(cmd, env=env, capture_output=True, text=True)
    # If module exits with sys.exit(1), process returncode should be non-zero
    assert p.returncode != 0
    assert "MQTT credentials required but missing" in p.stderr or "MQTT credentials required but missing" in p.stdout
