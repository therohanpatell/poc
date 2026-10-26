"""Each production script must work when it is the only local Python file."""

import ast
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("name", ["backup.py", "restore.py"])
def test_no_project_imports_and_standalone_help(name, tmp_path):
    script = ROOT / name
    tree = ast.parse(script.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names = [alias.name.split(".")[0] for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            assert node.level == 0
            names = [(node.module or "").split(".")[0]]
        else:
            continue
        assert all(module in sys.stdlib_module_names or module in {"google", "__future__"} for module in names)
    isolated = tmp_path / name
    shutil.copy2(script, isolated)
    env = {key: value for key, value in os.environ.items() if key != "PYTHONPATH"}
    result = subprocess.run([sys.executable, str(isolated), "--help"],
                            cwd=tmp_path, env=env, capture_output=True, text=True, timeout=90)
    assert result.returncode == 0, result.stderr
    assert "usage:" in result.stdout
    assert not result.stderr

