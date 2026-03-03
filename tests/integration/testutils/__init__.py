import subprocess
import os
from pathlib import Path

def find_project_root() -> str:
    current = Path.cwd().resolve()
    for _ in range(10):
        if (current / "Makefile").exists():
            return str(current)
        current = current.parent
    raise RuntimeError("Could not find project root (Makefile not found)")

def wipe_infra_state(targets: str = ""):
    """
    Wipes the specified infra (redis, kafka). 
    If targets is empty, wipes both.
    """
    root_dir = find_project_root()
    
    cmd = ["make", "wipe-infra-state"]
    if targets:
        cmd.append(f"TARGETS={targets}")
        
        
    import sys
    env = os.environ.copy()
    env["AEGIS_KUBE_CONTEXT"] = "kind-aegis-intg-test"
    subprocess.run(cmd, cwd=root_dir, env=env, check=True, stdout=sys.stdout, stderr=sys.stderr)
