#!/usr/bin/env python3
import socket
import urllib.request
import subprocess
import time
import os
import sys

REQUIRED_PORTS = {
    "tilt": 10352,
    "kafka": 9094,
    "redis": 6379,
    "mailpit_smtp": 10250,
    "mailpit_http": 8025,
    "llm": 11434
}

def check_port(host, port):
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False

def check_http(url):
    try:
        with urllib.request.urlopen(url, timeout=1) as response:
            return response.status == 200
    except Exception:
        return False

def check_ready():
    if not check_port("localhost", REQUIRED_PORTS["kafka"]): return False
    if not check_port("localhost", REQUIRED_PORTS["redis"]): return False
    if not check_port("localhost", REQUIRED_PORTS["mailpit_smtp"]): return False
    if not check_http(f"http://localhost:{REQUIRED_PORTS['mailpit_http']}/api/v1/messages"): return False
    if not check_http(f"http://localhost:{REQUIRED_PORTS['llm']}/"): return False
    return True



def main():
    print("\033[36mBootstrapping testing workflow...\033[0m", flush=True)
    if check_ready():
        print("\033[36mInfrastructure already running.\033[0m", flush=True)
        return 0

    subprocess.run(["make", "clear-test-ports"], check=True)

    print("\033[36mChecking if test cluster exists...\033[0m", flush=True)
    cluster_check = subprocess.run(["kind", "get", "clusters"], capture_output=True, text=True)
    if "aegis-intg-test" not in cluster_check.stdout:
        print("\033[36mTest cluster not found. Creating via make test-tf-up...\033[0m", flush=True)
        subprocess.run(["make", "test-tf-up"], check=True)

    tilt_log_path = os.environ.get("TILT_TEST_LOG", "/tmp/aegis_tilt_test.log")
    
    # Increase inotify limits inside the Kind node so Tilt can successfully stream logs for all pods
    # without hitting the "failed to create fsnotify watcher: too many open files" error.
    print("\033[36mScaling inotify limits inside the test cluster node...\033[0m", flush=True)
    subprocess.run(["docker", "exec", "aegis-intg-test-control-plane", "sysctl", "-w", "fs.inotify.max_user_instances=512"], check=False, stdout=subprocess.DEVNULL)
    subprocess.run(["docker", "exec", "aegis-intg-test-control-plane", "sysctl", "-w", "fs.inotify.max_user_watches=524288"], check=False, stdout=subprocess.DEVNULL)

    print(f"\033[36mStarting infrastructure via tilt up --port 10352 (logging to {tilt_log_path})...\033[0m", flush=True)
    env = os.environ.copy()
    env["AEGIS_ENV"] = "intg-test"

    tilt_log = open(tilt_log_path, "w")
    
    # We invoke the tilt binary directly instead of calling `make tilt-test-infra-up` for three reasons:
    # 1. Process Tree Termination: Popen spawning `make` creates a process tree. Calling tilt_proc.terminate() 
    #    later would only kill `make` and leave the tilt daemon orphaned indefinitely.
    # 2. Daemon Mode: We must pass --stream=true to safely daemonize without crashing on a missing /dev/tty. 
    #    Hardcoding this in the Makefile would break manual interactive HUD usage for developers.
    # 3. Port Clearing: `make tilt-test-infra-up` first clears open ports. Calling it here would cause 
    #    subsequent test runs to needlessly restart the infrastructure services, making tests extremely slow.
    #
    # --stream=true forces tilt into pure logging mode. Without it, tilt natively 
    # attempts to connect to the terminal's /dev/tty to draw its interactive HUD, 
    # which crashes instantly since we detach it from the CLI via start_new_session=True.
    tilt_proc = subprocess.Popen(
        ["tilt", "up", "--port", "10352", "--context", "kind-aegis-intg-test", "-f", "Tiltfile.infra", "--stream=true"], 
        env=env, 
        stdout=tilt_log, 
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        start_new_session=True # Detach so it stays alive after script exits
    )
    
    print("\033[36mWaiting for infrastructure to be ready...\033[0m", flush=True)
    ready = False
    for i in range(300):
        if check_ready():
            ready = True
            break
        time.sleep(1)

    if not ready:
        print("\033[31mInfrastructure failed to become ready after 300 seconds.\033[0m", flush=True)
        tilt_proc.terminate()
        return 1
        
    print("\033[36mInfrastructure is ready!\033[0m", flush=True)
    print("\033[36mRunning make init-kafka...\033[0m", flush=True)
    env["KUBE_CONTEXT"] = "kind-aegis-intg-test"
    subprocess.run(["make", "init-kafka"], env=env)
    return 0

if __name__ == "__main__":
    sys.exit(main())
