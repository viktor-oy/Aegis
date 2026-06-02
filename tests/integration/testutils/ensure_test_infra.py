#!/usr/bin/env python3
import subprocess
import time
import os
import sys

# We dynamically add the scripts directory to sys.path to import ensure_infra
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "..", "..", ".."))
sys.path.insert(0, os.path.join(project_root, "scripts"))
from ensure_infra import wait_for_infra

def check_ready():
    # We set a short timeout here because check_ready is already called in a 300-iteration loop in main()
    return wait_for_infra("intg-test", timeout_seconds=2)





def main():
    print("\033[36mBootstrapping testing workflow...\033[0m", flush=True)
    
    env = os.environ.copy()
    env["AEGIS_KUBE_CONTEXT"] = "kind-aegis-intg-test"
    
    if check_ready():
        print("\033[36mInfrastructure already running.\033[0m", flush=True)
        try:
            return 0
        except Exception as e:
            print(f"\033[31mError ensuring Kafka topics on existing infra: {e}\033[0m", flush=True)
            print("\033[31mClearing test infrastructure so it restarts on the next run...\033[0m", flush=True)
            subprocess.run(["make", "clear-tilt-ports"], env=env, check=False)
            
            # Nuke the entire K8s cluster to ensure a clean slate. Sometimes race conditions
            # cause Tilt API calls in wait_for_infra to hang until timeout because a particular 
            # Tilt resource is not responding due to the underlying k8s resource state being corrupted.
            print("\033[31mNuking the K8s cluster to clear potentially corrupted state...\033[0m", flush=True)
            subprocess.run(["make", "test-tf-kill"], check=False)
            raise

    subprocess.run(["make", "clear-tilt-ports"], env=env, check=True)

    print("\033[36mChecking if test cluster exists...\033[0m", flush=True)
    cluster_check = subprocess.run(["kind", "get", "clusters"], capture_output=True, text=True)
    if "aegis-intg-test" not in cluster_check.stdout:
        print("\033[36mTest cluster not found. Creating via make test-tf-up...\033[0m", flush=True)
        subprocess.run(["make", "test-tf-up"], check=True)

    tilt_log_path = os.environ.get("TILT_TEST_LOG", "/tmp/aegis_tilt_test.log")
    
    print(f"\033[36mStarting infrastructure via tilt up --port 10352 (logging to {tilt_log_path})...\033[0m", flush=True)
    env_tilt = os.environ.copy()
    env_tilt["AEGIS_ENV"] = "intg-test"

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
        ["mise", "exec", "--", "tilt", "up", "--port", "10352", "--context", "kind-aegis-intg-test", "-f", "Tiltfile.infra", "--stream=true"], 
        env=env_tilt, 
        stdout=tilt_log, 
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        start_new_session=True # Detach so it stays alive after script exits
    )
    
    try:
        print("\033[36mWaiting for infrastructure to be ready...\033[0m", flush=True)
        print("Note: This can take a few minutes if Tilt is building images from scratch. Please be patient!", flush=True)
        print("      You can monitor the build progress at \033[1;36mhttp://localhost:10352\033[0m", flush=True)
        print("      You can monitor the pods by running \033[1;36mkubectl get pods -n aegis-system --context=kind-aegis-intg-test\033[0m", flush=True)
        ready = False
        for _ in range(300):
            if check_ready():
                ready = True
                break
            time.sleep(1)

        if not ready:
            print("\033[31mInfrastructure failed to become ready after 300 seconds.\033[0m", flush=True)
            raise Exception("Infrastructure timeout")
            


        print("\033[36mWaiting for pods to reach Ready state...\033[0m", flush=True)
        subprocess.run(["mise", "exec", "--", "kubectl", "--context", "kind-aegis-intg-test", "wait", "--for=condition=ready", "pod", "--all", "-n", "aegis-system", "--timeout=300s"], check=True)

        print("\033[36mInfrastructure is ready!\033[0m", flush=True)
        return 0

    except Exception as e:
        print(f"\033[31mError during infrastructure setup: {e}\033[0m", flush=True)
        print("\033[31mClearing test infrastructure...\033[0m", flush=True)
        tilt_proc.terminate()
        subprocess.run(["make", "clear-tilt-ports"], env=env, check=False)
        
        # Nuke the entire K8s cluster to ensure a clean slate. Sometimes race conditions
        # cause Tilt API calls in wait_for_infra to hang until timeout because a particular 
        # Tilt resource is not responding due to the underlying k8s resource state being corrupted.
        print("\033[31mNuking the K8s cluster to clear potentially corrupted state...\033[0m", flush=True)
        subprocess.run(["make", "test-tf-kill"], check=False)
        raise

if __name__ == "__main__":
    sys.exit(main())
