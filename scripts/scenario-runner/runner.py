#!/usr/bin/env python3
import argparse
import json
import logging
import random
import subprocess
import sys
import time
from contextlib import contextmanager
from typing import Any, Iterator, Optional

import httpx
import os
import yaml
import asyncio

import datetime

def print_tilt_link_if_needed(action: dict, run_logger: logging.Logger) -> None:
    tilt_regex = action.get("show_urls_with_regex")
    if not tilt_regex:
        return
    
    import urllib.parse
    import json
    import os
    
    # Print Tilt links
    base_regex = r"\[(aegis-agent|aegis-control-plane|aegis-composer|aegis-sink)[^\]]*\]"
    final_regex = f"/{base_regex}.*({tilt_regex})/"
    encoded_query = urllib.parse.quote_plus(final_regex)
    run_logger.info(f"Hint: \033[1;36mView ALL logs at http://localhost:10354/r/(all)/overview\033[0m (useful for debugging)")
    run_logger.info(f"Hint: \033[1;36mView FILTERED scenario logs at http://localhost:10354/r/(all)/overview?term={encoded_query}\033[0m")
    run_logger.info("      (The filtered view removes unnecessary logs for clear observation of the scenario execution)")
    
    # Print Mailpit link
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        ports_path = os.path.join(script_dir, "..", "ports.json")
        with open(ports_path) as f:
            ports_data = json.load(f)
        mailpit_port = ports_data.get("scenario", {}).get("mailpit_http", 30252)
        run_logger.info(f"Hint: \033[1;36mView emails in Mailpit at http://localhost:{mailpit_port}/\033[0m")
    except Exception as e:
        run_logger.debug(f"Could not load Mailpit port: {e}")

log_file_path = f"/tmp/aegis_scenario_runner_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# Root logger captures everything and writes to file
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler(log_file_path)
file_handler.setLevel(logging.DEBUG)
file_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%H:%M:%S")
file_handler.setFormatter(file_formatter)
root_logger.addHandler(file_handler)

# Console handler filters out verbose aiokafka/kafka logs and debug logs
class ConsoleFilter(logging.Filter):
    def filter(self, record):
        if record.name.startswith("aiokafka") or record.name.startswith("kafka") or record.name.startswith("urllib3") or record.name.startswith("httpx"):
            return record.levelno >= logging.ERROR
        return True

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
console_handler.setFormatter(console_formatter)
console_handler.addFilter(ConsoleFilter())
root_logger.addHandler(console_handler)

logger = logging.getLogger("scenario_runner")
logger.info(f"\033[1;36m[LOGGING] All logs (including suppressed Kafka logs) are being saved to: \033[4m{log_file_path}\033[0m")

KIND_CLUSTER_NAME = "aegis-scenario"


def run_cmd(cmd: list[str], env: Optional[dict[str, str]] = None, check: bool = True, capture_output: bool = True, silent: bool = False) -> subprocess.CompletedProcess[str]:
    # We ALWAYS capture output internally to route it to the debug log file to keep the console clean.
    # capture_output arg is ignored for actual subprocess execution to enforce clean UX.
    result = subprocess.run(cmd, env=env, check=False, capture_output=True, text=True)
    cmd_name = os.path.basename(cmd[0]) if cmd else "cmd"
    
    if result.stdout:
        for line in result.stdout.strip().split("\n"):
            logger.debug(f"[{cmd_name}] {line}")
            
    if result.stderr:
        for line in result.stderr.strip().split("\n"):
            if result.returncode != 0 and not silent:
                logger.error(f"[{cmd_name}] {line}")
            else:
                logger.debug(f"[{cmd_name}] {line}")
                
    if check and result.returncode != 0:
        raise subprocess.CalledProcessError(result.returncode, cmd, output=result.stdout, stderr=result.stderr)
        
    return result


def apply_scenario_infra(node_count: int, cp_count: int, sink_count: int, composer_count: int, config: dict) -> None:
    import os
    import sys
    import json
    
    project_root = os.getcwd()
    overlay_dir = os.path.join(project_root, ".scenario-overlay")
    os.makedirs(overlay_dir, exist_ok=True)
    
    last_node_count_file = os.path.join(overlay_dir, "last_node_count")
    last_node_count = 0
    if os.path.exists(last_node_count_file):
        with open(last_node_count_file, "r") as f:
            try:
                last_node_count = int(f.read().strip())
            except ValueError:
                pass

    tilt_is_healthy = False
    try:
        resp = httpx.get("http://localhost:10354/api/view", timeout=2.0)
        if resp.status_code == 200:
            tilt_is_healthy = True
            logger.info("HTTP Request: GET http://localhost:10354/api/view \"HTTP/1.1 200 OK\"")
    except httpx.RequestError:
        pass

    cluster_check = run_cmd(["mise", "exec", "--", "kind", "get", "clusters"], check=False)
    cluster_exists = "aegis-scenario" in cluster_check.stdout

    if not tilt_is_healthy or last_node_count != node_count or not cluster_exists:
        if last_node_count != node_count and tilt_is_healthy:
            logger.warning(f"Node count changed from {last_node_count} to {node_count}. The cluster will be recreated. Terminating old Tilt daemon...")
        elif not cluster_exists and tilt_is_healthy:
            logger.warning("Cluster 'aegis-scenario' does not exist (likely deleted). Terminating old Tilt daemon...")
        elif not tilt_is_healthy:
            logger.info("Tilt HTTP server is unreachable or dead. Ensuring port 10354 is cleared of zombie processes...")
            
        runner_env = os.environ.copy()
        runner_env["AEGIS_KUBE_CONTEXT"] = "kind-aegis-scenario"
        run_cmd(["make", "clear-tilt-ports"], env=runner_env, check=False, capture_output=False)
        tilt_is_healthy = False
    logger.info(f"Provisioning scenario infrastructure via Terraform (Nodes={node_count})...")
    run_cmd(["make", "scenario-tf-up", f"NODE_COUNT={node_count}"], check=True, capture_output=False)
    
    with open(last_node_count_file, "w") as f:
        f.write(str(node_count))

    logger.info(f"Applying dynamic workloads via Tilt (CP={cp_count}, Sink={sink_count}, Composer={composer_count})...")
    
    kustomization_yaml = f"""
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../infra/kustomize/overlays/scenario
replicas:
  - name: aegis-control-plane
    count: {cp_count}
  - name: aegis-sink
    count: {sink_count}
  - name: aegis-composer
    count: {composer_count}
"""
    
    env_overrides = config.get("env_overrides", [])
    if env_overrides:
        kustomization_yaml += "patches:\n"
        
    patched_targets = []
    for override in env_overrides:
        target = override.get("target", "")
        if "/" not in target:
            continue
        patched_targets.append(target)
        target_kind, target_name = target.split("/", 1)
        envs = override.get("envs", {})
        
        patch_str = f"""apiVersion: apps/v1
kind: {target_kind}
metadata:
  name: {target_name}
spec:
  template:
    spec:
      containers:
        - name: {target_name}
          env:
"""
        for k, v in envs.items():
            patch_str += f"""            - name: {k}
              value: "{v}"
"""
        # Indent the patch_str to fit inside Kustomization patches block
        indented_patch = "\n".join(["      " + line for line in patch_str.strip().split("\n")])
        kustomization_yaml += f"""  - target:
      kind: {target_kind}
      name: {target_name}
    patch: |-
{indented_patch}
"""

    with open(os.path.join(overlay_dir, "kustomization.yaml"), "w") as f:
        f.write(kustomization_yaml)

    if not tilt_is_healthy:
        logger.info("Starting Tilt daemon in the background...")
        env = os.environ.copy()
        env["AEGIS_ENV"] = "scenario"
        env["AEGIS_KUSTOMIZE_OVERLAY"] = overlay_dir
        
        tilt_log_path = "/tmp/aegis_scenario_tilt.log"
        tilt_log = open(tilt_log_path, "w")
        
        subprocess.Popen(
            ["mise", "exec", "--", "tilt", "up", "--host", "0.0.0.0", "--port", "10354", "--context", "kind-aegis-scenario", "-f", "Tiltfile", "--stream=true"],
            env=env,
            stdout=tilt_log,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True
        )
    else:
        logger.info("Tilt is already running. Updates will be hot-reloaded automatically.")
        import time
        time.sleep(8) # Wait for Tilt to detect file changes and apply manifests to K8s
 
    try:
        logger.info("Polling Tilt API and local sockets to ensure all resources are fully built, deployed, and bound...")
        logger.info("Note: This can take a few minutes if Tilt is building images from scratch. Please be patient!")
        logger.info("      You can monitor the build progress at \033[1;36mhttp://localhost:10354\033[0m")
        logger.info("      You can monitor the pods by running \033[1;36mkubectl get pods -n aegis-system --context=kind-aegis-scenario\033[0m")
        logger.warning("\033[33mNote: If this polling step hangs, check the Tilt UI at http://localhost:10354 to ensure there are no build errors (e.g., Docker build failures).\033[0m")

        # We dynamically add the scripts directory to sys.path to import ensure_infra
        import sys
        parent_dir = os.path.dirname(os.path.abspath(__file__))
        scripts_dir = os.path.dirname(parent_dir)
        if scripts_dir not in sys.path:
            sys.path.insert(0, scripts_dir)
        from ensure_infra import wait_for_infra
        
        ready = wait_for_infra("scenario", timeout_seconds=600, logger=logger)
        
        if not ready:
            raise Exception("Timeout waiting for Tilt resources and local sockets to become ready!")
            
        if patched_targets:
            logger.info("Waiting for patched workloads to complete rollout (to prevent race conditions with old pods)...")
            for target in patched_targets:
                logger.info(f"Checking rollout status for {target}...")
                run_cmd(["mise", "exec", "--", "kubectl", "--context", "kind-aegis-scenario", "rollout", "status", target, "-n", "aegis-system", "--timeout=120s"], check=True, capture_output=False)
            
        logger.info("Waiting for all active pods to reach Ready state...")
        run_cmd(["mise", "exec", "--", "kubectl", "--context", "kind-aegis-scenario", "wait", "--for=condition=ready", "pod", "--all", "-n", "aegis-system", "--timeout=300s"], check=True, capture_output=False)
        logger.info("All workloads are fully synchronized and healthy.")
            
        logger.info("Flushing Redis to ensure a clean slate for the scenario (removing old incidents/deferrals)...")
        run_cmd(["mise", "exec", "--", "kubectl", "--context", "kind-aegis-scenario", "exec", "aegis-redis-master-0", "-n", "aegis-system", "--", "redis-cli", "FLUSHDB"], check=True, capture_output=False)
            
    except Exception as e:
        logger.error(f"Error during infrastructure setup: {e}")
        logger.error("Clearing test infrastructure so it restarts cleanly on the next run...")
        clear_env = os.environ.copy()
        clear_env["AEGIS_KUBE_CONTEXT"] = "kind-aegis-scenario"
        run_cmd(["make", "clear-tilt-ports"], env=clear_env, check=False, capture_output=False)
        
        # Nuke the entire K8s cluster to ensure a clean slate. Sometimes race conditions
        # cause Tilt API calls in wait_for_infra to hang until timeout because a particular 
        # Tilt resource is not responding due to the underlying k8s resource state being corrupted.
        logger.error("Nuking the K8s cluster to clear potentially corrupted state...")
        run_cmd(["make", "scenario-tf-kill"], env=clear_env, check=False, capture_output=False)
        sys.exit(1)

def get_agent_pods() -> list[str]:
    result = run_cmd(["mise", "exec", "--", "kubectl", "--context", "kind-aegis-scenario", "get", "pods", "-n", "aegis-system", "-l", "app.kubernetes.io/component=agent", "-o", "json"])
    data = json.loads(result.stdout)
    pods = [item["metadata"]["name"] for item in data.get("items", []) if "deletionTimestamp" not in item["metadata"]]
    return pods


@contextmanager
def port_forward(pod_name: str, local_port: int, remote_port: int = 8080) -> Iterator[None]:
    logger.debug(f"Starting port-forward for {pod_name} ({local_port}:{remote_port})")
    cmd = ["mise", "exec", "--", "kubectl", "--context", "kind-aegis-scenario", "port-forward", "-n", "aegis-system", f"pod/{pod_name}", f"{local_port}:{remote_port}"]
    process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    time.sleep(1.5) # Wait a bit longer for stability
    
    try:
        yield
    finally:
        logger.debug(f"Terminating port-forward for {pod_name}")
        process.terminate()
        process.wait()


def inject_fault(pod_name: str, mode: str, correlation_id: Optional[str] = None) -> None:
    local_port = random.randint(30000, 40000)
    with port_forward(pod_name, local_port):
        payload = {"mode": mode}
        if correlation_id:
            payload["correlation_id"] = correlation_id
            
        url = f"http://localhost:{local_port}/simulate"
        try:
            resp = httpx.post(url, json=payload, timeout=5.0)
            resp.raise_for_status()
            logger.info(f"Successfully injected '{mode}' into {pod_name}")
        except Exception as e:
            logger.error(f"Failed to inject fault into {pod_name}: {e}")
            raise


def wait_for_fsm_creation(worker_id: str, error_type: str, expected_corr_id: str, timeout_ms: int) -> Optional[str]:
    timeout_s = timeout_ms / 1000.0
    start_time = time.time()
    while True:
        redis_cmd = ["mise", "exec", "--", "kubectl", "--context", "kind-aegis-scenario", "exec", "aegis-redis-master-0", "-n", "aegis-system", "--", "redis-cli", "GET", f"aegis:cp:worker:state:{worker_id}:{error_type}"]
        res = run_cmd(redis_cmd, check=False, capture_output=True)
        if res.returncode == 0 and res.stdout.strip():
            try:
                state_data = json.loads(res.stdout.strip())
                if state_data.get("correlation_id") == expected_corr_id and "incident_id" in state_data:
                    return state_data["incident_id"]
            except json.JSONDecodeError:
                pass
                
        if time.time() - start_time >= timeout_s:
            break
        time.sleep(0.5)
    return None


def poll_for_fsm_state(worker_id: str, error_type: str, expected_state: str, timeout_ms: int) -> bool:
    timeout_s = timeout_ms / 1000.0
    start_time = time.time()
    while True:
        redis_cmd = ["mise", "exec", "--", "kubectl", "--context", "kind-aegis-scenario", "exec", "aegis-redis-master-0", "-n", "aegis-system", "--", "redis-cli", "GET", f"aegis:cp:worker:state:{worker_id}:{error_type}"]
        res = run_cmd(redis_cmd, check=False, capture_output=True)
        if res.returncode == 0 and res.stdout.strip():
            try:
                state_data = json.loads(res.stdout.strip())
                if state_data.get("current_state") == expected_state:
                    return True
            except json.JSONDecodeError:
                pass
                
        if time.time() - start_time >= timeout_s:
            break
        time.sleep(0.5)
    return False


async def kafka_produce_message(topic: str, correlation_id: str, payload: dict) -> None:
    from aiokafka import AIOKafkaProducer
    import json
    
    bootstrap_servers = "localhost:39094"
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await producer.start()
    
    try:
        # Merge correlation ID into payload
        payload["correlation_id"] = correlation_id
        await producer.send_and_wait(
            topic,
            json.dumps(payload).encode("utf-8")
        )
        logger.info(f"Published forged message to {topic} with correlation_id '{correlation_id}'")
    finally:
        await producer.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Aegis Scenario Runner")
    parser.add_argument("--file", type=str, required=True, help="Path to scenario YAML file")
    parser.add_argument("--usecase", type=str, default="scenario", help="Aegis usecase (local, intg-test, scenario)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show all debug, subprocess, and verbose logs in the console")
    args = parser.parse_args()

    if args.verbose:
        console_handler.setLevel(logging.DEBUG)
        for filter_obj in console_handler.filters[:]:
            if isinstance(filter_obj, ConsoleFilter):
                console_handler.removeFilter(filter_obj)
        logger.debug("Verbose logging enabled")

    # Ensure the file path is absolute before we change directory
    scenario_file = os.path.abspath(args.file)

    # Change to project root so all Make, Tilt, and Terraform commands work correctly
    # regardless of where this script was invoked from.
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
    os.chdir(project_root)

    with open(scenario_file, "r") as f:
        scenario = yaml.safe_load(f)

    logger.info(f"Loaded scenario: {scenario.get('name', 'Unknown')}")
    run_scenario(scenario, args)


def run_scenario(scenario_def: dict, args: argparse.Namespace) -> None:
    cluster = scenario_def.get("cluster", {})
    resources = cluster.get("resources", {})
    config = cluster.get("config", {})
    chain = scenario_def.get("chain", [])

    node_count = resources.get("node", {}).get("count", 1)
    
    aegis_cp_count = resources.get("cp", {}).get("count", 1)
    sink_count = resources.get("sink", {}).get("count", 1)
    composer_count = resources.get("composer", {}).get("count", 1)

    # 1. Validation
    for stage in chain:
        if stage.get("action") == "inject_fault":
            target_count = stage.get("target_count", 0)
            if target_count > node_count:
                logger.error(f"Validation failed: target_count ({target_count}) exceeds total cluster node_count ({node_count}).")
                sys.exit(1)

    # 2. Infrastructure Staging
    apply_scenario_infra(node_count, aegis_cp_count, sink_count, composer_count, config)

    # 3. Agent Discovery & State
    logger.info("Waiting for 5 seconds to allow infrastructure to fully stabilize before discovering agents...")
    import time
    time.sleep(5)
    
    agent_pods = get_agent_pods()
    if not agent_pods:
        logger.error("No agent pods found in the cluster. Are they still pending?")
        sys.exit(1)
        
    logger.info(f"Discovered {len(agent_pods)} active agent pods.")
    
    stage_groups: dict[str, list[str]] = {}
    stage_correlation_ids: dict[str, str] = {}
    executed_stages: set[str] = set()

    # 4. Execution Loop
    
    aborting = False
    for idx, stage in enumerate(chain):
        action = stage.get("action")
        
        stage_id = stage.get("stage_id")
        if not stage_id:
            stage_id = f"stage_{idx}"
            stage["stage_id"] = stage_id
            
        always_run = stage.get("always_run", False)
        always_run_depends_on = stage.get("always_run_depends_on")
        continue_on_err = stage.get("continue_on_err", False)
        
        if aborting and not always_run:
            continue
            
        if always_run_depends_on and always_run_depends_on not in executed_stages:
            logger.debug(f"[Stage {idx}] Skipping stage because the stage specified in always_run_depends_on '{always_run_depends_on}' was never executed.")
            continue

        try:
            
            color_name = stage.get("color", "").lower()
            colors = {
                "black": "30", "red": "31", "green": "32", "yellow": "33",
                "blue": "34", "magenta": "35", "cyan": "36", "white": "37"
            }
            
            if action == "print":
                message = stage.get("message", "")
                c_code = colors.get(color_name, "36") # default cyan for print
                logger.info(f"[Stage {idx}] \033[1;{c_code}mPRINT: {message}\033[0m")
                print_tilt_link_if_needed(stage, logger)
                
            elif action == "wait":
                delay = stage.get("delay_seconds", 0)
                reason = stage.get("reason", "")
                c_code = colors.get(color_name, "36") # default cyan
                if reason:
                    logger.info(f"[Stage {idx}] \033[1;{c_code}mWAIT: Waiting for {delay} seconds (Reason: {reason})...\033[0m")
                else:
                    logger.info(f"[Stage {idx}] \033[1;{c_code}mWAIT: Waiting for {delay} seconds...\033[0m")
                print_tilt_link_if_needed(stage, logger)
                time.sleep(delay)
                
            elif action == "pause":
                message = stage.get("message", "Scenario paused. Press Enter to continue...")
                c_code = colors.get(color_name, "33") # default yellow for pause
                logger.info(f"[Stage {idx}] \033[1;{c_code}mPAUSE: {message}\033[0m")
                print_tilt_link_if_needed(stage, logger)
                input()
                
            elif action == "inject_fault":
                mode = stage.get("mode")
                target_ref_id = stage.get("target_ref_id")
                
                corr_prefix = stage.get("correlation_id_prefix")
                corr_id = f"{corr_prefix}-{int(time.time())}" if corr_prefix else None
                
                targets = []
                if target_ref_id:
                    if target_ref_id not in stage_groups:
                        raise ValueError(f"Invalid target_ref_id '{target_ref_id}' - not found in previous stages.")
                    targets = stage_groups[target_ref_id]
                    logger.info(f"[Stage {idx}] Targeting exactly {len(targets)} agents from previous stage '{target_ref_id}' for fault '{mode}'.")
                else:
                    target_count = stage.get("target_count", 1)
                    targets = random.sample(agent_pods, min(target_count, len(agent_pods)))
                    logger.info(f"[Stage {idx}] Targeting {len(targets)} random agents for fault '{mode}'.")
                
                stage_groups[stage_id] = targets
                if corr_id:
                    stage_correlation_ids[stage_id] = corr_id
                
                for pod in targets:
                    inject_fault(pod, mode, correlation_id=corr_id)
                    
                wait_ms = stage.get("wait_for_fsm_creation_timeout_ms")
                if wait_ms and corr_id:
                    logger.info(f"[Stage {idx}] Waiting for FSM to be started/initialized (timeout: {wait_ms}ms)...")
                    for pod in targets:
                        worker_id = f"aegis-system--{pod}"
                        incident_id = wait_for_fsm_creation(worker_id, mode, corr_id, wait_ms)
                        if not incident_id:
                            raise RuntimeError(f"Timeout waiting for FSM creation for worker '{worker_id}' with expected correlation_id '{corr_id}'")
                    logger.info(f"[Stage {idx}] Verified FSM creation for {len(targets)} targets.")
                    
                if corr_id:
                    logger.info(f"Hint: \033[1;36mView trace at http://localhost:10354/r/(all)/overview?q={corr_id}\033[0m")
                    
            elif action == "kafka_produce_message":
                topic = stage.get("topic")
                payload = stage.get("payload", {})
                
                target_ref_id = stage.get("target_ref_id")
                if target_ref_id:
                    if target_ref_id not in stage_correlation_ids:
                        raise ValueError(f"Invalid target_ref_id '{target_ref_id}' for kafka_produce_message (could not find correlation ID)")
                    
                    corr_id = stage_correlation_ids[target_ref_id]
                    if "correlation_id" not in payload:
                        payload["correlation_id"] = corr_id
                        
                    targets = stage_groups.get(target_ref_id, [])
                else:
                    raise ValueError(f"Could not determine target pods. Please specify target_ref_id.")
                
                # Dynamically fetch the incident_id from Redis if the scenario needs it to forge a valid payload
                if stage.get("inject_incident_id", False):
                    # We just take the first pod from the target group
                    pod = targets[0]
                    worker_id = f"aegis-system--{pod}"
                    error_type = stage.get("error_type", "latency_spike")
                    wait_ms = stage.get("wait_for_fsm_creation_timeout_ms", 0)
                    
                    incident_id = wait_for_fsm_creation(worker_id, error_type, corr_id, wait_ms)
                    if incident_id:
                        payload["incident_id"] = incident_id
                        logger.info(f"[Stage {idx}] Dynamically injected incident_id '{payload['incident_id']}' into payload.")
                    else:
                        raise RuntimeError(f"Timeout or missing valid incident_id in Redis state matching correlation_id '{corr_id}'")

                logger.info(f"[Stage {idx}] Producing raw forged message to topic {topic} for correlation_id {corr_id}")
                asyncio.run(kafka_produce_message(topic, corr_id, payload))

            elif action == "wait_for_fsm_state":
                error_type = stage.get("error_type", "latency_spike")
                expected_state = stage.get("expected_state")
                timeout_ms = stage.get("timeout_ms", 30000)
                
                ref_id = stage.get("target_ref_id")
                
                if not ref_id:
                    raise ValueError(f"[Stage {idx}] 'target_ref_id' is required for action 'inject_fsm_event'")
                
                if ref_id not in stage_groups:
                    raise ValueError(f"[Stage {idx}] Invalid target_ref_id '{ref_id}' - not found in previous stages")
                    
                targets = stage_groups[ref_id]
                
                timeout_str = f"{timeout_ms / 60000.0:g} minutes" if timeout_ms >= 60000 else f"{timeout_ms / 1000.0:g} seconds"
                logger.info(f"[Stage {idx}] Waiting up to {timeout_str} for {len(targets)} targets to reach state '{expected_state}'...")
                
                for pod in targets:
                    worker_id = f"aegis-system--{pod}"
                    success = poll_for_fsm_state(worker_id, error_type, expected_state, timeout_ms)
                    if not success:
                        raise RuntimeError(f"Timeout waiting for worker '{worker_id}' to reach state '{expected_state}'")
                
                logger.info(f"[Stage {idx}] Successfully verified all targets reached state '{expected_state}'.")

            elif action == "aegis-cli":
                error_type = stage.get("error_type", "latency_spike")
                cli_args = stage.get("args", [])
                
                target_ref_id = stage.get("target_ref_id")
                if not target_ref_id:
                    raise ValueError(f"[Stage {idx}] 'target_ref_id' is required for action 'aegis-cli' to know which pods to resolve.")
                
                if target_ref_id in stage_groups:
                    targets = stage_groups[target_ref_id]
                    logger.info(f"[Stage {idx}] Targeting exactly {len(targets)} agents from previous stage '{target_ref_id}' for fault '{error_type}'.")
                else:
                    raise ValueError(f"[Stage {idx}] Invalid target_ref_id '{target_ref_id}' - not found in previous stages.")
                    
                logger.info(f"[Stage {idx}] Running SRE CLI 'resolve' against {len(targets)} agents with args {cli_args}...")
                
                run_cmd(["mise", "exec", "--", "make", "cli-build"], check=True, capture_output=False)
                
                for pod in targets:
                    worker_id = f"aegis-system--{pod}"
                    
                    cmd = [
                        "./bin/aegis", "resolve"
                    ] + cli_args + [
                        "--usecase", args.usecase,
                        "--non-interactive",
                        worker_id, error_type
                    ]
                    if getattr(args, "verbose", False):
                        cmd.append("--verbose")
                    run_cmd(cmd, check=True)
                    logger.info(f"Successfully executed CLI resolve for {worker_id} (Error: {error_type})")

            elif action == "kubectl_scale":
                deployment = stage.get("deployment")
                replicas = stage.get("replicas")
                logger.info(f"[Stage {idx}] Scaling deployment {deployment} to {replicas} replicas...")
                run_cmd(["mise", "exec", "--", "kubectl", "--context", "kind-aegis-scenario", "scale", "deployment", deployment, f"--replicas={replicas}", "-n", "aegis-system"], check=True, capture_output=False)

            elif action == "suspend_workload":
                workload = stage.get("workload")
                replicas = stage.get("replicas", 0)
                stage_id = stage.get("stage_id")
                logger.info(f"[Stage {idx}] Suspending workload {workload} to {replicas} active replicas...")
                if workload.lower().startswith("daemonset/"):
                    app_label = "aegis-agent" if "agent" in workload else workload.split("/")[-1]
                    pods_out = run_cmd(["mise", "exec", "--", "kubectl", "--context", "kind-aegis-scenario", "get", "pods", "-l", f"app.kubernetes.io/component={app_label.replace('aegis-', '')}", "-n", "aegis-system", "-o", "json"], check=True, capture_output=True).stdout.strip()
                    if not json.loads(pods_out).get("items"):
                        pods_out = run_cmd(["mise", "exec", "--", "kubectl", "--context", "kind-aegis-scenario", "get", "pods", "-l", f"app.kubernetes.io/name={app_label}", "-n", "aegis-system", "-o", "json"], check=True, capture_output=True).stdout.strip()
                    
                    data = json.loads(pods_out)
                    ds_pods = [item["metadata"]["name"] for item in data.get("items", []) if "deletionTimestamp" not in item["metadata"]]
                    
                    pod_nodes = {}
                    for item in data.get("items", []):
                        if item["metadata"]["name"] in ds_pods:
                            pod_nodes[item["metadata"]["name"]] = item["spec"].get("nodeName")
                        
                    num_suspend = max(0, len(ds_pods) - replicas)
                    target_pods = random.sample(ds_pods, min(num_suspend, len(ds_pods)))
                    nodes_to_suspend = [pod_nodes[p] for p in target_pods if p in pod_nodes]
                    
                    for node in nodes_to_suspend:
                        run_cmd(["mise", "exec", "--", "kubectl", "--context", "kind-aegis-scenario", "taint", "nodes", node, "scenario.aegis.io/suspended=true:NoSchedule", "--overwrite"], check=True, capture_output=False)
                        
                    for pod in target_pods:
                        run_cmd(["mise", "exec", "--", "kubectl", "--context", "kind-aegis-scenario", "delete", "pod", pod, "-n", "aegis-system", "--wait=false"], check=True, capture_output=False)
                        
                    if stage_id and target_pods:
                        stage_groups[stage_id] = target_pods
                else:
                    run_cmd(["mise", "exec", "--", "kubectl", "--context", "kind-aegis-scenario", "scale", workload, f"--replicas={replicas}", "-n", "aegis-system"], check=True, capture_output=False)

            # TODO: it might be better to receive target_ref_id to only resume what was suspended instead of resuming all which could affect devops manually operations
            elif action == "resume_workload":
                workload = stage.get("workload")
                
                # Determine target replicas
                replicas = stage.get("replicas")
                if replicas is None:
                    # Infer from config based on workload type
                    if "aegis-agent" in workload:
                        replicas = config.get("node_count", 1)
                    elif "aegis-control-plane" in workload:
                        replicas = config.get("cp_replicas", 3)
                    elif "aegis-composer" in workload:
                        replicas = config.get("composer_replicas", 1)
                    elif "aegis-sink" in workload:
                        replicas = config.get("sink_replicas", 1)
                    else:
                        replicas = 1
                        
                logger.info(f"[Stage {idx}] Resuming workload {workload} (Target Replicas: {replicas})...")
                if workload.lower().startswith("daemonset/"):
                    nodes = run_cmd(["mise", "exec", "--", "kubectl", "--context", "kind-aegis-scenario", "get", "nodes", "-o", "jsonpath={.items[*].metadata.name}"], check=True, capture_output=True).stdout.strip().split()
                    for node in nodes:
                        run_cmd(["mise", "exec", "--", "kubectl", "--context", "kind-aegis-scenario", "taint", "nodes", node, "scenario.aegis.io/suspended:NoSchedule-"], check=False, silent=True)
                else:
                    run_cmd(["mise", "exec", "--", "kubectl", "--context", "kind-aegis-scenario", "scale", workload, f"--replicas={replicas}", "-n", "aegis-system"], check=True, capture_output=False)

            else:
                logger.warning(f"Unknown action: {action}")
                
            stage_id = stage.get("stage_id")
            if stage_id:
                executed_stages.add(stage_id)
                
        except Exception as e:
            logger.error(f"[Stage {idx}] Unexpected exception during {action}: {e}", exc_info=True)
            if not continue_on_err:
                logger.error(f"[Stage {idx}] Exception occurred and continue_on_err=False. Aborting.")
                aborting = True
                continue
            logger.warning(f"[Stage {idx}] Exception occurred but continue_on_err=True. Continuing.")

    if aborting:
        logger.error("Scenario execution aborted due to errors.")
        sys.exit(1)
        
    logger.info("Scenario execution complete.")


if __name__ == "__main__":
    main()
