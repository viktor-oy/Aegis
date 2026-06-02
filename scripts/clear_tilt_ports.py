#!/usr/bin/env python3
import os
import subprocess
import sys

import json

# Define port mappings based on AEGIS_KUBE_CONTEXT dynamically
script_dir = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(script_dir, "ports.json"), "r") as f:
    port_map_data = json.load(f)

# Convert the format from ports.json to match what clear_tilt_ports.py expects
CONTEXT_PORTS = {
    "kind-aegis": {
        "tilt": port_map_data["local"]["tilt"],
    },
    "kind-aegis-intg-test": port_map_data["intg-test"],
    "kind-aegis-scenario": port_map_data["scenario"]
}
# Flatten the kafka brokers for clear_tilt_ports 
for ctx in ["kind-aegis-intg-test", "kind-aegis-scenario"]:
    brokers = CONTEXT_PORTS[ctx].pop("kafka_brokers", [])
    for i, p in enumerate(brokers):
        CONTEXT_PORTS[ctx][f"kafka_pf_{i}"] = p

def kill_port(port):
    try:
        output = subprocess.check_output(["lsof", "-i", f":{port}"], text=True).strip().splitlines()
        if len(output) <= 1:
            return
            
        for line in output[1:]:
            parts = line.split()
            if not parts:
                continue
                
            cmd = parts[0].lower()
            pid = parts[1]
            
            # NEVER kill docker/colima host proxies
            if any(x in cmd for x in ["docker", "colima", "lima", "vpnkit"]):
                continue
                
            print(f"\033[33mKilling process {cmd} (PID: {pid}) occupying port {port}...\033[0m", flush=True)
            subprocess.run(["kill", "-9", pid], check=False)
    except subprocess.CalledProcessError:
        pass

def main():
    context = os.getenv("AEGIS_KUBE_CONTEXT")
    if not context:
        print("\033[31mError: AEGIS_KUBE_CONTEXT environment variable must be specified.\033[0m")
        sys.exit(1)
        
    ports = CONTEXT_PORTS.get(context)
    if not ports:
        print(f"\033[31mError: Unknown AEGIS_KUBE_CONTEXT '{context}'. No port mapping found.\033[0m")
        sys.exit(1)
        
    print(f"\033[36mCleaning up any existing processes on test ports for context {context}...\033[0m", flush=True)
    for port in ports.values():
        kill_port(port)

    # Also wipe out any zombie resilient-port-forward bash scripts that might be 
    # infinitely looping and re-spawning kubectl port-forwards we just killed
    print("\033[36mEnsuring no zombie port-forward scripts are left behind...\033[0m", flush=True)
    subprocess.run(["pkill", "-f", "resilient-port-forward.sh"], check=False)

if __name__ == "__main__":
    main()
