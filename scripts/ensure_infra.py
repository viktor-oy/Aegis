import json
import socket
import urllib.request
import logging
import time

def check_port(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False

def check_http(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=1) as response:
            return response.status == 200
    except Exception:
        return False

def wait_for_infra(usecase: str, timeout_seconds: int = 600, logger: logging.Logger = None) -> bool:
    """
    Waits for Tilt API resources to be 'ok' AND for all local port-forwards to be bound.
    Reads port configurations from scripts/ports.json dynamically.
    """
    import os
    script_dir = os.path.dirname(os.path.abspath(__file__))
    ports_file = os.path.join(script_dir, "ports.json")
    
    with open(ports_file, "r") as f:
        port_map = json.load(f)
        
    if usecase not in port_map:
        raise ValueError(f"Usecase '{usecase}' not found in ports.json")
        
    ports = port_map[usecase]
    tilt_port = ports["tilt"]
    
    tcp_ports = [
        ports["kafka_bootstrap"],
        ports["redis"],
        ports["mailpit_smtp"]
    ] + ports["kafka_brokers"]
    
    http_endpoints = [
        f"http://localhost:{ports['mailpit_http']}/api/v1/messages",
        f"http://localhost:{ports['llm']}/"
    ]
        
    tilt_url = f"http://localhost:{tilt_port}/api/view"
    
    for _ in range(timeout_seconds // 2):
        all_ready = False
        
        # 1. Check Tilt API
        try:
            with urllib.request.urlopen(tilt_url, timeout=2.0) as response:
                if response.status == 200:
                    data = json.loads(response.read().decode('utf-8'))
                    resources = data.get("uiResources", [])
                    
                    if len(resources) > 5:
                        all_ready = True
                        for res in resources:
                            status = res.get("status", {})
                            if res.get("metadata", {}).get("name") == "(Tiltfile)":
                                continue
                                
                            upd = status.get("updateStatus")
                            rt = status.get("runtimeStatus")
                            
                            if upd not in ("ok", "not_applicable") or rt in ("error", "pending"):
                                all_ready = False
                                break
        except Exception:
            pass
            
        # 2. Check local TCP ports and HTTP endpoints (only if Tilt says it's ready)
        if all_ready:
            for port in tcp_ports:
                if not check_port("localhost", port):
                    all_ready = False
                    if logger: logger.debug(f"Waiting for TCP port {port}...")
                    break
                    
            if all_ready:
                for url in http_endpoints:
                    if not check_http(url):
                        all_ready = False
                        if logger: logger.debug(f"Waiting for HTTP endpoint {url}...")
                        break
                        
        if all_ready:
            return True
            
        time.sleep(2)
        
    return False
