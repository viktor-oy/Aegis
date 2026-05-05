#!/usr/bin/env python3
import sys
import subprocess
import json
import os
import re

def log_info(msg):
    print(f"\033[36m{msg}\033[0m", flush=True)

AEGIS_KUBE_CONTEXT = os.environ.get("AEGIS_KUBE_CONTEXT")
if not AEGIS_KUBE_CONTEXT:
    raise ValueError(
        "AEGIS_KUBE_CONTEXT environment variable must be specified. "
        "This script requires explicit context to avoid accidentally wiping the wrong cluster "
        "(e.g., specifying kind-aegis-intg-test vs your local kind-aegis dev cluster)."
    )

def wipe_redis():
    log_info("🧹 Wiping Redis State...")
    try:
        subprocess.run(
            ["kubectl", "--context", AEGIS_KUBE_CONTEXT, "exec", "-n", "aegis-system", "aegis-redis-master-0", "--", "redis-cli", "FLUSHALL"],
            stdout=subprocess.DEVNULL
        )
    except FileNotFoundError:
        print("kubectl not found, skipping redis wipe")

def wipe_kafka(target_topics=None):
    if target_topics:
        log_info(f"🧹 Wiping specific Kafka Topics ({', '.join(target_topics)}) (via kafka-delete-records.sh)...")
    else:
        log_info("🧹 Wiping ALL Kafka State (via kafka-delete-records.sh)...")
    
    # Find topics.yaml relative to this script
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    topics_file = os.path.join(root_dir, "infra", "kafka", "topics.yaml")
    
    partitions_to_delete = []
    
    try:
        with open(topics_file, "r") as f:
            content = f.read()
            # Split YAML docs
            docs = content.split("---")
            for doc in docs:
                if not doc.strip():
                    continue
                
                # Regex parsing avoids needing pyyaml dependency
                name_match = re.search(r'name:\s*(.+)', doc)
                part_match = re.search(r'partitions:\s*(\d+)', doc)
                
                if name_match and part_match:
                    topic = name_match.group(1).strip()
                    partitions = int(part_match.group(1))
                    
                    if target_topics and topic not in target_topics:
                        continue
                    
                    for p in range(partitions):
                        partitions_to_delete.append({
                            "topic": topic,
                            "partition": p,
                            "offset": -1
                        })
    except Exception as e:
        print(f"Failed to parse {topics_file}: {e}", file=sys.stderr)
        sys.exit(1)
        
    if not partitions_to_delete:
        print("No topics found in YAML to wipe.")
        return
        
    delete_json = {
        "partitions": partitions_to_delete,
        "version": 1
    }
    
    # We execute a single bash command inside the pod that creates the JSON and runs the deletion script.
    # This avoids the slow overhead of kubectl cp and minimizes JVM startups.
    json_str = json.dumps(delete_json)
    try:
        subprocess.run(
            ["kubectl", "--context", AEGIS_KUBE_CONTEXT, "exec", "-i", "-n", "aegis-system", "aegis-kafka-controller-0", "-c", "kafka", "--", 
             "/opt/bitnami/kafka/bin/kafka-delete-records.sh", "--bootstrap-server", "localhost:9092", "--offset-json-file", "/dev/stdin"],
            input=json_str.encode('utf-8'),
            stdout=subprocess.DEVNULL,
            check=True
        )
    except FileNotFoundError:
        print("kubectl not found, skipping kafka wipe")
    except subprocess.CalledProcessError as e:
        print(f"Failed to delete Kafka records. The cluster state might not match topics.yaml.", file=sys.stderr)
        sys.exit(1)

def main():
    # Treat TARGETS=... or standard positional args
    targets = []
    for arg in sys.argv[1:]:
        if arg.startswith("TARGETS="):
            targets.extend(arg.split("=")[1].split())
        else:
            targets.append(arg)
            
    if not targets:
        targets = ["redis", "kafka"]
        
    for target in targets:
        if target == "redis":
            wipe_redis()
        elif target == "kafka" or target.startswith("kafka:"):
            if target.startswith("kafka:"):
                topics = target.split(":", 1)[1].split(",")
                wipe_kafka(target_topics=topics)
            else:
                wipe_kafka()
        else:
            print(f"⚠️ Unknown target: {target}. Supported: redis, kafka")
            
    log_info("✅ Infra state wiped.")

if __name__ == "__main__":
    main()
