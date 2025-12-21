allow_k8s_contexts('kind-aegis')

default_registry('localhost:5001')

k8s_yaml(kustomize('infra/kustomize/overlays/local'))

docker_build(
    'aegis/control-plane',
    '.',
    dockerfile='services/control-plane/Dockerfile',
    live_update=[
        sync('services/control-plane', '/workspace/services/control-plane'),
        sync('go.mod', '/workspace/go.mod'),
        run('go build -o /tmp/aegis-control-plane ./services/control-plane/cmd/aegis-control-plane', trigger=['services/control-plane/**/*.go', 'go.mod']),
        restart_container(),
    ],
    ignore=['services/agent', 'services/composer', 'reports'],
)

docker_build(
    'aegis/agent',
    '.',
    dockerfile='services/agent/Dockerfile',
    live_update=[
        sync('services/agent', '/workspace/services/agent'),
        run('pkill -HUP -f aegis_agent || true', trigger=['services/agent/**/*.py']),
    ],
    ignore=['services/control-plane', 'services/composer', 'reports'],
)

docker_build(
    'aegis/composer',
    '.',
    dockerfile='services/composer/Dockerfile',
    live_update=[
        sync('services/composer', '/workspace/services/composer'),
        run('pkill -HUP -f aegis_composer || true', trigger=['services/composer/**/*.py']),
    ],
    ignore=['services/control-plane', 'services/agent', 'reports'],
)

docker_build(
    'aegis/sink-worker',
    '.',
    dockerfile='services/sink-workers/Dockerfile',
    live_update=[
        sync('services/sink-workers', '/workspace/services/sink-workers'),
        run('go build -o /tmp/aegis-sink-worker ./services/sink-workers/cmd/aegis-sink-worker', trigger=['services/sink-workers/**/*.go', 'go.mod']),
        restart_container(),
    ],
    ignore=['services/agent', 'services/composer', 'reports'],
)

k8s_resource('aegis-control-plane', port_forwards=['50051:50051', '8080:8080'])
k8s_resource('aegis-agent')
k8s_resource('aegis-composer')
k8s_resource('aegis-sink-worker')
k8s_resource('kafka', labels=['infra'])
k8s_resource('redis', labels=['infra'])
k8s_resource('postgresql', labels=['infra'])
k8s_resource('minio', port_forwards=['9000:9000'], labels=['infra'])
k8s_resource('otel-collector', labels=['observability'])

