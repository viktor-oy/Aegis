include('Tiltfile.infra')

# Push images to the local registry instead of using `kind load`.
# This bypasses the kind load / containerd config version incompatibility.
default_registry('localhost:5001')

docker_build(
    'aegis-control-plane',
    '.',
    dockerfile='services/control-plane/Dockerfile',
    only=['services/control-plane', 'gen', 'go.mod', 'go.sum'],
)

k8s_resource(
    workload='aegis-control-plane',
    port_forwards=['50051:50051'],
    labels=['control-plane']
)

docker_build(
    'aegis-agent',
    '.',
    dockerfile='services/agent/Dockerfile',
    only=['services/agent', 'gen/python', 'pyproject.toml'],
)

k8s_resource(
    workload='aegis-agent',
    port_forwards=['8080:8080'],
    labels=['agent']
)

docker_build(
    'aegis-composer',
    '.',
    dockerfile='services/composer/Dockerfile',
    only=['services/composer', 'pyproject.toml'],
)

k8s_resource(
    workload='aegis-composer',
    labels=['composer']
)

docker_build(
    'aegis-sink',
    '.',
    dockerfile='services/sink/Dockerfile',
    only=['services/sink', 'gen', 'go.mod', 'go.sum'],
)

k8s_resource(
    workload='aegis-sink',
    port_forwards=['8081:8081'],
    labels=['sink']
)
