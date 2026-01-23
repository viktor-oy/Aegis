include('Tiltfile.infra')

# Push images to the local registry instead of using `kind load`.
# This bypasses the kind load / containerd config version incompatibility.
default_registry('localhost:5001')

docker_build(
    'aegis-control-plane',
    '.',
    dockerfile='services/control-plane/Dockerfile',
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
)

k8s_resource(
    workload='aegis-composer',
    labels=['composer']
)
