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
