include('Tiltfile.infra')
load('ext://restart_process', 'docker_build_with_restart')

aegis_env_app = os.getenv('AEGIS_ENV', 'local')

AEGIS_PORT_MAP = {
    'local': {
        'cp': ['50051:50051'],
        'agent': ['8080:8080'],
        'sink': ['8081:8081']
    },
    'intg-test': {
        'cp': [],
        'agent': [],
        'sink': []
    },
    'scenario': {
        'cp': [],
        'agent': [],
        'sink': []
    }
}
aegis_ports = AEGIS_PORT_MAP.get(aegis_env_app, AEGIS_PORT_MAP['local'])

# Push images to the local registry instead of using `kind load`.
# This bypasses the kind load / containerd config version incompatibility.
default_registry('localhost:5001')

docker_build(
    'aegis-control-plane',
    '.',
    dockerfile='services/control-plane/Dockerfile',
    only=['services/control-plane', 'services/pkg', 'gen', 'go.mod', 'go.sum'],
)

k8s_resource(
    workload='aegis-control-plane',
    port_forwards=aegis_ports['cp'],
    labels=['control-plane']
)

docker_build_with_restart(
    'aegis-agent',
    '.',
    dockerfile='services/agent/Dockerfile',
    only=['services/agent', 'services/pkg', 'gen/python', 'pyproject.toml'],
    entrypoint=['python', '-m', 'services.agent.main'],
    live_update=[
        sync('pyproject.toml', '/workspace/pyproject.toml'),
        sync('services/agent', '/workspace/services/agent'),
        sync('services/pkg', '/workspace/services/pkg'),
        sync('gen/python', '/workspace/gen/python'),
        run('python -m pip install -e .', trigger=['pyproject.toml']),
    ],
)

k8s_resource(
    workload='aegis-agent',
    port_forwards=aegis_ports['agent'],
    labels=['agent']
)

docker_build_with_restart(
    'aegis-composer',
    '.',
    dockerfile='services/composer/Dockerfile',
    only=['services/composer', 'services/pkg', 'pyproject.toml'],
    entrypoint=['python', '-m', 'services.composer'],
    live_update=[
        sync('pyproject.toml', '/workspace/pyproject.toml'),
        sync('services/composer', '/workspace/services/composer'),
        sync('services/pkg', '/workspace/services/pkg'),
        run('python -m pip install -e .', trigger=['pyproject.toml']),
    ],
)

k8s_resource(
    workload='aegis-composer',
    labels=['composer']
)

docker_build(
    'aegis-sink',
    '.',
    dockerfile='services/sink/Dockerfile',
    only=['services/sink', 'services/pkg', 'gen', 'go.mod', 'go.sum'],
)

k8s_resource(
    workload='aegis-sink',
    port_forwards=aegis_ports['sink'],
    labels=['sink']
)
