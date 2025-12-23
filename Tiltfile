allow_k8s_contexts('kind-aegis')

default_registry('localhost:5001')

k8s_yaml(kustomize('infra/kustomize/overlays/local'))

k8s_resource('kafka', labels=['infra'])
k8s_resource('redis', labels=['infra'])
k8s_resource('postgresql', labels=['infra'])
k8s_resource('minio', port_forwards=['9000:9000'], labels=['infra'])
k8s_resource('otel-collector', labels=['observability'])
