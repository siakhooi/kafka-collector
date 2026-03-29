# kafka-collector Kubernetes Deployment

This directory contains Kubernetes manifests for deploying `kafka-collector` in service mode.

## Resources

| File | Description |
|------|-------------|
| `configmap.yaml` | Configuration via environment variables |
| `deployment.yaml` | Deployment with 1 replica |
| `service.yaml` | ClusterIP service exposing port 8080 |
| `kustomization.yaml` | Kustomize configuration |

## Quick Start

### Using kubectl

```bash
kubectl apply -f configmap.yaml
kubectl apply -f deployment.yaml
kubectl apply -f service.yaml
```

### Using Kustomize

```bash
kubectl apply -k .
```

## Configuration

Edit `configmap.yaml` to configure kafka-collector:

| Variable | Description | Default |
|----------|-------------|---------|
| `KAFKA_TOPICS` | Comma-separated list of topics to consume | (required) |
| `KAFKA_BOOTSTRAP_SERVER` | Kafka bootstrap server address | `localhost:9092` |
| `KAFKA_GROUP` | Consumer group ID | random UUID |
| `COLLECTOR_MODE` | Run mode: `cli` or `service` | `cli` |
| `COLLECTOR_SERVICE_PORT` | HTTP port for service mode | `8080` |
| `COLLECTOR_CAPTURE_DIR` | Capture directory for service mode | `/tmp/kafka-collector` |

## Accessing the Service

### Port Forward

```bash
kubectl port-forward svc/kafka-collector 8080:8080
```

### Ingress (optional)

Create an Ingress resource to expose the service externally:

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: kafka-collector
spec:
  rules:
    - host: kafka-collector.example.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: kafka-collector
                port:
                  number: 8080
```

## Cleanup

```bash
kubectl delete -k .
```

Or:

```bash
kubectl delete -f service.yaml
kubectl delete -f deployment.yaml
kubectl delete -f configmap.yaml
```
