# Startup Playbook

Daily operations guide for the crypto streaming pipeline on Kubernetes. Use this any time you sit down at the machine and want everything back up.

---

## TL;DR — The 3 Commands

90% of the time, this is all you need:

```powershell
# 1. Start the cluster
minikube start

# 2. Wait ~2–4 minutes, then verify
kubectl get pods -n crypto-pipeline

# 3. Port-forward the dashboard (leave running)
kubectl port-forward -n crypto-pipeline service/dashboard 8501:80
```

Then open <http://localhost:8501>.

---

## Full Startup Sequence

### Step 0 — Prerequisites

- Docker Desktop is running (whale icon steady in tray)
- No `docker compose` containers are running (`docker ps` shouldn't show any `crypto-k8s-pipeline-*` containers)

### Step 1 — Start Minikube

**Where:** Anywhere. **Window:** Any regular PowerShell.

```powershell
minikube start
```

No flags needed — Minikube remembers settings from the first run. Takes 30–90 seconds the second time.

Verify:

```powershell
minikube status
```

All three lines (`host`, `kubelet`, `apiserver`) should say `Running`.

### Step 2 — Check the pods auto-recovered

```powershell
kubectl get pods -n crypto-pipeline
```

Expected: **7 pods, all `1/1 Running`**.

```
NAME                         READY   STATUS    RESTARTS   AGE
consumer-xxx                 1/1     Running   ...
consumer-yyy                 1/1     Running   ...
dashboard-xxx                1/1     Running   ...
kafka-0                      1/1     Running   ...
postgres-0                   1/1     Running   ...
producer-xxx                 1/1     Running   ...
zookeeper-0                  1/1     Running   ...
```

⏱️ **Takes 2–4 minutes after `minikube start`** for everything to come up. Order: postgres + zookeeper → kafka → producer/consumer/dashboard.

If anything is `CrashLoopBackOff` or stuck at `0/1` after 5 minutes, jump to [Recovery](#recovery).

### Step 3 — Open the dashboard (port-forward)

**Window 1 (dedicated — keep open while using dashboard):**

```powershell
kubectl port-forward -n crypto-pipeline service/dashboard 8501:80
```

Leave it running. Browser → <http://localhost:8501>.

Stop: `Ctrl+C` in this window.

### Step 4 — Project directory (for kubectl commands)

**Window 2 — your "work" window:**

```powershell
cd "C:\Users\DEVICES\Desktop\Data E\kubernetes_proj\crypto-k8s\crypto-k8s-pipeline"
```

Use this window for demos, logs, scaling, and any other `kubectl` activity.

---

## Common Operations

### View logs

```powershell
# Live tail of one component
kubectl logs -n crypto-pipeline -l app=producer -f --tail=10
kubectl logs -n crypto-pipeline -l app=consumer -f --tail=10 --prefix --max-log-requests=5

# Last 50 lines of a specific pod
kubectl logs -n crypto-pipeline kafka-0 --tail=50

# Why did <pod> crash? (logs from the previous container before restart)
kubectl logs -n crypto-pipeline <pod-name> --previous --tail=50
```

### Open a shell in a pod

```powershell
# Postgres
kubectl exec -it -n crypto-pipeline postgres-0 -- psql -U crypto -d crypto

# Any pod
kubectl exec -it -n crypto-pipeline <pod-name> -- sh
```

### Scale the consumer

```powershell
kubectl scale deployment/consumer -n crypto-pipeline --replicas=5
kubectl scale deployment/consumer -n crypto-pipeline --replicas=2
```

### Apply manifest changes

```powershell
kubectl apply -f k8s/
```

Output to look for:
- `configured` → spec changed, K8s applied the update
- `unchanged` → no diff between local file and cluster

---

## Rebuilding an Image (only when Python code changes)

Skip if you haven't modified `producer/`, `consumer/`, or `dashboard/` code.

```powershell
# 1. Point THIS shell's Docker at Minikube's daemon
& minikube docker-env --shell powershell | Invoke-Expression

# 2. Verify (should show Kubernetes system containers, not host's)
docker ps

# 3. Rebuild the changed image
docker build -t crypto-dashboard:latest .\dashboard
# (or crypto-consumer / crypto-producer)

# 4. Force the deployment to roll the new image in
kubectl rollout restart deployment/dashboard -n crypto-pipeline

# 5. Verify the new pods come up healthy
kubectl get pods -n crypto-pipeline -l app=dashboard -w
```

Each new PowerShell window starts pointed at host Docker again. Run step 1 in every fresh window you want to build in.

---

## Recovery

Use these in order, from least to most disruptive.

### Soft restart — bounce a Deployment

For producer / consumer / dashboard (any stateless workload):

```powershell
kubectl rollout restart deployment/<name> -n crypto-pipeline
```

### Force pod recreation — for stuck StatefulSet pods

If `rollout restart` is hanging on a single-replica StatefulSet whose pod is unhealthy:

```powershell
kubectl delete pod <pod-name> -n crypto-pipeline --force --grace-period=0
```

The StatefulSet immediately recreates the pod using the latest manifest. The PVC re-attaches; data is preserved.

### Re-apply all manifests

Safe (idempotent — `kubectl apply` is the recommended way):

```powershell
kubectl apply -f k8s/
```

### Verify the live probe spec on a pod

When in doubt about whether a manifest change actually took:

```powershell
kubectl get pod kafka-0 -n crypto-pipeline -o jsonpath="{.spec.containers[0].readinessProbe}"
```

Returns the probe spec that's *currently in the cluster*, not what's in your local YAML.

### Restart-storm escape hatch

If postgres/kafka/zookeeper are all unhappy, recover them in dependency order:

```powershell
kubectl delete pod zookeeper-0 -n crypto-pipeline --force --grace-period=0
kubectl wait --for=condition=ready pod/zookeeper-0 -n crypto-pipeline --timeout=120s

kubectl delete pod kafka-0 -n crypto-pipeline --force --grace-period=0
kubectl wait --for=condition=ready pod/kafka-0 -n crypto-pipeline --timeout=180s

kubectl rollout restart deployment/producer deployment/consumer -n crypto-pipeline
```

---

## Shutdown

When you're done for the day:

```powershell
# Window 1: Ctrl+C to stop the port-forward
# Window 2:
minikube stop
```

`minikube stop` pauses everything cleanly while preserving the cluster, PVCs, images, and pod specs. Next startup just runs `minikube start` and everything wakes up where it left off.

⚠️ **Don't run `minikube delete`** — that destroys the cluster permanently. You'd have to rebuild images and re-apply manifests.
