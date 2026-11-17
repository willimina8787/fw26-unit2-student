"""A single-instance controller for COSC 3657 ResilientDeployments."""
import logging
import threading

from kubernetes import client, config, watch
from kubernetes.client.exceptions import ApiException
from kubernetes.config.config_exception import ConfigException

GROUP = "cosc3657.tu.ca"
VERSION = "v1alpha1"
PLURAL = "resilientdeployments"
LABEL = "app.kubernetes.io/managed-by"
INTERVAL = 2
LOG = logging.getLogger("reconciler")


def owned_by(pod, uid):
    return any(ref.uid == uid and ref.controller
               for ref in (pod.metadata.owner_references or []))


def reconcile(resource, core):
    """Observe once and apply only the operations needed for this resource."""
    meta, spec = resource["metadata"], resource["spec"]
    if meta.get("deletionTimestamp"):
        return
    name, namespace, uid = meta["name"], meta["namespace"], meta["uid"]
    desired, image = spec["replicas"], spec["image"]
    pods = core.list_namespaced_pod(
        namespace, label_selector=f"{LABEL}={name}",
        _request_timeout=10).items
    # Labels locate candidates; owner UID prevents interference with other pods.
    pods = [pod for pod in pods if owned_by(pod, uid)]
    active = []
    for pod in pods:
        if pod.metadata.deletion_timestamp:
            continue
        if pod.status.phase in ("Failed", "Succeeded"):
            delete_pod(core, namespace, pod.metadata.name)
        elif pod.spec.containers[0].image != image:
            # Replace pods when declarative image intent changes.
            delete_pod(core, namespace, pod.metadata.name)
        else:
            active.append(pod)
    running = sum(pod.status.phase == "Running" for pod in active)
    drift = desired - len(active)
    LOG.info("[RECONCILE LOOP] Resource: %s | Desired: %s | Observed: %s "
             "| Running: %s | Drift: %s", name, desired, len(active), running, drift)
    if drift > 0:
        LOG.info("Scale-Up Event: Provisioning %s pod(s)...", drift)
        for _ in range(drift):
            body = client.V1Pod(
                metadata=client.V1ObjectMeta(
                    generate_name=f"{name}-", labels={LABEL: name},
                    owner_references=[client.V1OwnerReference(
                        api_version=f"{GROUP}/{VERSION}",
                        kind="ResilientDeployment", name=name, uid=uid,
                        controller=True, block_owner_deletion=True)]),
                spec=client.V1PodSpec(containers=[client.V1Container(
                    name="app", image=image,
                    ports=[client.V1ContainerPort(container_port=5678)])]))
            # Abort this pass on an uncertain API result; observe again before retrying.
            created = core.create_namespaced_pod(namespace, body, _request_timeout=10)
            LOG.info("Created pod %s", created.metadata.name)
    elif drift < 0:
        LOG.info("Scale-Down Event: Removing %s pod(s)...", -drift)
        # Prefer removing pending pods before running pods.
        active.sort(key=lambda pod: (pod.status.phase == "Running", pod.metadata.name))
        for pod in active[:-drift]:
            delete_pod(core, namespace, pod.metadata.name)
    elif running == desired:
        LOG.info("EQUILIBRIUM REACHED: System state matches target spec for '%s'.", name)
    else:
        LOG.info("Target pod count allocated; waiting for pods to become Running.")


def delete_pod(core, namespace, name):
    try:
        core.delete_namespaced_pod(name, namespace, _request_timeout=10)
        LOG.info("Deleted pod %s", name)
    except ApiException as exc:
        if exc.status != 404:
            raise


def watch_resources(api, wake, stop):
    """Watch intent changes; reconnect after expiration, disconnect or API failure."""
    while not stop.is_set():
        stream = watch.Watch()
        try:
            for event in stream.stream(
                    api.list_cluster_custom_object, GROUP, VERSION, PLURAL,
                    timeout_seconds=10, _request_timeout=15):
                if stop.is_set():
                    break
                LOG.info("Watch event: %s | Resource: %s", event["type"],
                         event.get("object", {}).get("metadata", {}).get("name", "unknown"))
                wake.set()
        except Exception as exc:
            LOG.warning("Watch interrupted; reconnecting: %s", exc)
            stop.wait(INTERVAL)
        finally:
            stream.stop()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    try:
        config.load_kube_config()
    except ConfigException:
        config.load_incluster_config()
    core, api = client.CoreV1Api(), client.CustomObjectsApi()
    wake, stop = threading.Event(), threading.Event()
    worker = threading.Thread(target=watch_resources, args=(api, wake, stop), daemon=True)
    worker.start()
    LOG.info("=== COSC 3657 Custom Kubernetes Reconciler Started ===")
    try:
        while not stop.is_set():
            wake.clear()
            try:
                resources = api.list_cluster_custom_object(
                    GROUP, VERSION, PLURAL, _request_timeout=10)["items"]
                for resource in resources:
                    try:
                        reconcile(resource, core)
                    except Exception:
                        LOG.exception("Reconciliation failed; will observe again on next pass")
            except Exception:
                LOG.exception("Unable to list resources; retrying")
            # Periodic observations also repair pod deletions without a CR event.
            wake.wait(INTERVAL)
    except KeyboardInterrupt:
        LOG.info("Stopping reconciler.")
    finally:
        stop.set()
        worker.join(timeout=16)


if __name__ == "__main__":
    main()
