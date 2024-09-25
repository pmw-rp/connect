NAMESPACE=redpanda

kubectl delete -n ${NAMESPACE} -f forwarders-sts.yaml
kubectl delete -n ${NAMESPACE} -f ingesters-sts.yaml
#kubectl delete -n ${NAMESPACE} -f metrics-sts.yaml
#kubectl delete -n ${NAMESPACE} -f logs-sts.yaml
kubectl delete -n ${NAMESPACE} -f prereqs.yaml

kubectl delete secret redpanda-password -n ${NAMESPACE}
kubectl delete secret grafana-cloud-password -n ${NAMESPACE}
kubectl delete configmap metrics-forwarder -n ${NAMESPACE}
kubectl delete configmap logs-forwarder -n ${NAMESPACE}
kubectl delete configmap metrics-ingester -n ${NAMESPACE}
kubectl delete configmap logs-ingester -n ${NAMESPACE}
kubectl delete configmap schemas -n ${NAMESPACE}
