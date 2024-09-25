NAMESPACE=redpanda

kubectl create secret generic grafana-cloud-password --from-file=grafana-cloud-password=./grafana-cloud-password.txt -n ${NAMESPACE}
kubectl create secret generic redpanda-password --from-file=redpanda-password=./redpanda-password.txt -n ${NAMESPACE}

kubectl create configmap metrics-forwarder --from-file=./metrics-forwarder.yaml -n ${NAMESPACE}
kubectl create configmap logs-forwarder --from-file=./logs-forwarder.yaml -n ${NAMESPACE}
kubectl create configmap metrics-ingester --from-file=./metrics-ingester.yaml -n ${NAMESPACE}
kubectl create configmap logs-ingester --from-file=./logs-ingester.yaml -n ${NAMESPACE}

kubectl create configmap schemas --from-file=./schemas -n ${NAMESPACE}

kubectl apply -n ${NAMESPACE} -f prereqs.yaml
kubectl apply -n ${NAMESPACE} -f ingesters-sts.yaml
kubectl apply -n ${NAMESPACE} -f forwarders-sts.yaml