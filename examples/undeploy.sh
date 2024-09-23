kubectl delete -n green -f metrics-sts.yaml
kubectl delete -n green -f logs-sts.yaml
kubectl delete -n green -f prereqs.yaml

kubectl delete secret grafana-cloud-password -n green
kubectl delete configmap metrics-forwarder -n green
kubectl delete configmap logs-forwarder -n green
kubectl delete configmap schemas -n green