kubectl create secret generic grafana-cloud-password --from-file=grafana-cloud-password=./grafana-cloud-password.txt -n green
kubectl create configmap metrics-forwarder --from-file=./metrics-forwarder.yaml -n green
kubectl create configmap logs-forwarder --from-file=./logs-forwarder.yaml -n green
kubectl create configmap schemas --from-file=./schemas -n green

kubectl apply -n green -f prereqs.yaml
kubectl apply -n green -f metrics-sts.yaml
kubectl apply -n green -f logs-sts.yaml