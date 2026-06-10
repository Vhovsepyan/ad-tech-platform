IMAGE_TAG := local

SERVICES := rtb-engine campaign-api audience-edge event-tracker publisher-tag dsp-server reconciliation-worker

.PHONY: build deploy clean reset status

build:
	docker build -f Dockerfile.rtb                  -t adtech-rtb-engine:$(IMAGE_TAG)              .
	docker build -f Dockerfile.campaign-api          -t adtech-campaign-api:$(IMAGE_TAG)            .
	docker build -f Dockerfile.audience-edge         -t adtech-audience-edge:$(IMAGE_TAG)           .
	docker build -f Dockerfile.event-tracker         -t adtech-event-tracker:$(IMAGE_TAG)           .
	docker build -f Dockerfile.publisher-tag         -t adtech-publisher-tag:$(IMAGE_TAG)           .
	docker build -f Dockerfile.dsp-server            -t adtech-dsp-server:$(IMAGE_TAG)              .
	docker build -f Dockerfile.reconciliation-worker -t adtech-reconciliation-worker:$(IMAGE_TAG)   .

deploy:
	kubectl apply -f infra/k8s/infrastructure.yaml
	kubectl apply -f infra/k8s/redpanda.yaml
	kubectl apply -f infra/k8s/rtb-engine.yaml
	kubectl apply -f infra/k8s/campaign-api.yaml
	kubectl apply -f infra/k8s/audience-edge.yaml
	kubectl apply -f infra/k8s/event-tracker.yaml
	kubectl apply -f infra/k8s/publisher-tag.yaml
	kubectl apply -f infra/k8s/dsp-server.yaml
	kubectl apply -f infra/k8s/reconciliation-worker.yaml

clean:
	kubectl delete -f infra/k8s/ --ignore-not-found=true
	docker rmi -f \
		adtech-rtb-engine:$(IMAGE_TAG) \
		adtech-campaign-api:$(IMAGE_TAG) \
		adtech-audience-edge:$(IMAGE_TAG) \
		adtech-event-tracker:$(IMAGE_TAG) \
		adtech-publisher-tag:$(IMAGE_TAG) \
		adtech-dsp-server:$(IMAGE_TAG) \
		adtech-reconciliation-worker:$(IMAGE_TAG) 2>/dev/null || true
	docker volume prune -f

reset: clean build deploy

status:
	kubectl get pods,svc
