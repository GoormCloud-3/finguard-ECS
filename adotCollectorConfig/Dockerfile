FROM public.ecr.aws/aws-observability/aws-otel-collector:latest
COPY ecs-otel-config.yaml /etc/ecs/ecs-otel-config.yaml
ENTRYPOINT ["/awscollector","--config=/etc/ecs/ecs-otel-config.yaml"]
