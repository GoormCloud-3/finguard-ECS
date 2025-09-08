FROM public.ecr.aws/aws-observability/aws-otel-collector:latest
RUN (microdnf install -y curl || yum install -y curl) && (microdnf clean all || yum clean all || true)

COPY ecs-otel-config.yaml /etc/ecs/otel-config.yaml
CMD ["--config=/etc/ecs/otel-config.yaml"]


