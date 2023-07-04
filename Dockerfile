ARG baseImage=devops-harbor.zeekrlife.com/kubeflow/ubuntu:18.04
FROM ${baseImage}
WORKDIR /gpu-admission
COPY gpu-admission   ./gpu-admission
CMD ["/bin/bash", "-c", "./gpu-admission --address=0.0.0.0:3456 --v=5 --logtostderr=true"]
