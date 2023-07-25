#!/bin/bash
go build
current=`date "+%Y%m%d%H%M%S"`
sudo docker build -t gpu-admission:${current} .
sudo docker tag gpu-admission:${current} harbor-hz.zeekrlife.com/kubeflow/gpu-admission:${current}
sudo docker push  harbor-hz.zeekrlife.com/kubeflow/gpu-admission:${current}
echo harbor-hz.zeekrlife.com/kubeflow/gpu-admission:${current}
