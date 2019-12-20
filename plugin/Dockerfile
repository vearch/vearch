FROM centos/python-36-centos7
USER root
RUN yum install -y libXrender-0.9.10-1.el7.x86_64
WORKDIR /app
COPY . .
WORKDIR /app/src
RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt \
    && pip install -i https://pypi.tuna.tsinghua.edu.cn/simple torch torchvision \
    && wget -P /opt/app-root/src/.cache/torch/checkpoints/ https://download.pytorch.org/models/vgg16-397923af.pth \
    && wget -P /app/model/ -c https://pjreddie.com/media/files/yolov3.weights
CMD ["bash", "./bin/run.sh", "image"]