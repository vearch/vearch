#!/usr/bin/env bash

# install conda
cd /env/app/
if [ ! -f "Miniconda3-py310_24.3.0-0-Linux-x86_64.sh" ]; then
    wget --quiet https://mirrors.tuna.tsinghua.edu.cn/anaconda/miniconda/Miniconda3-py310_24.3.0-0-Linux-x86_64.sh
fi

sh Miniconda3-py310_24.3.0-0-Linux-x86_64.sh -b
/root/miniconda3/bin/conda init bash
source ~/.bashrc

rm -rf Miniconda3-py310_24.3.0-0-Linux-x86_64.sh

pip install requests pytest numpy -i https://pypi.tuna.tsinghua.edu.cn/simple --quiet

# install ifconfig
yum install -y net-tools