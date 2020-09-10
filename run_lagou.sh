#!/bin/bash
source /home/conda3/bin/activate py368
python --version
cd /home/FuJianTech/LagouJobInfo/lagou_spider/
python handle_crawl_lagou.py
