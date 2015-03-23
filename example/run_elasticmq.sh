#!/bin/bash

sudo docker pull pakohan/elasticmq
sudo docker run -p 9324:9324 pakohan/elasticmq
