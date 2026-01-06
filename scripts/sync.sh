#!/bin/bash
rsync --include='imgs/***' --include='*.log' --include='log/***'  --exclude='*' -avz ubuntu@clab:~/csbot/ .