#!/bin/bash
rsync --include='imgs' --include='main.db' --include='*.log' --include='log' --include='main.db' --exclude='*' -avz ubuntu@clab:~/csbot/ .