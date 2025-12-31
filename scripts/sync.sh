#!/bin/bash
rsync --include='imgs' --include='main.db' --include='*.log' --include='*.txt' --include='main.db' --exclude='*' -avz ubuntu@clab:~/csbot/ .