#!/bin/sh
yes | pacman -S python-pip
pip install pika
python -u /tmp/graph-tool/GraphToolManager.py