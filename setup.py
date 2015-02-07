#!/usr/bin/python

import os
directories = ["./data", "./plugins/store"]
for directory in directories:
  if not os.path.exists(directory)
    os.makedirs(directory)
