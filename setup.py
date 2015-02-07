#!/usr/bin/python

import os
directories = ["./data", "./plugins/tmp"]
for directory in directories:
  if not os.path.exists(directory)
    os.makedirs(directory)
