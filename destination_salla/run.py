#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from .destination import DestinationSalla

def run():
    destination = DestinationSalla()
    destination.run(sys.argv[1:])
