#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#


import sys

from destination_salla import DestinationSalla

if __name__ == "__main__":
    DestinationSalla().run(sys.argv[1:])
