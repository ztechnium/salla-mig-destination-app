#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import logging
import requests
from typing import Any, Iterable, Mapping

from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import AirbyteConnectionStatus, AirbyteMessage, ConfiguredAirbyteCatalog, Status , Type



class DestinationSalla(Destination):
    def write(
        self, config: Mapping[str, Any], configured_catalog: ConfiguredAirbyteCatalog, input_messages: Iterable[AirbyteMessage]
    ) -> Iterable[AirbyteMessage]:

        """
        This method writes data to the Salla API.
        """
        api_url = config['api_url']
        api_key = config['api_key']
        headers = {'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'}
        
        for message in input_messages:
            if message.type == Type.RECORD:
                # Ensure the payload structure matches your requirement
                data = message.record.data
                
                # Send the data to Salla API
                if self.send_to_salla_api(data, api_url, headers):
                    yield message  # Return success message

            if message.type == Type.STATE:
                yield message


  
    def send_to_salla_api(self, payload: dict, api_url: str, headers: dict) -> bool:
        try:
            response = requests.post(api_url, json=payload, headers=headers)
            if response.status_code in (200, 201):
                logging.info(f"Product synced successfully. Response: {response.status_code}")
                return True
            else:
                logging.error(f"Failed to sync product. Status: {response.status_code}, Error: {response.text}")
                return False
        except Exception as e:
            logging.error(f"Error sending to Salla API: {e}")
            return False










    def check(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        """
        Tests if the input configuration can be used to successfully connect to the destination with the needed permissions
            e.g: if a provided API token or password can be used to connect and write to the destination.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this destination, content of this json is as specified in
        the properties of the spec.json file

        :return: AirbyteConnectionStatus indicating a Success or Failure
        """
        try:
            # TODO

            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {repr(e)}")
