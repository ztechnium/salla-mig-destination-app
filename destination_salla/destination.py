import time
import logging
import requests
from typing import Any, Iterable, Mapping
from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import AirbyteConnectionStatus, AirbyteMessage, ConfiguredAirbyteCatalog, Status, Type
import json
from decimal import Decimal
from datetime import date
logger = logging.getLogger("airbyte")

class DestinationSalla(Destination):
    def __init__(self):
        # Salla API configuration
        self.api_url = "https://api.salla.dev/admin/v2/products"
        self.headers = {
            "Authorization": "Bearer 5713bd1f2c76d8d11362831ce0128b866e139c5f7c47175eec92e0d8fb7747dd9fe226203e",
            "Content-Type": "application/json",
        }

    def write(
        self,
        config: Mapping[str, Any],
        configured_catalog: ConfiguredAirbyteCatalog,
        input_messages: Iterable[AirbyteMessage],
    ) -> Iterable[AirbyteMessage]:
        """
        Process input messages and sync product data to the Salla API, tracking failed records.
        """
        logger.info("Starting to process Airbyte input messages for Salla API.")
        last_state = None
        failed_records = []  # List to track failed records

        for message in input_messages:
            if message.type == Type.RECORD:
                if message.record.stream == "products_unified_data":
                    record = message.record.data
                    logger.debug(f"Processing product record: {record}")

                    # Transform the record into the Salla API payload
                    #payload = self.transform_product(record)

                    # Send the payload to the Salla API
                    #if self.send_to_salla_api(payload, failed_records):
                    payload = self.format_payload(record)
                    payload = self.convert_to_correct_format(payload)
                    if self.send_to_salla_api(payload, failed_records):
                        logger.info(f"Product synced successfully: {record.get('name', 'Unknown')}")
                    else:
                        logger.error(f"Failed to sync product: {record.get('name', 'Unknown')}")

            elif message.type == Type.STATE:
                logger.info(f"State message received: {message.state.data}")
                last_state = message.state.data

        # Emit updated state including failed records
        if last_state:
            logger.info(f"Emitting state with failed records: {len(failed_records)} failed.")
            last_state["failed_records"] = failed_records
            yield AirbyteMessage(type=Type.STATE, state=last_state)

        logger.info("Finished processing all input messages for Salla API.")

    def convert_data_to_serializable(self,data):
        """Recursively convert all Decimal and date objects to a serializable type."""
        if isinstance(data, Decimal):
            return float(data)
        elif isinstance(data, date):  # Handle date objects
            return data.isoformat()  # Convert date to string (ISO format)
        elif isinstance(data, dict):
            return {key: self.convert_data_to_serializable(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self.convert_data_to_serializable(item) for item in data]
        return data


    def format_payload(self,data):
        # Convert any Decimal or date objects to serializable format
        data = self.convert_data_to_serializable(data)
        return data
    

    def convert_to_correct_format(self,payload):
        # Convert 'images' from JSON string to a Python list
        if isinstance(payload.get('images'), str):
            payload['images'] = json.loads(payload['images'])

        # Convert 'options' from JSON string to a Python list
        if isinstance(payload.get('options'), str):
            payload['options'] = json.loads(payload['options'])

        # Convert 'channels' from JSON string to a Python list
        if isinstance(payload.get('channels'), str):
            payload['channels'] = json.loads(payload['channels'])

        # Convert 'categories' from JSON string to a Python list
        if isinstance(payload.get('categories'), str):
            payload['categories'] = json.loads(payload['categories'])

        # Ensure all number fields (like price, cost_price, sale_price) are floats
        for field in ['price', 'cost_price', 'sale_price']:
            if isinstance(payload.get(field), (int, float)):
                payload[field] = float(payload[field])

        # Ensure booleans (True/False) remain correct, no changes needed but added for clarity
        # for field in ['pinned', 'is_available', 'enable_note', 'hide_quantity', 'active_advance', 'require_shipping',
        #             'enable_upload_image']:
        #     if isinstance(payload.get(field), str):
        #         payload[field] = payload[field].lower() == 'true'

        return payload
    def transform_product(self, record: Mapping[str, Any]) -> dict:
        """
        Transform the input product record into the required Salla API payload format.
        """
        logger.debug(f"Transforming record for Salla API: {record}")

        def safe_float(value, default=0.0):
            try:
                return float(value) if value is not None else default
            except (ValueError, TypeError):
                return default

        if not record.get("name") or record["name"].strip().lower() in ["test product", ""]:
            raise ValueError(f"Invalid product name: {record.get('name')}")

        return {
            "name": record["name"],
            "subtitle": record.get("subtitle", ""),
            "description": record.get("description", ""),
            "price": safe_float(record.get("price")),
            "cost_price": safe_float(record.get("cost_price")),
            "sale_price": safe_float(record.get("sale_price")),
            "status": record.get("status", "inactive"),
            "weight": safe_float(record.get("weight")),
            "weight_type": record.get("weight_type", "kg"),
            "pinned": record.get("pinned", False),
            "quantity": record.get("quantity", 0),
            "sale_end": record.get("sale_end", None),
            "source_id": record.get("source_id", ""),
            "brand_id": record.get("brand_id", None),
            "enable_note": record.get("enable_note", False),
            "is_available": record.get("is_available", True),
            "product_type": record.get("product_type", "physical"),
            "promotion_title": record.get("promotion_title", ""),
            "hide_quantity": record.get("hide_quantity", False),
            "active_advance": record.get("active_advance", False),
            "require_shipping": record.get("require_shipping", True),
            "enable_upload_image": record.get("enable_upload_image", False),
            "maximum_quantity_per_order": record.get("maximum_quantity_per_order", 1),
            "images": record.get("images", []),
            "options": record.get("options", []),
            "categories": record.get("categories", []),
            "channels": record.get("channels", []),
        }

    def send_to_salla_api(self, payload: dict, failed_records: list, max_retries: int = 3, retry_delay: int = 2) -> bool:
        """
        Send a product payload to the Salla API with retry logic for transient failures.
        Tracks records that fail with a 500 error.

        :param payload: The product payload to be sent.
        :param failed_records: A list to track failed records.
        :param max_retries: The maximum number of retries for failed requests.
        :param retry_delay: The delay (in seconds) between retries.
        :return: True if the product was synced successfully, False otherwise.
        """
        try:
            attempts = 0
            while attempts < max_retries:
                try:
                    logger.debug(f"Sending payload to Salla API (attempt {attempts + 1}): {payload}")
                    response = requests.post(self.api_url, headers=self.headers, json=payload)

                    if response.status_code in (200, 201):
                        logger.info(f"Product synced successfully. Response code: {response.status_code}")
                        return True
                    elif response.status_code >= 500:
                        # Retry on server-side errors
                        logger.warning(f"Salla API returned server error: {response.status_code}. Retrying...")
                    else:
                        # Log client-side or validation errors
                        logger.error(
                            f"Failed to sync product. Response code: {response.status_code}, Error: {response.text}"
                        )
                        failed_records.append({"payload": payload, "error": response.text})
                        return False
                except requests.exceptions.RequestException as e:
                    logger.error(f"Connection error during API request: {e}. Retrying...")
                finally:
                    attempts += 1
                    if attempts < max_retries:
                        time.sleep(retry_delay)

            # If all retries fail, log and track the failed record
            logger.error(f"Exceeded maximum retries ({max_retries}) for payload: {payload}")
            failed_records.append({"payload": payload, "error": "Exceeded retries"})
            return False

        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            failed_records.append({"payload": payload, "error": repr(e)})
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