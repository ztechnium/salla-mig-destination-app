import logging
import requests
from typing import Any, Iterable, Mapping
from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import AirbyteConnectionStatus, AirbyteMessage, ConfiguredAirbyteCatalog, Status, Type

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
        Main write method to process input messages and sync product data to the Salla API.
        """
        logger.info("Starting to process Airbyte input messages for Salla API.")

        for message in input_messages:
            if message.type == Type.RECORD:
                # Ensure we're processing the correct stream (products)
                if message.record.stream == "products":
                    record = message.record.data
                    logger.debug(f"Processing product record: {record}")

                    # Transform the record into the Salla API payload
                    payload = self.transform_product(record)

                    # Send the payload to the Salla API
                    if self.send_to_salla_api(payload):
                        logger.info(f"Product synced successfully: {record.get('name', 'Unknown')}")
                    else:
                        logger.error(f"Failed to sync product: {record.get('name', 'Unknown')}")

            elif message.type == Type.STATE:
                # Emit state messages back to Airbyte
                logger.info("State message received and emitted back.")
                yield message
            else:
                logger.warning(f"Unsupported message type: {message.type}")

        logger.info("Finished processing all input messages for Salla API.")

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

    def send_to_salla_api(self, payload: dict) -> bool:
        """
        Send a product payload to the Salla API.
        """
        try:
            logger.debug(f"Sending payload to Salla API: {payload}")
            response = requests.post(self.api_url, headers=self.headers, json=payload)

            if response.status_code in (200, 201):
                logger.info(f"Product synced successfully. Response code: {response.status_code}")
                return True
            else:
                logger.error(
                    f"Failed to sync product. Response code: {response.status_code}, Error: {response.text}"
                )
                return False
        except Exception as e:
            logger.error(f"Error sending product to Salla API: {e}")
            return False
        

    def check(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        """
        Verifies that the input configuration is valid and the Salla API is reachable.
        """
        logger.info("Checking connection to the Salla API...")

        api_url = "https://api.salla.dev/admin/v2/products"  # Test endpoint
        headers = {
            "Authorization": f"Bearer {config['api_key']}",
            "Content-Type": "application/json",
        }

        # Detailed test payload
        test_payload = {
            "name": "Test Product",
            "source_id": "1234567890",
            "price": 100.0,
            "status": "out",
            "product_type": "product",
            "quantity": 10,
            "booking_details": {
                "location": "Test Location",
                "type": "date",
                "time_strict_value": 3,
                "time_strict_type": "days",
                "sessions_count": 5,
                "session_gap": 2,
                "session_duration": 60,
                "availabilities": [
                    {
                        "day": "sunday",
                        "is_available": True,
                        "times": [{"from": "10:00", "to": "12:00"}],
                    }
                ],
                "overrides": [{"day": "sunday", "date": "2030-01-01"}],
            },
            "description": "This is a test product for connection validation.",
            "categories": [2037622520],
            "sale_price": 90.0,
            "cost_price": 80.0,
            "require_shipping": False,
            "maximum_quantity_per_order": 1,
            "weight": 5.0,
            "weight_type": "kg",
            "hide_quantity": False,
            "images": [
                {
                    "original": "https://example.com/image.jpg",
                    "thumbnail": "https://example.com/thumbnail.jpg",
                    "alt": "Test Image",
                    "default": True,
                }
            ],
            "options": [
                {
                    "name": "Color",
                    "display_type": "text",
                    "values": [{"name": "Red"}, {"name": "Blue"}],
                }
            ],
            "translations": {
                "en": {
                    "name": "Test Product - EN",
                    "description": "Test description - EN",
                    "subtitle": "Test subtitle - EN",
                }
            },
        }

        try:
            # Make a test request to validate the connection
            response = requests.post(api_url, headers=headers, json=test_payload)

            if response.status_code in (200, 201):
                logger.info("Connection to Salla API successful.")
                return AirbyteConnectionStatus(status=Status.SUCCEEDED)
            else:
                logger.error(
                    f"Failed to connect to Salla API. Status code: {response.status_code}, Error: {response.text}"
                )
                return AirbyteConnectionStatus(
                    status=Status.FAILED,
                    message=f"Failed to connect to Salla API. Error: {response.text}",
                )
        except Exception as e:
            logger.error(f"Exception while connecting to Salla API: {e}")
            return AirbyteConnectionStatus(
                status=Status.FAILED, message=f"Exception while connecting to Salla API: {repr(e)}"
            )
