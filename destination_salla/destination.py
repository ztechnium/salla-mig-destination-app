#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#

import logging
import requests
from typing import Any, Iterable, Mapping

from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import AirbyteConnectionStatus, AirbyteMessage, ConfiguredAirbyteCatalog, Status , Type
import psycopg2
from psycopg2.extras import DictCursor
from datetime import datetime


logger = logging.getLogger("airbyte")

DB2_CONFIG = {
    "host": "104.248.18.120",
    "port": 5432,
    "dbname": "test1",
    "user": "khaled",
    "password": "kh@2030",
    "sslmode": "allow"
}



SALLA_API_URL = "https://api.salla.dev/admin/v2/products"
SALLA_API_HEADERS = {
    "Authorization": "Bearer 5713bd1f2c76d8d11362831ce0128b866e139c5f7c47175eec92e0d8fb7747dd9fe226203e",
    "Content-Type": "application/json"
}


class DestinationSalla(Destination):
    
    def write(
        self, config: Mapping[str, Any], configured_catalog: Any, input_messages: Iterable[AirbyteMessage]
    ) -> Iterable[AirbyteMessage]:
        # logger.info("Begin writing to the destination...")
        # yield AirbyteMessage(
        #     type=Type.LOG,
        #     log={"level":"INFO","message":"Begin writing to the destination..."}
        # )

        try:
            self.sync_products()
            yield AirbyteMessage(
                type=Type.LOG,
                log={"level":"INFO","message":"Products synced successfully."}
            )
        except Exception as e:
            error_message = f"Error syncing products: {e}"
            logger.error(error_message)
            yield AirbyteMessage(
                type=Type.LOG,
                log={"level":"INFO","message":f"error_message"}
            )
            raise e  # Re-raise the exception to ensure the Airbyte framework handles it.
    
    def sync_products(self):
        conn = psycopg2.connect(**DB2_CONFIG)
        try:
            with conn.cursor(cursor_factory=DictCursor) as cursor:
                # Fetch products that are not synced
                cursor.execute("""
                    SELECT p.id AS product_id, p.*, 
                        ARRAY_AGG(DISTINCT pc.category_id) AS categories,
                        ARRAY_AGG(DISTINCT ch.name) AS channels
                    FROM products p
                    LEFT JOIN product_categories pc ON p.id = pc.product_id
                    LEFT JOIN product_channels pch ON p.id = pch.product_id
                    LEFT JOIN channels ch ON ch.id = pch.channel_id
                    WHERE p.is_synced = FALSE
                    GROUP BY p.id;
                """)
                products = cursor.fetchall()

                for product in products:
                    product_id = product["product_id"]

                    # Fetch product images
                    cursor.execute("""
                        SELECT 
                            alt, sort, is_default, original_url AS original, thumbnail_url AS thumbnail
                        FROM product_images
                        WHERE product_id = %s;
                    """, (product_id,))
                    images = cursor.fetchall()

                    # Fetch product options and their values
                    cursor.execute("""
                        SELECT 
                            po.id AS option_id, po.name AS option_name, po.display_type,
                            ARRAY_AGG(pov.value_name) AS values
                        FROM product_options po
                        LEFT JOIN product_option_values pov ON po.id = pov.option_id
                        WHERE po.product_id = %s
                        GROUP BY po.id;
                    """, (product_id,))
                    options = cursor.fetchall()

                    # Format options into the expected structure
                    formatted_options = [
                        {
                            "name": option["option_name"],
                            "display_type": option["display_type"],
                            "values": [{"name": value} for value in option["values"] if value]
                        }
                        for option in options
                    ]

                    # Prepare payload for Salla API
                    payload = {
                        "name": product["name"],
                        "subtitle": product["subtitle"],
                        "description": product["description"],
                        "price": float(product["price"]),
                        "cost_price": float(product["cost_price"]) if product["cost_price"] else None,
                        "sale_price": float(product["sale_price"]) if product["sale_price"] else None,
                        "status": product["status"],
                        "weight": float(product["weight"]) if product["weight"] else None,
                        "weight_type": product["weight_type"],
                        "pinned": product["pinned"],
                        "quantity": product["quantity"],
                        "sale_end": product["sale_end"].isoformat() if product["sale_end"] else None,
                        "source_id": product["source_id"],
                        "brand_id": product["brand_id"],
                        "enable_note": product["enable_note"],
                        "is_available": product["is_available"],
                        "product_type": product["product_type"],
                        "promotion_title": product["promotion_title"],
                        "hide_quantity": product["hide_quantity"],
                        "active_advance": product["active_advance"],
                        "require_shipping": product["require_shipping"],
                        "enable_upload_image": product["enable_upload_image"],
                        "maximum_quantity_per_order": product["maximum_quantity_per_order"],
                        "images": [dict(image) for image in images],
                        "options": formatted_options,
                        "categories": list(filter(None, product["categories"])),
                        "channels": list(filter(None, product["channels"])),
                    }

                    # Send the payload to Salla API
                    if self.send_to_salla_api(payload):
                        self.update_product_sync_status(product_id, conn)

        except Exception as e:
            logger.error(f"Error during syncing products: {e}")
        finally:
            conn.close()

    def send_to_salla_api(self, payload):
        try:
            response = requests.post(SALLA_API_URL, json=payload, headers=SALLA_API_HEADERS)
            if response.status_code in (200, 201):
                logger.info(f"Product synced successfully. Response: {response.status_code}")
                return True
            else:
                logger.error(f"Failed to sync product. Status: {response.status_code}, Error: {response.text}")
                return False
        except Exception as e:
            logger.error(f"Error sending to Salla API: {e}")
            return False

    def update_product_sync_status(self, product_id, conn):
        try:
            with conn.cursor() as cursor:
                synced_time = datetime.now()
                cursor.execute("""
                    UPDATE products 
                    SET is_synced = TRUE, synced_time = %s 
                    WHERE id = %s;
                """, (synced_time, product_id))
                conn.commit()
                logger.info(f"Product {product_id} sync status updated.")
        except Exception as e:
            logger.error(f"Error updating sync status for product {product_id}: {e}")
            conn.rollback()











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










# class DestinationSalla(Destination):
#     def write(
#         self, config: Mapping[str, Any], configured_catalog: ConfiguredAirbyteCatalog, input_messages: Iterable[AirbyteMessage]
#     ) -> Iterable[AirbyteMessage]:

#         """
#         This method writes data to the Salla API.
#         """
#         api_url = config['api_url']
#         api_key = config['api_key']
#         headers = {'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'}
        
#         for message in input_messages:
#             if message.type == Type.RECORD:
#                 # Ensure the payload structure matches your requirement
#                 data = message.record.data
                
#                 # Send the data to Salla API
#                 if self.send_to_salla_api(data, api_url, headers):
#                     yield message  # Return success message

#             if message.type == Type.STATE:
#                 yield message


  
#     def send_to_salla_api(self, payload: dict, api_url: str, headers: dict) -> bool:
#         try:
#             response = requests.post(api_url, json=payload, headers=headers)
#             if response.status_code in (200, 201):
#                 logging.info(f"Product synced successfully. Response: {response.status_code}")
#                 return True
#             else:
#                 logging.error(f"Failed to sync product. Status: {response.status_code}, Error: {response.text}")
#                 return False
#         except Exception as e:
#             logging.error(f"Error sending to Salla API: {e}")
#             return False
