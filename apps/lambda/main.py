import os
import json
from loguru import logger
from cleanarr.cleanup import MediaCleanup

def lambda_handler(event, context):
    try:
        logger.info("Starting Lambda execution for Cleanarr cleanup")
        cleaner = MediaCleanup()
        cleaner.run()
        return {
            "statusCode": 200,
            "body": json.dumps("Cleanup successful")
        }
    except Exception as e:
        logger.error(f"Handler error: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps(str(e))
        }

if __name__ == "__main__":
    lambda_handler(None, None)
