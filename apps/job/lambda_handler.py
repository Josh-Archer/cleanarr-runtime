import os
import sys

def lambda_handler(event, context):
    try:
        from cleanarr.cleanup import MediaCleanup
        cleaner = MediaCleanup()
        cleaner.run()
        return {
            'statusCode': 200,
            'body': 'Cleanup executed successfully.'
        }
    except Exception as e:
        print(f"Error during cleanup execution: {e}", file=sys.stderr)
        return {
            'statusCode': 500,
            'body': str(e)
        }
