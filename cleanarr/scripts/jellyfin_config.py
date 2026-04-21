import json
import os
import sys

def configure_jellyfin_webhook():
    filepath = os.environ.get("JELLYFIN_WEBHOOK_CONFIG_PATH", "/config/plugins/configurations/Jellyfin.Plugin.Webhooks.json")
    webhook_url = os.environ.get("JELLYFIN_WEBHOOK_URL", "http://cleanarr-webhook-proxy:8000/jellyfin/webhook")
    token = os.environ.get("JELLYFIN_WEBHOOK_SECRET", "")

    if not token:
        print("Error: JELLYFIN_WEBHOOK_SECRET environment variable is not set.")
        sys.exit(1)

    # Ensure the plugin configuration directory exists
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    # Note: Jellyfin Webhook plugin Generic destination structure
    new_webhook = {
        "Url": webhook_url,
        "Name": "Cleanarr Webhook",
        "Enable": True,
        "RequestContentType": "application/json",
        "AddHeaders": [
            {
                "Key": "X-Cleanarr-Webhook-Token",
                "Value": token
            }
        ],
        "NotificationTypes": [
            "ItemMarkPlayed",
            "PlaybackStart",
            "PlaybackStopped"
        ],
        "ItemId": None,
        "UserId": None
    }

    data = []
    if os.path.exists(filepath):
        with open(filepath, "r") as f:
            try:
                data = json.load(f)
            except Exception:
                data = []

    if not isinstance(data, list):
        data = []

    # Check if our webhook already exists; update it if it does, append if not.
    found = False
    for i, item in enumerate(data):
        if isinstance(item, dict) and item.get("Url") == webhook_url:
            data[i]["AddHeaders"] = new_webhook["AddHeaders"]
            data[i]["NotificationTypes"] = list(set(item.get("NotificationTypes", []) + new_webhook["NotificationTypes"]))
            data[i]["Name"] = new_webhook["Name"]
            data[i]["Enable"] = True
            found = True
            break

    if not found:
        data.append(new_webhook)

    # Write the configuration back
    with open(filepath, "w") as f:
        json.dump(data, f, indent=4)

    print(f"Successfully configured Jellyfin webhook for {webhook_url}")

if __name__ == "__main__":
    configure_jellyfin_webhook()
