from google import auth
from google.cloud import secretmanager

_, PROJECT_ID = auth.default()
FACEBOOK_SECRET = f"projects/{PROJECT_ID}/secrets/facebook/versions/latest"


def get_secret():
    with secretmanager.SecretManagerServiceClient() as client:
        return client.access_secret_version(
            request={
                "name": FACEBOOK_SECRET,
            }
        ).payload.data.decode("UTF-8")
