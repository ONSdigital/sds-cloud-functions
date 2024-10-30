import base64

from cloudevents.http import CloudEvent
import functions_framework

logger = logging.getLogger(__name__)

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def subscribe(cloud_event: CloudEvent) -> None:
    logger.debug(base64.b64decode(cloud_event.data["message"]["data"]).decode())
