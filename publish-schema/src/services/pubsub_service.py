from google.cloud import pubsub_v1

class PubSubService:
    def __init__(self, project_id):
        self.project_id = project_id
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(self.project_id, "publish-schema")

    def publish_message(self, data):
        data = data.encode("utf-8")
        future = self.publisher.publish(self.topic_path, data)
        return future.result()