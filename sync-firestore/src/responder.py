import json

class ResponseModel():
    status: str
    message: str

class Responder:
    @staticmethod
    def send_response(message: str, status: str, status_code: int):
        response = ResponseModel()
        response.status = status
        response.message = message

        return json.dumps(response.__dict__), status_code, {"ContentType": "application/json"}