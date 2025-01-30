from services.schema_service import SCHEMA_SERVICE


class Schema:
    def __init__(self, schema_json: dict, filepath: str) -> None:
        self.json = schema_json
        self.filepath = filepath
        self.survey_id = SCHEMA_SERVICE.fetch_survey_id(schema_json)

    def get_json(self) -> dict:
        return self.json

    def get_filepath(self) -> str:
        return self.filepath

    def get_survey_id(self) -> str:
        return self.survey_id
