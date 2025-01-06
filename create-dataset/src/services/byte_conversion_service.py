import json


class ByteConversionService:
    @staticmethod
    def get_serialized_size(obj) -> int:
        """
        Calculate the size of serialized object in bytes.

        Parameters:
        obj (Any): The object to be serilaized and measured

        Returns:
        int: The size of the serialized object in bytes
        """
        serialized_obj = json.dumps(obj)
        return len(serialized_obj.encode('utf-8'))
