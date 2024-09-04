from enum import StrEnum


class Status(StrEnum):
    DELETED = "Deleted"
    ERROR = "Error"
    PENDING = "Pending"
    PROCESSING = "Processing"