from enum import Enum


class AttributeType(Enum):
    STRING = "S"
    NUMBER = "N"
    BINARY = "B"

    def __str__(self):
        return f"{self.value}"


class KeyType(Enum):
    PARTITION_KEY = "PartitionKey"
    PARTITION_SORT_KEY = "PartitionSortKey"
    SORT_KEY = "SortKey"

    def __str__(self):
        return f"{self.value}"
