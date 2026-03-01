
from dataclasses import dataclass
from typing import List, Optional, Union
from enum import Enum

@dataclass
class QueryResult:
    """
    Represents a query result from the database.
    """
    data: list
    row_count: int
    success: bool
    query: str
    source: str
    execution_time_ms: Optional[float] = None
    error_message: Optional[str] = None

@dataclass
class TableInfo:
        project_id: str
        database_name: str
        table_name: str

class Operator(Enum):
    EQUAL = "="
    NOT_EQUAL = "!="
    GREATER_THAN = ">"
    LESS_THAN = "<"
    GREATER_THAN_OR_EQUAL = ">="
    LESS_THAN_OR_EQUAL = "<="
    CONTAINS = "LIKE"
    NOT_CONTAINS = "NOT LIKE"
    START_WITH = "LIKE"
    END_WITH = "LIKE"
    IN_ = "IN"
    NOT_IN = "NOT IN"
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"


@dataclass
class QueryFilter():
    column:str
    value: Union[str, int, List[Union[str, int]]]
    operator:Operator | str