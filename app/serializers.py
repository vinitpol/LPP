from .resources.merge_data import HistoryInfo
from .settings import marsh
from marshmallow import fields
from .models import MergeData,OptimizeData


class HistoryInfoSchema(marsh.SQLAlchemyAutoSchema):
    class Meta:
        model = HistoryInfo
        # fields = ("unique_id",)


class MergeDataSchema(marsh.SQLAlchemyAutoSchema):
    class Meta:
        model = MergeData


class OptimizeDataSchema(marsh.SQLAlchemyAutoSchema):
    class Meta:
        model = OptimizeData