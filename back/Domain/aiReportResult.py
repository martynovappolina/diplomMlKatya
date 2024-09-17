from datetime import datetime
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from Domain.base import Base


class AiReportResult(Base):
    __tablename__ = "AiReportResults"
    Id: Mapped[str] = mapped_column(primary_key=True)
    CreateAt: Mapped[datetime] = mapped_column()
    Request: Mapped[str] = mapped_column()
    IsSuccess: Mapped[bool] = mapped_column()
    Sql: Mapped[str] = mapped_column()
    Result: Mapped[str] = mapped_column()
