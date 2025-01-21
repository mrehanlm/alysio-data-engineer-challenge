import logging
import os
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///data.db")
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)


class BaseModel(Base):
    __abstract__ = True

    def __init__(self, **kwargs):
        self.validation_errors = []
        # Report unknown columns into validation errors
        unknown_columns = set(kwargs.keys()) - set([c.name for c in self.columns])
        if unknown_columns:
            self.validation_errors.append(
                f"Unknown columns: {', '.join(unknown_columns)}"
            )

        # Drop unknown columns
        for col in unknown_columns:
            kwargs.pop(col)

        super().__init__(**kwargs)

    def validate(self):
        for column in self.columns:
            value = getattr(self, column.name)
            try:
                if not column.nullable and value is None:
                    raise ValueError(f"{column.name} cannot be null.")

                if hasattr(self, f"validate_{column.name}"):
                    validator = getattr(self, f"validate_{column.name}")
                    if callable(validator):
                        value = validator(column.name, value)
                        setattr(self, column.name, value)
            except ValueError as e:
                self.validation_errors.append(str(e))
                continue

        return not self.validation_errors

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    @property
    def columns(self):
        return [c for c in self.__table__.columns]


@contextmanager
def db_session():
    session = Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
