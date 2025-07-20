from sqlalchemy.exc import SQLAlchemyError

Error = SQLAlchemyError


class DoesNotExistError(Error):
    pass


class AlreadyExistsError(Error):
    pass
