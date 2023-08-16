from sqlalchemy_uow.src.repository import Repository


class RepositoryConfig:
    def __init__(self, name: str, repository: type(Repository)):
        self.name = name
        self.repository = repository
