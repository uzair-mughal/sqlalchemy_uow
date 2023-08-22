from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.schema import CreateSchema
from sqlalchemy.orm import sessionmaker
from typing import List
from sqlalchemy_uow.src.config import RepositoryConfig
from sqlalchemy_uow.src.entity import Entity


class UnitOfWork:
    def __init__(self, engine: AsyncEngine, repositories: List[RepositoryConfig]):
        self._engine = engine
        self._repositories = repositories
        self._session = None

    @asynccontextmanager
    async def session(self):
        if self._session is None:
            self._session = sessionmaker(
                self._engine, autoflush=False, autocommit=False, class_=AsyncSession
            )()

        try:
            for repository in self._repositories:
                setattr(self, repository.name, repository.repository(self._session))

            yield self._session
        except Exception as error:
            await self._session.close()
            raise error

    async def start_transaction(self):
        async with self.session() as session:
            await session.begin()

    async def rollback(self):
        async with self.session() as session:
            await session.rollback()

    async def commit(self):
        async with self.session() as session:
            await session.commit()

    async def close(self):
        async with self.session() as session:
            await session.close()

    async def create_schemas(self, schemas: List[str]):
        for schema in schemas:
            try:
                await self.start_transaction()
                await self.execute(CreateSchema(name=schema))
                await self.commit()
                await self.close()
            except:
                await self.close()
        
    async def create_tables(self):
        async with self._engine.begin() as connection:
            await connection.run_sync(Entity.metadata.create_all)

    async def drop_tables(self):
        async with self._engine.begin() as connection:
            await connection.run_sync(Entity.metadata.drop_all)

    async def execute(self, statement):
        async with self._engine.begin() as connection:
            await connection.execute(statement)

    async def migrate(self, entity: Entity, data: List[dict]):
        async with self._engine.begin() as connection:
            await connection.execute(entity.__table__.insert(), data)
