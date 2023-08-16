from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.base import Executable
from sqlalchemy.sql.expression import delete
from sqlalchemy.future import select
from sqlalchemy_uow.src.entity import Entity
from typing import Optional, List


class Repository:
    def __init__(self, session: AsyncSession):
        self._session = session

    async def insert(self, entity: Entity):
        self._session.add(entity)
        await self._session.flush()

    async def bulk_insert(self, entities: Optional[List[Entity]] = []):
        rows = []
        if entities:
            for entity in entities:
                properties = entity.__dict__.copy()
                properties.pop("_sa_instance_state")
                rows.append(properties)
            await self.session.execute(insert(entities[0].__table__), rows)
            await self.session.flush()

    async def select(self, entity: Entity, filters: dict):
        rows = await self._session.execute(select(entity).filter_by(**filters))

        try:
            return rows.fetchone()[0]
        except Exception as error:
            return None

    async def select_all(self, entity: Entity, filters: dict):
        rows = await self._session.execute(select(entity).filter_by(**filters))

        return [row for row, in rows.fetchall()]

    async def update(self, entity: Entity, filters: dict, properties: dict):
        rows = await self._session.execute(select(entity).filter_by(**filters))

        rows = [row for row, in rows.fetchall()]

        for row in rows:
            for key, value in properties.items():
                setattr(row, key, value)

    async def delete(self, entity: Entity, filters: dict):
        await self._session.execute(delete(entity).filter_by(**filters))

    async def execute(self, query: Executable):
        rows = await self._session.execute(query)
        return rows.fetchall()

    async def execute_statement(self, query: Executable):
        result = await self._session.execute(query)
        return result
