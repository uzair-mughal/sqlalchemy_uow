from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.base import Executable
from sqlalchemy.sql.expression import delete
from sqlalchemy.future import select
from sqlalchemy_uow.src.entity import Entity
from typing import Optional, List
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.engine.reflection import Inspector


class Repository:
    def __init__(self, session: AsyncSession):
        self._session = session

    async def insert(self, entity: Entity):
        self._session.add(entity)
        await self._session.flush()

    async def insert_on_conflict_do_nothing(self, entity: Entity):
        properties = entity.__dict__.copy()
        properties.pop("_sa_instance_state")
        await self._session.execute(insert(entity.__table__, properties).on_conflict_do_nothing())
        await self._session.flush()

    async def insert_on_conflict_do_update(self, entity: Entity, index_elements: list = ["id"]):
        properties = entity.__dict__.copy()
        properties.pop("_sa_instance_state")
        await self._session.execute(
            insert(entity.__table__, properties).on_conflict_do_update(index_elements=index_elements, set_=properties)
        )
        await self._session.flush()

    async def insert_ignore_get_id(self, entity: Entity):
        properties = entity.__dict__.copy()
        properties.pop("_sa_instance_state")

        query = insert(entity.__table__, properties).on_conflict_do_nothing()
        result = await self._session.execute(query)
        await self._session.flush()

        if result.inserted_primary_key:
            return result.inserted_primary_key[0]
        else:
            non_timestamp_properties = {k: v for k, v in properties.items() if type(v) in [str, int, float, bool]}
            result = await self._session.execute(select(entity.__table__).filter_by(**non_timestamp_properties))
            return result.fetchone()[0]

    async def bulk_insert(self, entities: Optional[List[Entity]] = []):
        rows = []
        if entities:
            for entity in entities:
                properties = entity.__dict__.copy()
                properties.pop("_sa_instance_state")
                rows.append(properties)
            await self._session.execute(insert(entities[0].__table__), rows)
            await self._session.flush()

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
