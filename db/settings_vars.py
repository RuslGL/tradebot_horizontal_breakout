import os
import asyncio
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import Column, BigInteger, String, Float, DateTime, func, text, delete, select, inspect
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.dialects.postgresql import insert
import json

load_dotenv()
DATABASE_URL = str(os.getenv('database_url'))


Base = declarative_base()


class SettingsVars(Base):
    __tablename__ = 'settings_vars'
    name = Column(String, primary_key=True, nullable=False)
    value = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class SettingsVarsOperations:
    def __init__(self):
        self.engine = create_async_engine(DATABASE_URL, echo=False)
        self.async_session = sessionmaker(self.engine, class_=AsyncSession)


    async def table_exists(self, table_name):
        async with self.engine.connect() as conn:
            result = await conn.scalar(
                text("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = :table_name)"),
                {"table_name": table_name}
            )
            return result

    async def create_table(self):
        if not await self.table_exists(SettingsVars.__tablename__):
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            print(f"Table '{SettingsVars.__tablename__}' created successfully.")
        else:
            print(f"Table '{SettingsVars.__tablename__}' already exists, skipping creation.")


    async def upsert_settings(self, name, value):
        async with self.async_session() as session:
            async with session.begin():
                stmt = insert(SettingsVars).values(
                    name=name,
                    value=value
                ).on_conflict_do_update(
                    index_elements=['name'],
                    set_={
                        'value': value,
                    }
                )
                await session.execute(stmt)
                await session.commit()

#
    async def select_all(self):
        async with self.async_session() as session:
            stmt = select(SettingsVars)
            result = await session.execute(stmt)
            return result.scalars().all()


if __name__ == '__main__':
    async def main():

        db_settings_vars = SettingsVarsOperations()
        #await db_settings_vars.create_table()

        #await db_settings_vars.upsert_settings( 'DB', '2')
        #await db_settings_vars.upsert_settings('DBBB', '2')
        # res = await db_settings_vars.select_all()
        settings = {row.name: row.value for row in await db_settings_vars.select_all()}

        days_levels = json.loads(settings['days_levels'].replace("'", '"'))
        print(days_levels.get('BTCUSDT'))


    asyncio.run(main())