import os
import asyncio
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import Column, BigInteger, String, Float, DateTime, func, text, delete, select, inspect
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.dialects.postgresql import insert

load_dotenv()

DATABASE_URL = str(os.getenv('database_url'))

Base = declarative_base()


class FutureKlines(Base):
    __tablename__ = 'future_klines'
    symbol = Column(String, primary_key=True, nullable=False)
    start = Column(BigInteger, nullable=False)  # Timestamp as start time
    interval = Column(String, nullable=False)
    open = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    #updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class FutureKlinesOperations:
    def __init__(self):
        self.engine = create_async_engine(DATABASE_URL, echo=False)
        self.async_session = sessionmaker(self.engine, class_=AsyncSession)

        # Создание таблицы, если она не существует
        # if not self.table_exists(FutureKlines.__tablename__):
        #     self.create_table()

    async def table_exists(self, table_name):
        async with self.engine.connect() as conn:
            result = await conn.scalar(
                text("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = :table_name)"),
                {"table_name": table_name}
            )
            return result

    async def create_table(self):
        if not await self.table_exists(FutureKlines.__tablename__):
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            print(f"Table '{FutureKlines.__tablename__}' created successfully.")
        else:
            print(f"Table '{FutureKlines.__tablename__}' already exists, skipping creation.")

    async def upsert_kline(self, start, symbol, interval, open, close, high, low, volume):
        async with self.async_session() as session:
            async with session.begin():
                stmt = insert(FutureKlines).values(
                    start=start,
                    symbol=symbol,
                    interval=interval,
                    open=open,
                    close=close,
                    high=high,
                    low=low,
                    volume=volume
                ).on_conflict_do_update(
                    index_elements=['symbol'],
                    set_={
                        'start': start,
                        'interval': interval,
                        'open': open,
                        'close': close,
                        'high': high,
                        'low': low,
                        'volume': volume
                    }
                )
                await session.execute(stmt)
                await session.commit()

    async def select_klines(self):
        async with self.async_session() as session:
            stmt = select(FutureKlines)
            result = await session.execute(stmt)
            klines = result.scalars().all()

            # Создаем DataFrame из списка объектов FutureKlines
            df = pd.DataFrame([
                {'start': kline.start, 'symbol': kline.symbol, 'interval': kline.interval,
                 'open': kline.open, 'close': kline.close, 'high': kline.high,
                 'low': kline.low, 'volume': kline.volume}
                for kline in klines
            ])

            return df

    async def delete_kline(self, symbol):
        async with self.async_session() as session:
            async with session.begin():
                result = await session.execute(select(FutureKlines).filter_by(symbol=symbol))
                kline = result.scalar_one()
                await session.delete(kline)
                await session.commit()

    async def select_and_delete_all_klines(self):
        async with self.async_session() as session:
            async with session.begin():
                # Выборка всех записей
                result = await session.execute(select(FutureKlines))
                klines = result.scalars().all()

                # Создаем DataFrame из списка объектов FutureKlines
                df = pd.DataFrame([
                    {'start': kline.start, 'symbol': kline.symbol, 'interval': kline.interval,
                     'open': kline.open, 'close': kline.close, 'high': kline.high,
                     'low': kline.low, 'volume': kline.volume}
                    for kline in klines
                ])

                # Удаление всех записей
                await session.execute(delete(FutureKlines))
                await session.commit()

                return df

    # Надо тестить
    # async def select_and_delete_all_klines(self):
    #     async with self.async_session() as session:
    #         async with session.begin():
    #             # Выборка всех записей
    #             result = await session.execute(select(FutureKlines))
    #             klines = result.scalars().all()
    #
    #             # Создаем DataFrame из списка объектов FutureKlines
    #             df = pd.DataFrame([
    #                 {'start': kline.start, 'symbol': kline.symbol, 'interval': kline.interval,
    #                  'open': kline.open, 'close': kline.close, 'high': kline.high,
    #                  'low': kline.low, 'volume': kline.volume}
    #                 for kline in klines
    #             ])
    #
    #             # Удаление только извлеченных записей по связке символ и старт
    #             conditions_to_delete = [(kline.symbol, kline.start) for kline in klines]
    #             for symbol, start in conditions_to_delete:
    #                 await session.execute(
    #                     delete(FutureKlines).where(
    #                         FutureKlines.symbol == symbol,
    #                         FutureKlines.start == start
    #                     )
    #                 )
    #             await session.commit()
    #
    #             return df


if __name__ == '__main__':
    async def main():

        db_futures = FutureKlinesOperations()
        await db_futures.create_table()

        await db_futures.upsert_kline(1625438400, 'BTCU', '10', 35000.0, 35500.0, 36000.0, 34500.0, 1000.0)

        df_klines = await db_futures.select_klines()
        print(df_klines)
        #
        #await db_futures.delete_kline('BTCUSDT')
        #
        #df_received = await db_futures.select_and_delete_all_klines()
        #print(df_received)


    asyncio.run(main())