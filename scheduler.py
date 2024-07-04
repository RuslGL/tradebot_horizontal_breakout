import asyncio
import schedule
from aiojobs import create_scheduler


async def scheduled_task():
    print("Executing scheduled task...")
    await asyncio.sleep(13)  # Просто для примера добавим задержку в 2 секунды
    print("Scheduled task execution complete.")


async def run_scheduler(scheduler):
    async def task_wrapper():
        await scheduled_task()

    schedule.every().day.at("11:10").do(lambda: asyncio.create_task(task_wrapper()))

    while True:
        schedule.run_pending()
        await asyncio.sleep(30)


async def main():
    print("Starting the main async function...")
    scheduler = await create_scheduler()

    await run_scheduler(scheduler)

    await scheduler.close()
    print("Main async function completed.")

if __name__ == "__main__":
    asyncio.run(main())