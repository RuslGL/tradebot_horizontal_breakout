import os
import asyncio
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, types, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton

from db.settings_vars import SettingsVarsOperations


load_dotenv()


bot_question = None
settings = {
    # fixed and default settings now - if required mb changed via telg bot
    'if_test': '1',
    'risk_limit': '0.8',
    'tp_rate': '0.02',
    'sl_rate': '0.01',
    'sma_period': '5',

    # changed via teleg bot under user control
    'start_trade': '0',
}


kline_values = ['1', '3', '5', '15', '30', '60', '120', '240', '360', '720', 'D', 'M', 'W']
window_values = [str(i) for i in range(5, 366)]  # [5-365]
multiplicator_values = [str(i) for i in range(1, 51)]  # [1-50]

owner_id = int(os.getenv('owner_id'))
chat_id = int(os.getenv('private_channel'))
telegram_token = str(os.getenv('boto_token'))


bot = Bot(token=telegram_token,
          default=DefaultBotProperties(
              parse_mode=ParseMode.HTML),
          disable_web_page_preview=True
          )

dp = Dispatcher()
# DB connector
db_settings_vars = SettingsVarsOperations()


async def menu_trade_off():
    btn_1 = InlineKeyboardButton(
        text='Включить торговлю',
        callback_data='menu_trade_on'
    )

    our_menu = [[btn_1]]
    return InlineKeyboardMarkup(inline_keyboard=our_menu)


async def menu_trade_on():
    btn_1 = InlineKeyboardButton(
        text='Отключить торговлю',
        callback_data='menu_trade_off'
    )
    our_menu = [[btn_1]]
    return InlineKeyboardMarkup(inline_keyboard=our_menu)


@dp.callback_query(F.data == 'menu_trade_on')
async def start(message: types.Message | types.CallbackQuery):
    await db_settings_vars.upsert_settings('start_trade', '1')
    await bot.send_message(
        chat_id=chat_id,
        text='Режим торговли активен',
        reply_markup=await menu_trade_on())

@dp.callback_query(F.data == 'menu_trade_off')
async def stop(message: types.Message | types.CallbackQuery):
    await db_settings_vars.upsert_settings('start_trade', '0')
    await bot.send_message(
        chat_id=chat_id,
        text='Режим торговли отключен',
        reply_markup=await menu_trade_off())


def validate_and_set_params(param, value, valid_range):
    global settings
    if value in valid_range:
        settings[param] = value
        return True
    return False


async def on_start():
    # executed once upon start code running
    global bot_question
    bot_question = 'window'
    await bot.send_message(chat_id=chat_id, text='Для старта торговли необходимо заполнить параметры стратегии')
    await bot.send_message(
        chat_id=chat_id,
        text=(
            "Введите параметр window.\n\n"
            "Window применяется для расчета дневных уровней и определяет период расчета.\n\n"
            "Допустимые значения: целые числа [5 - 365]."
        )
    )


@dp.message(F.text)
async def gather_settings(message: Message):
    global bot_question, settings

    if bot_question == 'window':
        corect = validate_and_set_params(
            'window', message.text, window_values)
        print(corect)

        if corect:
            bot_question = 'kline'
            await message.answer(
                text=(
                    "Введите параметр kline - размер/длительность свечи.\n\n"
                    "Допустимые значения:\n1, 3, 5, 15, 30, 60,\n120, 240, 360, 720,\nD, M, W"
                )
            )
        else:
            await message.answer(f'Уcтановите корректное значение window')
        return

    if bot_question == 'kline':
        corect = validate_and_set_params(
            'kline', message.text, kline_values)
        print(corect)

        if corect:
            bot_question = 'multiplicator'
            await message.answer(
                text=(
                    "Введите параметр multiplicator.\n\n"
                    "Применятеся для расчета интересующего объема при пробое.\n\n"
                    "То есть, позиция размещается, если текущий объем "
                    "в multiplicator раз больше среднего объема за прошлый период \n\n"
                    "Допустимые значения: целые числа [1 - 50]."
                )
            )
        else:
            await message.answer(f'Уcтановите корректное значение kline')
        return

    if bot_question == 'multiplicator':
        corect = validate_and_set_params(
             'multiplicator', message.text, multiplicator_values)
        print(corect)
    #
        if corect:
            global db_settings_vars
            await db_settings_vars.create_table()
            status = await db_settings_vars.upsert_settings_bulk(settings)
            if status:
                await message.answer(
                    text='Все настройки установлены.\n\nТорговый бот запускается.\n\nСейчас вы можете начать торговлю.',
                    reply_markup= await menu_trade_off())

            else:
                await message.answer(text=(
                    "Что-то пошло не так, давайте попробуем еще раз. \n\n"
                    "Если ошибка повторится - свяжитесь с разработчиком!"))
        else:
            await message.answer(f'Уcтановите корректное значение multiplicator')
        return



async def start_bot():
    dp.startup.register(on_start)

    await bot.delete_webhook(drop_pending_updates=True)
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()


if __name__ == '__main__':
    asyncio.run(start_bot())
