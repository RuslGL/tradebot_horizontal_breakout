# Описание стратегии
1. Дневной уровень не проверяется на подверждение (не отслеживается подходили ли к нему цены в текущем дне)
2. Уровень считается пробитым, если цена закрытия текущей свечи превысила верхний уровень (для лонга), а также, когда цена закрытия была ниже нижнего уровня (для шорта) соответсвующий дневной уровень
3. Не проверяется leverage и тип маржи (изолированная/кросс)
4. Для графического ознакомления c установлением уровней можно отдельно запустить файл strategy.py
5. Позиция открывается на весь доступный баланс
6. Торговля прекращается, если общий баланс падает ниже 80% от начального (может изменяться через настройки)
7. SL - 0.99 цены входа в позицию
8. TP - 1.02 цены входа в позицию

# Базовые требования к аккаунту и настройкам
1. Тип аккаунта UNIFIED
2. Созданы api key & secret key для акканута. Ключи внесены в файл .env (см пример в файле .env_example)


# Рекомендации заказчику
1. Обдумать возможность отказа от открытия позиции, если средний оборот по фьючерсу за период ниже опредленного уровня (возможно в проценте), такие пары производят очень большое количество сигналах на коротких свечах и могут иметь сильное проскальзывание из-за низкой ликвидности
2. Обдумать возможность отказа от торговли по позициям где последние n-периодов есть пустые свечи