import httpx
from datetime import datetime
import logging
import os
import time
import schedule

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация из переменных окружения
TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
URL_CBR = "https://www.cbr-xml-daily.ru/daily_json.js"

def send_exchange_rate():
    try:
        logger.info("Начало отправки курса валют")
        
        # Получаем данные от ЦБ
        response = httpx.get(URL_CBR)
        response.raise_for_status()
        data = response.json()

        # Извлекаем нужные данные
        usd = data["Valute"]["USD"]["Value"]
        eur = data["Valute"]["EUR"]["Value"]
        date = datetime.fromisoformat(data["Date"]).strftime("%d.%m.%Y")

        # Формируем сообщение
        message = f"Курс валют на {date}:\nUSD: {usd:.2f} ₽\nEUR: {eur:.2f} ₽"
        logger.info(f"Сформировано сообщение: {message}")

        # Отправляем в VK Teams
        send_response = httpx.get(
            "https://api.internal.myteam.mail.ru/bot/v1/messages/sendText",
            params={"token": TOKEN, "chatId": CHAT_ID, "text": message}
        )
        
        if send_response.status_code == 200:
            logger.info("Сообщение успешно отправлено!")
        else:
            logger.error(f"Ошибка отправки: {send_response.text}")

    except Exception as e:
        logger.error(f"Ошибка: {str(e)}", exc_info=True)

# Настройка расписания
schedule.every().day.at("05:00").do(send_exchange_rate)  # 05:00 UTC = 08:00 МСК

if __name__ == "__main__":
    logger.info("Сервис запущен")
    send_exchange_rate()  # Первая отправка при запуске
    
    while True:
        schedule.run_pending()
        time.sleep(60)
