import httpx
from datetime import datetime
import logging
import schedule
import time

# Устанавливаем уровень логирования для httpx
logging.getLogger("httpx").setLevel(logging.WARNING)

import os
TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID", "testbot2")  # Замените на нужный chatId
URL_CBR = "https://www.cbr-xml-daily.ru/daily_json.js"

def send_exchange_rate():
    try:
        # Получаем курс валют с сайта ЦБ РФ
        response = httpx.get(URL_CBR)
        response.raise_for_status()
        data = response.json()

        usd = data["Valute"]["USD"]["Value"]
        eur = data["Valute"]["EUR"]["Value"]

        # Парсим дату из ответа
        cbr_date_raw = data["Date"]
        cbr_date = datetime.fromisoformat(cbr_date_raw).strftime("%d.%m.%Y")

        # Формируем сообщение
        message = f"Курс валют на {cbr_date}:\nUSD: {usd:.4f} ₽\nEUR: {eur:.4f} ₽"

        # Отправляем сообщение в VK Teams
        send_response = httpx.get(
            "https://api.internal.myteam.mail.ru/bot/v1/messages/sendText",
            params={
                "token": TOKEN,
                "chatId": CHAT_ID,
                "text": message
            }
        )

        if send_response.status_code == 200:
            print("Сообщение успешно отправлено!")
        else:
            print("Ошибка при отправке сообщения:", send_response.text)

    except httpx.RequestError as e:
        print(f"Ошибка при выполнении запроса: {e}")
    except Exception as e:
        print(f"Произошла ошибка: {e}")

# Планируем выполнение функции каждый день в 8:00 утра
schedule.every().day.at("08:00").do(send_exchange_rate)

# Бесконечный цикл для выполнения запланированных задач
while True:
    schedule.run_pending()
    time.sleep(60)
