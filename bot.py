from flask import Flask, jsonify
import threading
import httpx
from datetime import datetime
import logging
import os
import time
import schedule
import socket

app = Flask(__name__)

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация из переменных окружения
TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
URL_CBR = "https://www.cbr-xml-daily.ru/daily_json.js"

# Глобальные переменные для мониторинга
service_start_time = datetime.now()
last_successful_send = None

def send_exchange_rate():
    global last_successful_send
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
            last_successful_send = datetime.now()
            return True
        else:
            logger.error(f"Ошибка отправки: {send_response.text}")
            return False

    except Exception as e:
        logger.error(f"Ошибка: {str(e)}", exc_info=True)
        return False

def run_schedule():
    # Настройка расписания
    schedule.every().day.at("05:00").do(send_exchange_rate)  # 05:00 UTC = 08:00 МСК
    
    # Первая отправка при запуске
    send_exchange_rate()
    
    while True:
        schedule.run_pending()
        time.sleep(60)

@app.route('/')
def home():
    return """
    <h1>Сервис отправки курса валют</h1>
    <p>Сервис работает. Ожидайте ежедневное сообщение в 08:00 МСК.</p>
    <p>Проверьте <a href="/health">статус работы</a> сервиса.</p>
    """

@app.route('/health')
def health_check():
    hostname = socket.gethostname()
    status = {
        "status": "healthy",
        "service_start_time": service_start_time.isoformat(),
        "last_successful_send": last_successful_send.isoformat() if last_successful_send else None,
        "hostname": hostname,
        "current_time": datetime.now().isoformat(),
        "next_scheduled_run": str(schedule.next_run())
    }
    return jsonify(status)

# Запуск фоновой задачи при старте приложения
threading.Thread(target=run_schedule, daemon=True).start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
