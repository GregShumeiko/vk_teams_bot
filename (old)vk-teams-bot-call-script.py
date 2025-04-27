import logging
import requests
import time
import subprocess
import json

# Настройка логирования
logging.basicConfig(level=logging.INFO)

# Токен и ID чата для отправки сообщений
TOKEN = '002.1881976940.2477662618:1000005422'
CHAT_ID = 'testbot2'  # Замените на ID вашего чата

# URL для получения новых сообщений
API_URL = 'https://api.internal.myteam.mail.ru/bot/v1/events/get'
SEND_URL = 'https://api.internal.myteam.mail.ru/bot/v1/messages/sendText'

# Функция для отправки сообщения в чат
def send_message(text):
    params = {
        'token': TOKEN,
        'chatId': CHAT_ID,
        'text': text
    }
    try:
        response = requests.get(SEND_URL, params=params, timeout=10)
        response.raise_for_status()
        logging.info(f"Сообщение отправлено: {text}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка при отправке сообщения: {e}")

# Функция для получения новых сообщений
def get_new_messages(last_event_id):
    params = {
        'token': TOKEN,
        'lastEventId': last_event_id,
        'pollTime': 60  # Увеличенный интервал опроса
    }
    try:
        response = requests.get(API_URL, params=params, timeout=70)
        response.raise_for_status()
        events = response.json().get('events', [])
        return events
    except requests.exceptions.RequestException as e:
        logging.error(f"Ошибка при получении событий: {e}")
        return []

# Функция для вызова скрипта и получения результата
def call_script(script_name):
    try:
        result = subprocess.run(['python3', script_name], capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        logging.error(f"Ошибка при выполнении {script_name}: {e}")
        return f"Ошибка при выполнении {script_name}."

# Главная функция
def main():
    logging.info("Запуск бота...")
    last_event_id = 0
    while True:
        events = get_new_messages(last_event_id)
        for event in events:
            last_event_id = event.get('eventId', last_event_id)
            if event.get('type') == 'newMessage':
                text = event['payload'].get('text', '').strip()
                if text == '/rate':
                    logging.info("Команда /rate получена.")
                    message = call_script('get_exchange_rate.py')
                    send_message(message)
                elif text == '/rateavg':
                    logging.info("Команда /rateavg получена.")
                    message = call_script('get_rate_avg.py')
                    send_message(message)

if __name__ == '__main__':
    main()
