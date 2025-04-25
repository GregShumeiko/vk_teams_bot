from flask import Flask, jsonify
import threading
import httpx
from datetime import datetime, timedelta
import logging
import os
import time
import schedule
import socket
import calendar
from typing import Optional, Dict

app = Flask(__name__)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурация
TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
BASE_CBR_URL = "https://www.cbr-xml-daily.ru"
DAILY_URL = f"{BASE_CBR_URL}/daily_json.js"
ARCHIVE_URL = f"{BASE_CBR_URL}/archive/{{year}}/{{month:02d}}/{{day:02d}}/daily_json.js"

class CurrencyService:
    def __init__(self):
        self.last_rate: Optional[float] = None
        self.last_successful_send: Optional[datetime] = None
        self.start_time = datetime.now()

    def get_rate(self, date: datetime) -> Optional[float]:
        """Получаем курс USD на указанную дату"""
        try:
            if date.date() == datetime.now().date():
                response = httpx.get(DAILY_URL, timeout=10)
            else:
                url = ARCHIVE_URL.format(
                    year=date.year,
                    month=date.month,
                    day=date.day
                )
                response = httpx.get(url, timeout=10)
            
            if response.status_code == 200:
                return response.json()["Valute"]["USD"]["Value"]
            return None
        except Exception as e:
            logger.error(f"Ошибка получения курса на {date.strftime('%d.%m.%Y')}: {str(e)}")
            return None

    def get_last_available_rate(self, date: datetime) -> Optional[float]:
        """Рекурсивно ищет последний доступный курс"""
        rate = self.get_rate(date)
        if rate is not None:
            return rate
        
        prev_date = date - timedelta(days=1)
        if prev_date.year < 2024:  # Ограничиваем глубину поиска
            return None
            
        return self.get_last_available_rate(prev_date)

    def calculate_monthly_stats(self, year: int, month: int) -> Dict:
        """Рассчитывает статистику за указанный месяц"""
        last_day = calendar.monthrange(year, month)[1]
        rates = []
        current_rate = None
        
        # Собираем курсы за месяц с заполнением пропусков
        for day in range(1, last_day + 1):
            date = datetime(year, month, day)
            rate = self.get_rate(date)
            
            if rate is not None:
                current_rate = rate
            elif current_rate is None:
                # Для первых дней месяца ищем в предыдущих месяцах
                current_rate = self.get_last_available_rate(date - timedelta(days=1))
                if current_rate is None:
                    continue
            
            rates.append(current_rate)
            logger.info(f"{date.strftime('%d.%m.%Y')}: {current_rate:.2f} ₽")
        
        if not rates:
            return None
            
        last_rate = rates[-1]
        avg_rate = sum(rates) / len(rates)
        
        return {
            "last_rate": last_rate,
            "avg_rate": avg_rate,
            "days_count": len(rates)
        }

    def send_to_chat(self, text: str) -> bool:
        """Отправляет сообщение в чат (всегда реальная отправка)"""
        try:
            response = httpx.get(
                "https://api.internal.myteam.mail.ru/bot/v1/messages/sendText",
                params={"token": TOKEN, "chatId": CHAT_ID, "text": text},
                timeout=10
            )
            if response.status_code == 200:
                logger.info(f"Сообщение отправлено: {text}")
                return True
            logger.error(f"Ошибка отправки: {response.text}")
            return False
        except Exception as e:
            logger.error(f"Ошибка отправки сообщения: {str(e)}")
            return False

    def send_test_reports(self) -> bool:
        """Отправляет тестовые отчеты (март как последний день месяца)"""
        try:
            logger.info("Отправка ТЕСТОВЫХ отчетов")
            
            # 1. Получаем текущий реальный курс
            response = httpx.get(DAILY_URL, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            usd = data["Valute"]["USD"]["Value"]
            eur = data["Valute"]["EUR"]["Value"]
            current_date = datetime.fromisoformat(data["Date"])
            date_str = current_date.strftime("%d.%m.%Y")

            # 2. Отправляем текущий курс (реальный)
            current_msg = f"🟢 ТЕСТ: Текущий курс на {date_str}:\nUSD: {usd:.2f} ₽\nEUR: {eur:.2f} ₽"
            self.send_to_chat(current_msg)

            # 3. Рассчитываем статистику за март 2025 (имитация)
            march_stats = self.calculate_monthly_stats(2025, 3)
            if march_stats:
                # Курс Bidease на апрель (последний курс марта × 1.06)
                bidease_msg = (
                    f"🟡 ТЕСТ: Курс Bidease на апрель 2025:\n"
                    f"{march_stats['last_rate'] * 1.06:.2f} ₽ "
                    f"(рассчитано как {march_stats['last_rate']:.2f} * 1.06)"
                )
                self.send_to_chat(bidease_msg)

                # Средневзвешенный курс за март
                avg_msg = (
                    f"🔵 ТЕСТ: Средневзвешенный курс USD за март 2025:\n"
                    f"{march_stats['avg_rate']:.2f} ₽ "
                    f"(по {march_stats['days_count']} дням)"
                )
                self.send_to_chat(avg_msg)

            self.last_successful_send = datetime.now()
            self.last_rate = usd
            return True

        except Exception as e:
            logger.error(f"Ошибка формирования тестового отчета: {str(e)}", exc_info=True)
            return False

# Инициализация сервиса
currency_service = CurrencyService()

def run_scheduler():
    """Запускает планировщик задач"""
    # Для теста запускаем сразу при старте
    currency_service.send_test_reports()
    
    # Основное расписание (оставлено для будущего использования)
    schedule.every().day.at("05:00").do(currency_service.send_test_reports)
    
    while True:
        schedule.run_pending()
        time.sleep(60)

@app.route('/')
def home():
    return """
    <h1>Сервис курса валют (ТЕСТОВЫЙ РЕЖИМ)</h1>
    <p>Сервис в тестовом режиме. Проводится проверка расчетов за март 2025.</p>
    <p><a href="/health">Проверить статус</a></p>
    """

@app.route('/health')
def health_check():
    return jsonify({
        "status": "test_mode",
        "start_time": currency_service.start_time.isoformat(),
        "last_successful_send": currency_service.last_successful_send.isoformat() if currency_service.last_successful_send else None,
        "last_rate": currency_service.last_rate,
        "next_run": str(schedule.next_run())
    })

# Запуск фоновых задач
threading.Thread(target=run_scheduler, daemon=True).start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
