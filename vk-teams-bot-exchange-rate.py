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
from typing import Optional, Dict, Tuple

app = Flask(__name__)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('currency_service.log')
    ]
)
logger = logging.getLogger(__name__)

# Конфигурация
TOKEN = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
BASE_CBR_URL = "https://www.cbr-xml-daily.ru"
DAILY_URL = f"{BASE_CBR_URL}/daily_json.js"
ARCHIVE_URL = f"{BASE_CBR_URL}/archive/{{year}}/{{month:02d}}/{{day:02d}}/daily_json.js"
MIN_YEAR = 2025

class CurrencyService:
    def __init__(self):
        self.last_rate: Optional[float] = None
        self.last_successful_send: Optional[datetime] = None
        self.start_time = datetime.now()
        self.http_client = httpx.Client(timeout=30.0)
        self.previous_rate: Optional[float] = None

    def __del__(self):
        self.http_client.close()

    def get_rate_with_change(self, date: datetime) -> Tuple[Optional[float], Optional[float]]:
        """Получаем курс USD и изменение от предыдущего дня"""
        try:
            if date.date() == datetime.now().date():
                response = self.http_client.get(DAILY_URL)
            else:
                if date.year < MIN_YEAR:
                    return None, None
                
                url = ARCHIVE_URL.format(
                    year=date.year,
                    month=date.month,
                    day=date.day
                )
                response = self.http_client.get(url)
            
            if response.status_code == 200:
                current_rate = round(response.json()["Valute"]["USD"]["Value"], 4)
                
                # Получаем предыдущий рабочий день
                prev_date = date - timedelta(days=1)
                prev_rate = self.get_last_available_rate(prev_date)
                
                return current_rate, (current_rate - prev_rate) if prev_rate else None
            return None, None
        except Exception as e:
            logger.error(f"Ошибка получения курса: {str(e)}")
            return None, None

    def get_last_available_rate(self, date: datetime) -> Optional[float]:
        """Рекурсивно ищет последний доступный курс"""
        if date.year < MIN_YEAR:
            return None
            
        rate, _ = self.get_rate_with_change(date)
        if rate is not None:
            return rate
        
        prev_date = date - timedelta(days=1)
        return self.get_last_available_rate(prev_date)

    def calculate_monthly_stats(self, year: int, month: int) -> Optional[Dict]:
        """Рассчитывает статистику за месяц"""
        if year < MIN_YEAR:
            logger.warning(f"Запрошен год {year} (минимум {MIN_YEAR})")
            return None
            
        last_day = calendar.monthrange(year, month)[1]
        rates = []
        current_rate = None
        
        for day in range(1, last_day + 1):
            date = datetime(year, month, day)
            rate, _ = self.get_rate_with_change(date)
            
            if rate is not None:
                current_rate = rate
            elif current_rate is None:
                current_rate = self.get_last_available_rate(date - timedelta(days=1))
                if current_rate is None:
                    continue
            
            rates.append(current_rate)
            logger.debug(f"{date.strftime('%d.%m.%Y')}: {current_rate:.4f} ₽")
        
        if not rates:
            return None
            
        last_rate = rates[-1]
        avg_rate = round(sum(rates) / len(rates), 4)
        
        return {
            "last_rate": last_rate,
            "avg_rate": avg_rate,
            "days_count": len(rates)
        }

    def send_to_chat(self, text: str) -> bool:
        """Отправляет сообщение в чат"""
        try:
            response = self.http_client.get(
                "https://api.internal.myteam.mail.ru/bot/v1/messages/sendText",
                params={"token": TOKEN, "chatId": CHAT_ID, "text": text}
            )
            if response.status_code == 200:
                logger.info("Сообщение успешно отправлено")
                return True
            logger.error(f"Ошибка отправки: {response.text}")
            return False
        except Exception as e:
            logger.error(f"Ошибка отправки сообщения: {str(e)}")
            return False

    def format_change(self, change: float) -> str:
        """Форматирует изменение курса с эмодзи"""
        if change > 0:
            return f"📈 +{abs(change):.4f}"
        elif change < 0:
            return f"📉 -{abs(change):.4f}"
        return "➡️ 0.0000"

    def send_daily_report(self) -> bool:
        """Формирует и отправляет ежедневный отчет"""
        try:
            logger.info("Начало формирования отчета")
            
            # Получаем текущий курс и изменение
            current_rate, change = self.get_rate_with_change(datetime.now())
            if current_rate is None:
                raise ValueError("Не удалось получить текущий курс")
            
            current_date = datetime.now()
            date_str = current_date.strftime("%d.%m.%Y")

            # Основное сообщение
            change_str = self.format_change(change) if change is not None else "🔄 Нет данных"
            message = (
                f"💵 Курс USD на {date_str}:\n"
                f"🔹 {current_rate:.4f} ₽\n"
                f"🔸 Изменение: {change_str}"
            )
            
            if not self.send_to_chat(message):
                return False

            # Дополнительные отчеты в последний день месяца
            if current_date.day == calendar.monthrange(current_date.year, current_date.month)[1]:
                logger.info("Отправка дополнительных отчетов за месяц")
                
                # Курс Bidease
                next_month = (current_date + timedelta(days=32)).replace(day=1)
                bidease_msg = (
                    f"🔮 Прогноз Bidease на {next_month.strftime('%B %Y')}:\n"
                    f"🔹 {round(current_rate * 1.06, 4):.4f} ₽\n"
                    f"🔸 На основе: {current_rate:.4f} ₽ × 1.06"
                )
                self.send_to_chat(bidease_msg)

                # Средневзвешенный курс
                stats = self.calculate_monthly_stats(current_date.year, current_date.month)
                if stats:
                    avg_msg = (
                        f"📊 Средний курс за {current_date.strftime('%B %Y')}:\n"
                        f"🔹 {stats['avg_rate']:.4f} ₽\n"
                        f"🔸 Дней в расчете: {stats['days_count']}\n"
                        f"🔹 Последний курс: {stats['last_rate']:.4f} ₽"
                    )
                    self.send_to_chat(avg_msg)

            self.last_successful_send = datetime.now()
            self.last_rate = current_rate
            return True

        except Exception as e:
            logger.error(f"Ошибка формирования отчета: {str(e)}", exc_info=True)
            return False

# Инициализация сервиса
currency_service = CurrencyService()

def run_scheduler():
    """Запускает планировщик задач"""
    schedule.every().day.at("18:55").do(currency_service.send_daily_report)  # 05:00 UTC = 08:00 МСК
    currency_service.send_daily_report()  # Первая отправка при запуске
    
    while True:
        schedule.run_pending()
        time.sleep(60)

@app.route('/')
def home():
    return """
    <h1>Сервис курса USD</h1>
    <p>Сервис работает. Отчеты отправляются ежедневно в 08:00 МСК.</p>
    <p>В последний день месяца включаются дополнительные отчеты.</p>
    <p><a href="/health">Проверить статус</a></p>
    """

@app.route('/health')
def health_check():
    return jsonify({
        "status": "running",
        "start_time": currency_service.start_time.isoformat(),
        "last_successful_send": currency_service.last_successful_send.isoformat() if currency_service.last_successful_send else None,
        "last_rate": currency_service.last_rate,
        "is_last_day_of_month": calendar.monthrange(datetime.now().year, datetime.now().month)[1] == datetime.now().day,
        "next_run": str(schedule.next_run()),
        "min_year": MIN_YEAR
    })

# Запуск фоновых задач
threading.Thread(target=run_scheduler, daemon=True).start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
