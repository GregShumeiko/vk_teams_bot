from flask import Flask, jsonify
import threading
import httpx
from datetime import datetime, timedelta
import logging
import os
import time
import schedule
import calendar
from functools import lru_cache

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
        self.start_time = datetime.now()
        self.last_successful_send = None
        self.last_rate = None
        self.rate_cache = {}
        self.http_client = httpx.Client(timeout=30.0)

    def __del__(self):
        self.http_client.close()

    @lru_cache(maxsize=365)
    def get_rate(self, date: datetime) -> float | None:
        """Получение курса на конкретную дату с кэшированием."""
        try:
            date_key = date.date()
            if date_key in self.rate_cache:
                return self.rate_cache[date_key]
            
            if date_key == datetime.now().date():
                url = DAILY_URL
            else:
                if date.year < MIN_YEAR:
                    return None
                url = ARCHIVE_URL.format(year=date.year, month=date.month, day=date.day)
            
            response = self.http_client.get(url)
            if response.status_code != 200:
                return None

            rate = round(response.json()["Valute"]["USD"]["Value"], 4)
            self.rate_cache[date_key] = rate
            return rate
        except Exception as e:
            logger.error(f"Ошибка получения курса: {e}")
            return None

    def get_previous_workday_rate(self, from_date: datetime) -> float | None:
        """Получение курса за последний рабочий день перед from_date."""
        for days_ago in range(1, 10):  # максимум 9 дней назад
            candidate = from_date - timedelta(days=days_ago)
            rate = self.get_rate(candidate)
            if rate is not None:
                return rate
        return None

    def calculate_monthly_stats(self, year: int, month: int) -> dict | None:
        """Расчет средней ставки и последней ставки месяца."""
        if year < MIN_YEAR:
            return None
        last_day = calendar.monthrange(year, month)[1]
        rates = []
        for day in range(1, last_day + 1):
            rate = self.get_rate(datetime(year, month, day))
            if rate is not None:
                rates.append(rate)

        if not rates:
            return None

        return {
            "last_rate": rates[-1],
            "avg_rate": round(sum(rates) / len(rates), 4),
            "days_count": len(rates)
        }

    def send_to_chat(self, text: str) -> bool:
        """Отправка текста в чат VK Teams."""
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
            logger.error(f"Ошибка отправки сообщения: {e}")
            return False

    def format_change(self, change: float | None) -> str:
        if change is None:
            return "🔄 Нет данных"
        return f"📈 +{change:.4f}" if change > 0 else f"📉 {change:.4f}" if change < 0 else "🚫 изменений нет"

    def send_daily_report(self) -> bool:
        """Отправка ежедневного отчета."""
        try:
            today = datetime.now()
            rate_today = self.get_rate(today)
            if rate_today is None:
                raise ValueError("Курс сегодня не получен.")

            prev_rate = self.get_previous_workday_rate(today)
            change = (rate_today - prev_rate) if prev_rate is not None else None

            date_str = today.strftime("%d.%m.%Y")
            message = (
                f"💵 Курс USD на {date_str}:\n"
                f"🔹 {rate_today:.4f} ₽\n"
                f"🔸 Изменение: {self.format_change(change)}"
            )
            self.send_to_chat(message)

            # Дополнительные отчеты в первый день месяца
            if today.day == 1:
                prev_month = today.replace(day=1) - timedelta(days=1)
                stats = self.calculate_monthly_stats(prev_month.year, prev_month.month)

                if stats:
                    avg_message = (
                        f"📊 Средневзвешенный курс за {prev_month.strftime('%B %Y')}:\n"
                        f"🔹 {stats['avg_rate']:.4f} ₽\n"
                        f"🔸 Дней в расчете: {stats['days_count']}\n"
                        f"💰 Последний курс: {stats['last_rate']:.4f} ₽"
                    )
                    self.send_to_chat(avg_message)

                    bidease_message = (
                        f"🔮 Прогноз курса Bidease на {today.strftime('%B %Y')}:\n"
                        f"🔹 {round(rate_today * 1.06, 4):.4f} ₽\n"
                        f"🔸 На основе курса: {rate_today:.4f} ₽ × 1.06"
                    )
                    self.send_to_chat(bidease_message)

            self.last_successful_send = datetime.now()
            self.last_rate = rate_today
            return True

        except Exception as e:
            logger.error(f"Ошибка отправки отчета: {e}")
            return False

currency_service = CurrencyService()

def run_scheduler():
    schedule.every().day.at("11:00").do(currency_service.send_daily_report)  # 08:00 МСК
    schedule.every(55).minutes.do(lambda: logger.info("Self-ping"))

    currency_service.send_daily_report()  # стартовое сообщение

    while True:
        schedule.run_pending()
        time.sleep(60)

@app.route('/')
def home():
    return """
    <h1>Сервис курса USD</h1>
    <p>Бот работает, отправляет курс ежедневно в 08:00 МСК.</p>
    <p><a href="/health">Проверить статус</a></p>
    <p><a href="/ping">Ping</a></p>
    """

@app.route('/health')
def health():
    return jsonify({
        "status": "running",
        "start_time": currency_service.start_time.isoformat(),
        "last_successful_send": currency_service.last_successful_send.isoformat() if currency_service.last_successful_send else None,
        "last_rate": currency_service.last_rate
    })

@app.route('/ping')
def ping():
    logger.info("Ping received")
    return jsonify({"status": "alive", "time": datetime.now().isoformat()})

# Запуск фонового потока
threading.Thread(target=run_scheduler, daemon=True).start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv("PORT", 5000)))
