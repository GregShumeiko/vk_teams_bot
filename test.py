from flask import Flask, jsonify
import threading
import httpx
from datetime import datetime, timedelta
import logging
import os
import time
import schedule
import calendar
from typing import Optional, Dict, Tuple, Any
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
        self.last_rate: Optional[float] = None
        self.last_successful_send: Optional[datetime] = None
        self.start_time = datetime.now()
        self.http_client = httpx.Client(timeout=30.0)
        self.rate_cache: Dict[datetime.date, float] = {}

    def __del__(self):
        self.http_client.close()

    @lru_cache(maxsize=365)
    def get_rate(self, date: datetime) -> Optional[float]:
        """Получаем курс USD с кешированием"""
        try:
            if date.date() in self.rate_cache:
                return self.rate_cache[date.date()]
                
            if date.date() == datetime.now().date():
                response = self.http_client.get(DAILY_URL)
            else:
                if date.year < MIN_YEAR:
                    return None
                url = ARCHIVE_URL.format(year=date.year, month=date.month, day=date.day)
                response = self.http_client.get(url)
            
            if response.status_code == 200:
                rate = round(response.json()["Valute"]["USD"]["Value"], 4)
                self.rate_cache[date.date()] = rate
                return rate
            return None
        except Exception as e:
            logger.error(f"Ошибка получения курса: {str(e)}")
            return None

    def get_previous_workday_rate(self, date: datetime) -> Optional[float]:
        """Ищет курс за последний доступный рабочий день"""
        for delta in range(1, 8):
            prev_date = date - timedelta(days=delta)
            rate = self.get_rate(prev_date)
            if rate is not None:
                return rate
        return None

    def get_rate_with_change(self, date: datetime) -> Tuple[Optional[float], Optional[float]]:
        """Получаем курс и изменение"""
        current_rate = self.get_rate(date)
        if current_rate is None:
            return None, None
            
        prev_rate = self.get_previous_workday_rate(date)
        change = (current_rate - prev_rate) if prev_rate is not None else None
        
        return current_rate, change

    def calculate_monthly_stats(self, year: int, month: int) -> Optional[Dict]:
        """Статистика за месяц"""
        if year < MIN_YEAR:
            return None
            
        last_day = calendar.monthrange(year, month)[1]
        rates = []
        
        for day in range(1, last_day + 1):
            date = datetime(year, month, day)
            rate = self.get_rate(date)
            if rate is not None:
                rates.append(rate)
        
        if not rates:
            return None
            
        return {
            "last_rate": rates[-1],
            "avg_rate": round(sum(rates) / len(rates), 4),
            "min_rate": min(rates),
            "max_rate": max(rates),
            "range": round(max(rates) - min(rates), 4),
            "days_count": len(rates),
            "trend": self.calculate_trend(rates)
        }

    def calculate_trend(self, rates: list) -> str:
        """Определение тренда"""
        if not rates:
            return "нет данных"
        if rates[-1] > rates[0]:
            return "📈 Рост"
        elif rates[-1] < rates[0]:
            return "📉 Падение"
        return "➡️ Стабильность"

    def send_to_chat(self, text: str) -> bool:
        """Отправка сообщения"""
        try:
            response = self.http_client.get(
                "https://api.internal.myteam.mail.ru/bot/v1/messages/sendText",
                params={"token": TOKEN, "chatId": CHAT_ID, "text": text}
            )
            if response.status_code == 200:
                logger.info("Сообщение отправлено")
                return True
            logger.error(f"Ошибка: {response.text}")
            return False
        except Exception as e:
            logger.error(f"Ошибка отправки: {str(e)}")
            return False

    def format_change(self, change: float) -> str:
        """Форматирование изменения"""
        if change is None:
            return "🔄 Нет данных"
        if change > 0:
            return f"📈 +{change:.4f}"
        elif change < 0:
            return f"📉 {change:.4f}"
        return "нет ❎"

    def format_change_percent(self, change: float, prev_rate: float) -> str:
        """Форматирование процентного изменения"""
        if change is None or prev_rate == 0:
            return "🔄 Нет данных"
        percent = (change / prev_rate) * 100
        return f"({percent:+.2f}%)"

    def send_daily_report(self) -> bool:
        """Ежедневный отчет"""
        try:
            current_rate, change = self.get_rate_with_change(datetime.now())
            if current_rate is None:
                raise ValueError("Не удалось получить курс")
            
            current_date = datetime.now()
            date_str = current_date.strftime("%d.%m.%Y")

            # Подсчет процентного изменения
            prev_rate = self.get_previous_workday_rate(current_date)
            change_percent = self.format_change_percent(change, prev_rate) if prev_rate else ""

            # Большой скачок
            jump_comment = ""
            if change and abs(change) >= 1.0:
                jump_comment = "\n🚨 Обнаружен большой скачок курса!"

            message = (
                f"💵 Курс USD на {date_str}:\n"
                f"🔹 {current_rate:.4f} ₽\n"
                f"🔸 Изменение: {self.format_change(change)} {change_percent}"
                f"{jump_comment}"
            )
            
            self.send_to_chat(message)

            # Дополнительные отчеты
            if current_date.day == 1:
                logger.info("Отправка дополнительных отчетов за предыдущий месяц")
                
                prev_month_date = (current_date.replace(day=1) - timedelta(days=1))
                stats = self.calculate_monthly_stats(prev_month_date.year, prev_month_date.month)
                
                if stats:
                    # Курс Bidease
                    bidease_msg = (
                        f"🔮 Курс Bidease на {current_date.strftime('%B %Y')}:\n"
                        f"🔹 {round(stats['last_rate'] * 1.06, 4):.4f} ₽\n"
                        f"🔸 На основе: {stats['last_rate']:.4f} ₽ × 1.06"
                    )
                    self.send_to_chat(bidease_msg)

                    # Средневзвешенный курс
                    avg_msg = (
                        f"📊 Средневзвешенный курс за {prev_month_date.strftime('%B %Y')}:\n"
                        f"🔹 {stats['avg_rate']:.4f} ₽\n"
                        f"🔸 Дней в расчете: {stats['days_count']}\n"
                        f"▪️ Последний курс месяца: {stats['last_rate']:.4f} ₽"
                    )
                    self.send_to_chat(avg_msg)

                    # Аналитика месяца
                    analytics_msg = (
                        f"📅 Аналитика за {prev_month_date.strftime('%B %Y')}:\n"
                        f"🟢 Минимальный курс: {stats['min_rate']:.4f} ₽\n"
                        f"🔴 Максимальный курс: {stats['max_rate']:.4f} ₽\n"
                        f"⚠️ Размах курса: {stats['range']:.4f} ₽\n"
                        f"🎯 Тренд месяца: {stats['trend']}"
                    )
                    self.send_to_chat(analytics_msg)

            self.last_successful_send = datetime.now()
            self.last_rate = current_rate
            return True

        except Exception as e:
            logger.error(f"Ошибка отчета: {str(e)}")
            return False

currency_service = CurrencyService()

def run_scheduler():
    """Планировщик задач"""
    schedule.every().day.at("11:30").do(currency_service.send_daily_report)  # 08:00 МСК
    schedule.every(55).minutes.do(lambda: logger.info("Self-ping to keep alive"))
    
    currency_service.send_daily_report()  # Первый запуск сразу

    while True:
        schedule.run_pending()
        time.sleep(60)

@app.route('/')
def home():
    return """
    <h1>Сервис курса USD</h1>
    <p>Сервис работает. Отчеты отправляются ежедневно в 08:00 МСК.</p>
    <p>В первый день месяца отправляются дополнительные отчеты.</p>
    <p><a href="/health">Проверить статус</a></p>
    <p><a href="/ping">Проверить активность</a></p>
    """

@app.route('/health')
def health_check():
    return jsonify({
        "status": "running",
        "start_time": currency_service.start_time.isoformat(),
        "last_successful_send": currency_service.last_successful_send.isoformat() if currency_service.last_successful_send else None,
        "last_rate": currency_service.last_rate,
        "next_run": str(schedule.next_run()),
        "min_year": MIN_YEAR,
        "cache_size": len(currency_service.rate_cache)
    })

@app.route('/ping')
def ping():
    """Эндпоинт для внешнего пинга"""
    logger.info("Received ping request")
    return jsonify({
        "status": "alive",
        "time": datetime.now().isoformat(),
        "last_report": currency_service.last_successful_send.isoformat() if currency_service.last_successful_send else None
    })

# Запуск планировщика в фоне
threading.Thread(target=run_scheduler, daemon=True).start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
