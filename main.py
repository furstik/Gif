import os
import asyncio
import logging
import random
import tempfile
from datetime import datetime, timedelta

import pytz
from aiogram import Bot, Dispatcher
from aiogram.types import FSInputFile
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import yadisk

# Настройка подробного логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
BOT_TOKEN = os.getenv("BOT_TOKEN")
YADISK_TOKEN = os.getenv("YADISK_TOKEN")
TARGET_CHAT_ID = os.getenv("TARGET_CHAT_ID")

# Инициализация бота и диспетчера aiogram 3.x
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

async def fetch_and_schedule_videos():
    """Ежедневная задача по скачиванию и отложенной отправке видео."""
    logger.info("Запуск ежедневной задачи: проверка Яндекс.Диска...")
    
    try:
        # Используем асинхронный клиент Яндекс.Диска
        async with yadisk.AsyncClient(token=YADISK_TOKEN) as disk:
            if not await disk.check_token():
                logger.error("Недействительный токен Яндекс.Диска!")
                return

            target_folder = "/AutoPost_Queue/"
            
            # Проверяем наличие папки и получаем список файлов
            try:
                items = [
                    item async for item in disk.listdir(target_folder) 
                    if item.type == 'file' and item.name.lower().endswith(('.mp4', '.avi', '.mov', '.mkv'))
                ]
            except yadisk.exceptions.PathNotFoundError:
                logger.error(f"Папка {target_folder} не найдена на Яндекс.Диске.")
                return

            if not items:
                logger.info("Файлов в очереди нет. Ожидание следующего дня.")
                return
            
            # Выбираем 2 случайных видео (или 1, если в папке остался только один файл)
            sample_size = min(len(items), 2)
            selected_files = random.sample(items, sample_size)
            
            # Настройка московского времени
            msk_tz = pytz.timezone('Europe/Moscow')
            now_msk = datetime.now(msk_tz)
            tomorrow_msk = now_msk + timedelta(days=1)
            
            # Точное время на завтра (14:00 и 18:00)
            schedule_times_msk = [
                tomorrow_msk.replace(hour=14, minute=0, second=0, microsecond=0),
                tomorrow_msk.replace(hour=18, minute=0, second=0, microsecond=0)
            ]
            
            for i, file in enumerate(selected_files):
                # Если файлов меньше двух, берем только первое время из массива
                schedule_time = schedule_times_msk[i]
                schedule_ts = int(schedule_time.timestamp())
                
                # Путь для сохранения во временную директорию ОС (очищается при рестарте ОС, но мы удалим руками)
                local_path = os.path.join(tempfile.gettempdir(), file.name)
                logger.info(f"Скачивание {file.name} во временный файл {local_path}...")
                
                try:
                    # 1. Скачиваем файл на VPS
                    await disk.download(file.path, local_path)
                    
                    # 2. Отправляем в Telegram с параметром schedule_date
                    logger.info(f"Отправка {file.name} как отложенное на {schedule_time.strftime('%Y-%m-%d %H:%M:%S')} MSK...")
                    video = FSInputFile(local_path)
                    
                    await bot.send_video(
                        chat_id=TARGET_CHAT_ID,
                        video=video,
                        schedule_date=schedule_ts
                    )
                    logger.info(f"✅ Успешно: Видео {file.name} запланировано.")
                    
                    # 3. Удаляем файл из Яндекс.Диска (ТОЛЬКО после успешной отправки)
                    await disk.remove(file.path)
                    logger.info(f"🗑 Успешно: Файл {file.name} удален с Яндекс.Диска.")
                    
                except Exception as e:
                    logger.error(f"❌ Ошибка при обработке {file.name}: {e}", exc_info=True)
                    # Если Telegram вернул ошибку, скрипт перейдет сюда и НЕ удалит файл из облака
                    
                finally:
                    # 4. Удаляем временный файл с VPS при любом исходе
                    if os.path.exists(local_path):
                        os.remove(local_path)
                        logger.info(f"🧹 Очистка: Временный файл {local_path} удален.")
                        
    except Exception as e:
        logger.error(f"Критическая ошибка в выполнении задачи: {e}", exc_info=True)

async def main():
    logger.info("Инициализация планировщика...")
    msk_tz = pytz.timezone('Europe/Moscow')
    scheduler = AsyncIOScheduler(timezone=msk_tz)
    
    # Добавляем задачу на ежедневный запуск в 10:00 по МСК
    scheduler.add_job(fetch_and_schedule_videos, 'cron', hour=10, minute=0)
    scheduler.start()
    
    # ---> ДОБАВЛЯЕМ ПРИНУДИТЕЛЬНЫЙ ЗАПУСК СРАЗУ ПРИ СТАРТЕ <---
    logger.info("Принудительный запуск первой итерации задачи...")
    # Оборачиваем в create_task, чтобы не заблокировать запуск бота
    asyncio.create_task(fetch_and_schedule_videos())
    
    logger.info("Планировщик запущен. Бот начинает polling (цикл событий открыт).")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Остановка работы скрипта.")
