import os
import uuid
import asyncio
import logging
import random
import tempfile
from datetime import datetime, timedelta

import pytz
from aiogram import Bot, Dispatcher, F
from aiogram.types import FSInputFile, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import yadisk

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
BOT_TOKEN = os.getenv("BOT_TOKEN")
YADISK_TOKEN = os.getenv("YADISK_TOKEN")
TARGET_CHAT_ID = os.getenv("TARGET_CHAT_ID")
ADMIN_ID = os.getenv("ADMIN_ID")  # <-- Твой ID для панели управления

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
msk_tz = pytz.timezone('Europe/Moscow')
scheduler = AsyncIOScheduler(timezone=msk_tz)

# Словарь для хранения запланированных задач в памяти
# Формат: { "job_id": {"file_path": "...", "file_name": "...", "run_date": datetime, "message_id": int} }
scheduled_jobs = {}

async def process_single_video(file_path: str, file_name: str, job_id: str):
    """Срабатывает строго в назначенное время: скачивает, отправляет в канал и удаляет."""
    logger.info(f"⏳ Время публикации! Начинаю обработку: {file_name}")
    local_path = os.path.join(tempfile.gettempdir(), file_name)
    
    try:
        async with yadisk.AsyncClient(token=YADISK_TOKEN) as disk:
            # 1. Скачивание
            await disk.download(file_path, local_path)
            
            # 2. Публикация в канал
            video = FSInputFile(local_path)
            await bot.send_video(chat_id=TARGET_CHAT_ID, video=video)
            
            # 3. Удаление с Диска
            await disk.remove(file_path)
            
            # 4. Обновление статуса в твоих личных сообщениях (админке)
            if job_id in scheduled_jobs:
                job_data = scheduled_jobs.pop(job_id)
                try:
                    await bot.edit_message_text(
                        f"✅ <b>Опубликовано:</b> {file_name}",
                        chat_id=ADMIN_ID,
                        message_id=job_data['message_id'],
                        parse_mode="HTML"
                    )
                except Exception as e:
                    logger.error(f"Не удалось обновить админ-сообщение: {e}")
                    
            logger.info(f"✅ Успешно: {file_name} опубликован и удален с Диска.")
            
    except Exception as e:
        logger.error(f"❌ Ошибка при публикации {file_name}: {e}", exc_info=True)
        
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)

async def assign_and_notify_video(run_date: datetime, disk: yadisk.AsyncClient) -> bool:
    """Выбирает видео, ставит в планировщик и отправляет кнопку управления админу."""
    target_folder = "/AutoPost_Queue/"
    
    try:
        items = [
            item async for item in disk.listdir(target_folder) 
            if item.type == 'file' and item.name.lower().endswith(('.mp4', '.avi', '.mov', '.mkv', '.gif'))
        ]
    except yadisk.exceptions.PathNotFoundError:
        logger.error("Папка не найдена.")
        return False

    # Исключаем файлы, которые уже стоят в очереди на публикацию (в других слотах)
    scheduled_paths = [job['file_path'] for job in scheduled_jobs.values()]
    available_items = [item for item in items if item.path not in scheduled_paths]

    if not available_items:
        return False

    selected = random.choice(available_items)
    job_id = uuid.uuid4().hex[:8]

    # Добавляем задачу в планировщик
    scheduler.add_job(
        process_single_video, 
        trigger='date', 
        run_date=run_date, 
        args=[selected.path, selected.name, job_id],
        id=job_id
    )

    # Формируем сообщение с инлайн-кнопкой
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑 Удалить и заменить", callback_data=f"replace_{job_id}")]
    ])
    
    try:
        msg = await bot.send_message(
            chat_id=ADMIN_ID,
            text=f"🕒 <b>Запланировано видео:</b> <code>{selected.name}</code>\n"
                 f"📅 <b>Время (МСК):</b> {run_date.strftime('%Y-%m-%d %H:%M:%S')}",
            reply_markup=kb,
            parse_mode="HTML"
        )
        
        # Сохраняем состояние
        scheduled_jobs[job_id] = {
            'file_path': selected.path,
            'file_name': selected.name,
            'run_date': run_date,
            'message_id': msg.message_id
        }
        return True
    except Exception as e:
        logger.error(f"Ошибка отправки уведомления админу (проверьте ADMIN_ID и диалог с ботом): {e}")
        return False

async def fetch_and_schedule_videos():
    """Ежедневная задача (в 10:00)."""
    logger.info("Запуск распределения видео на следующий день...")
    now_msk = datetime.now(msk_tz)
    tomorrow_msk = now_msk + timedelta(days=1)
    
    # Слоты времени: 14:00 и 18:00
    schedule_times = [
        tomorrow_msk.replace(hour=14, minute=0, second=0, microsecond=0),
        tomorrow_msk.replace(hour=18, minute=0, second=0, microsecond=0)
    ]
    
    async with yadisk.AsyncClient(token=YADISK_TOKEN) as disk:
        if not await disk.check_token():
            logger.error("Недействительный токен Яндекс.Диска!")
            return

        for run_date in schedule_times:
            success = await assign_and_notify_video(run_date, disk)
            if not success:
                try:
                    await bot.send_message(ADMIN_ID, f"⚠️ Не хватило видео для слота на {run_date.strftime('%H:%M')}!")
                except:
                    pass

# --- ОБРАБОТЧИК КНОПКИ "Удалить и заменить" ---
@dp.callback_query(F.data.startswith('replace_'))
async def handle_replace_video(callback: CallbackQuery):
    job_id = callback.data.split('_')[1]
    
    # Проверка актуальности задачи
    if job_id not in scheduled_jobs:
        await callback.answer("Эта задача уже выполнена или отменена.", show_alert=True)
        return
        
    job_data = scheduled_jobs.pop(job_id)
    
    # 1. Снимаем с таймера
    try:
        scheduler.remove_job(job_id)
    except Exception:
        pass
        
    async with yadisk.AsyncClient(token=YADISK_TOKEN) as disk:
        # 2. Удаляем забракованный файл с Яндекс.Диска
        try:
            await disk.remove(job_data['file_path'])
            logger.info(f"🗑 Забраковано админом. Файл {job_data['file_name']} удален с Диска.")
        except Exception as e:
            logger.error(f"Ошибка удаления файла при замене: {e}")
            
        # 3. Редактируем сообщение (убираем кнопку)
        await callback.message.edit_text(
            f"❌ <b>Забраковано и удалено:</b> <code>{job_data['file_name']}</code>", 
            parse_mode="HTML"
        )
        await callback.answer("Видео удалено. Ищу замену...")
        
        # 4. Ищем новое видео на ТОТ ЖЕ временной слот
        success = await assign_and_notify_video(job_data['run_date'], disk)
        if not success:
            await bot.send_message(ADMIN_ID, f"⚠️ Файлы закончились! Не удалось найти замену на слот {job_data['run_date'].strftime('%H:%M')}.")

async def main():
    scheduler.add_job(fetch_and_schedule_videos, 'cron', hour=10, minute=0)
    scheduler.start()
    
    # Принудительный запуск для тестирования прямо сейчас
    asyncio.create_task(fetch_and_schedule_videos())
    
    logger.info("Бот начал работу.")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
