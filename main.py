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

BOT_TOKEN = os.getenv("BOT_TOKEN")
YADISK_TOKEN = os.getenv("YADISK_TOKEN")
TARGET_CHAT_ID = os.getenv("TARGET_CHAT_ID")
ADMIN_ID = os.getenv("ADMIN_ID")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
msk_tz = pytz.timezone('Europe/Moscow')
scheduler = AsyncIOScheduler(timezone=msk_tz)

# Словарь для хранения состояний
scheduled_jobs = {}

async def process_single_video(file_path: str, file_name: str, job_id: str):
    """Срабатывает строго в назначенное время: публикует в канал и удаляет с Диска."""
    logger.info(f"⏳ Время публикации! Начинаю обработку: {file_name}")
    
    local_path = os.path.join(tempfile.gettempdir(), file_name)
    
    try:
        job_data = scheduled_jobs.pop(job_id, None)
        
        async with yadisk.AsyncClient(token=YADISK_TOKEN) as disk:
            # 1. Отправка в канал
            if job_data and 'file_id' in job_data:
                # ОПТИМИЗАЦИЯ: Видео уже загружено в Telegram, отправляем по file_id мгновенно
                logger.info(f"Использую закэшированный file_id для отправки {file_name}")
                await bot.send_video(chat_id=TARGET_CHAT_ID, video=job_data['file_id'])
            else:
                # Резервный вариант, если скрипт перезапускался и потерял кэш
                logger.info(f"Скачивание {file_name} на VPS (резервный метод)...")
                await disk.download(file_path, local_path)
                video = FSInputFile(local_path)
                await bot.send_video(chat_id=TARGET_CHAT_ID, video=video)
            
            # 2. Удаление с Диска
            await disk.remove(file_path)
            
            # 3. Обновление статуса в личных сообщениях
            if job_data:
                try:
                    # ИЗМЕНЕНО: теперь редактируем caption (подпись к видео)
                    await bot.edit_message_caption(
                        chat_id=ADMIN_ID,
                        message_id=job_data['message_id'],
                        caption=f"✅ <b>Опубликовано:</b> {file_name}",
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
    """Выбирает видео, показывает его админу и ставит в расписание."""
    target_folder = "/AutoPost_Queue/"
    
    try:
        items = [
            item async for item in disk.listdir(target_folder) 
            if item.type == 'file' and item.name.lower().endswith(('.mp4', '.avi', '.mov', '.mkv', '.gif'))
        ]
    except yadisk.exceptions.PathNotFoundError:
        logger.error("Папка не найдена.")
        return False

    scheduled_paths = [job.get('file_path') for job in scheduled_jobs.values()]
    available_items = [item for item in items if item.path not in scheduled_paths]

    if not available_items:
        return False

    selected = random.choice(available_items)
    job_id = uuid.uuid4().hex[:8]
    local_path = os.path.join(tempfile.gettempdir(), selected.name)

    try:
        # Скачиваем файл временно, чтобы отправить админу
        logger.info(f"Скачивание {selected.name} для уведомления админа...")
        await disk.download(selected.path, local_path)
        video_file = FSInputFile(local_path)
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🗑 Удалить и заменить", callback_data=f"replace_{job_id}")]
        ])
        
        # ИЗМЕНЕНО: Отправляем само видео с подписью
        msg = await bot.send_video(
            chat_id=ADMIN_ID,
            video=video_file,
            caption=f"🕒 <b>Запланировано видео:</b> <code>{selected.name}</code>\n"
                    f"📅 <b>Время (МСК):</b> {run_date.strftime('%Y-%m-%d %H:%M:%S')}",
            reply_markup=kb,
            parse_mode="HTML"
        )
        
        # Сохраняем состояние, ВКЛЮЧАЯ file_id от Telegram
        scheduled_jobs[job_id] = {
            'file_path': selected.path,
            'file_name': selected.name,
            'run_date': run_date,
            'message_id': msg.message_id,
            'file_id': msg.video.file_id  # Сохраняем ID видео на серверах TG
        }

        scheduler.add_job(
            process_single_video, 
            trigger='date', 
            run_date=run_date, 
            args=[selected.path, selected.name, job_id],
            id=job_id
        )
        return True
        
    except Exception as e:
        logger.error(f"Ошибка отправки уведомления админу: {e}")
        return False
        
    finally:
        # Удаляем локальный файл в любом случае
        if os.path.exists(local_path):
            os.remove(local_path)

async def fetch_and_schedule_videos():
    """Ежедневная задача (в 10:00)."""
    logger.info("Запуск распределения видео на следующий день...")
    now_msk = datetime.now(msk_tz)
    tomorrow_msk = now_msk + timedelta(days=1)
    
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

# --- ОБРАБОТЧИК КНОПКИ ---
@dp.callback_query(F.data.startswith('replace_'))
async def handle_replace_video(callback: CallbackQuery):
    job_id = callback.data.split('_')[1]
    
    if job_id not in scheduled_jobs:
        await callback.answer("Эта задача уже выполнена или отменена.", show_alert=True)
        return
        
    job_data = scheduled_jobs.pop(job_id)
    
    try:
        scheduler.remove_job(job_id)
    except Exception:
        pass
        
    async with yadisk.AsyncClient(token=YADISK_TOKEN) as disk:
        try:
            await disk.remove(job_data['file_path'])
            logger.info(f"🗑 Забраковано админом. Файл {job_data['file_name']} удален с Диска.")
        except Exception as e:
            logger.error(f"Ошибка удаления файла при замене: {e}")
            
        # ИЗМЕНЕНО: Редактируем подпись к видео (текст кнопки пропадает)
        await callback.message.edit_caption(
            caption=f"❌ <b>Забраковано и удалено:</b> <code>{job_data['file_name']}</code>", 
            parse_mode="HTML"
        )
        await callback.answer("Видео удалено. Ищу замену...")
        
        success = await assign_and_notify_video(job_data['run_date'], disk)
        if not success:
            await bot.send_message(ADMIN_ID, f"⚠️ Файлы закончились! Не удалось найти замену на слот {job_data['run_date'].strftime('%H:%M')}.")

async def main():
    scheduler.add_job(fetch_and_schedule_videos, 'cron', hour=10, minute=0)
    scheduler.start()
    
    # Принудительный запуск для тестирования
    asyncio.create_task(fetch_and_schedule_videos())
    
    logger.info("Бот начал работу.")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
