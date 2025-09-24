import re
import asyncio
import os
import logging
import requests
import httpx
import json
import fitz  
from datetime import datetime, timedelta
from urllib.parse import urljoin
from telethon import TelegramClient
from telethon.sessions import StringSession
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import pytz

# ========== ЗАГРУЗКА ПЕРЕМЕННЫХ ИЗ .env ==========
load_dotenv()

# ========== ЛОГИРОВАНИЕ ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ========== КОНФИГУРАЦИЯ ==========
API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
BOT_TOKEN = os.getenv('BOT_TOKEN')
CHANNEL_LINK = os.getenv('CHANNEL_LINK', "https://t.me/+8bLwTjyymSdjMDQy")
TIMEZONE = pytz.timezone('Europe/Moscow')
BOT_SESSION_STRING = os.getenv('BOT_SESSION_STRING')

# Конфигурация LLM
LLM_BASE_URL = "https://kong-proxy.yc.amvera.ru/api/v1/models/llama"
LLM_API_KEY = "345475782567879655"
LLM_MODEL = "llama8b"

# Пути к файлам
DATA_FOLDER = os.getenv('DATA_FOLDER', "/data")
import os
os.makedirs(DATA_FOLDER, exist_ok=True)

def download_pdf():
    """Скачивание PDF файла с финансовой отчетностью"""
    logger.info("🚀 Запуск скачивания PDF файла")

    # Настройки
    url = 'https://ir.yandex.ru/financial-releases'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    try:
        # Получаем содержимое страницы
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        # Парсим HTML
        soup = BeautifulSoup(response.text, 'html.parser')

        # Ищем все ссылки на странице
        all_links = soup.find_all('a', href=True)

        # Фильтруем ссылки, которые ведут на PDF файлы с финансовой отчетностью
        pdf_links = []
        for link in all_links:
            href = link['href']
            if ('financials' in href or 'IFRS' in href or 'financial' in href) and href.endswith('.pdf'):
                pdf_links.append(href)

        if not pdf_links:
            # Если не нашли прямых ссылок, попробуем найти по тексту
            for link in all_links:
                if link.get_text().strip() in ['Отчетность', 'Financial Statements']:
                    pdf_links.append(link['href'])

        # Выбираем первую найденную ссылку
        pdf_url = pdf_links[0]

        # Если ссылка относительная, преобразуем в абсолютную
        if not pdf_url.startswith('http'):
            pdf_url = urljoin(url, pdf_url)

        logger.info(f"Найдена ссылка на отчетность: {pdf_url}")

        # Скачиваем файл
        pdf_response = requests.get(pdf_url, headers=headers)
        pdf_response.raise_for_status()

        # Определяем имя файла
        filename = os.path.basename(pdf_url)
        if not filename.endswith('.pdf'):
            filename = f"Yandex_Financial_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"

        # Сохраняем файл в папку data
        filepath = os.path.join(DATA_FOLDER, filename)
        with open(filepath, 'wb') as f:
            f.write(pdf_response.content)

        logger.info(f'Файл успешно сохранен: {filepath}')
        logger.info(f'Размер файла: {len(pdf_response.content)} байт')
        return filepath
    except Exception as e:
        logger.error(f'Произошла ошибка при скачивании PDF: {str(e)}')
        raise

def pdf_to_text(pdf_path):
    """Преобразование PDF в текст"""
    logger.info(f"📄 Конвертация PDF в текст: {pdf_path}")

    # Формируем полные пути к файлам
    txt_filename = "output.txt"
    txt_path = os.path.join(DATA_FOLDER, txt_filename)

    try:
        # Открываем PDF файл
        doc = fitz.open(pdf_path)
        text = ""

        # Извлекаем текст из всех страниц
        for page_num in range(len(doc)):
            page = doc.load_page(page_num)
            text += page.get_text() + "\n"

        # Сохраняем текст в файл
        with open(txt_path, "w", encoding="utf-8") as txt_file:
            txt_file.write(text)

        doc.close()
        logger.info(f"Текст успешно сохранен в {txt_path}")
        return txt_path

    except Exception as e:
        logger.error(f"Ошибка при конвертации PDF: {e}")
        raise

def process_text():
    """Обработка текста с помощью LLM"""
    logger.info("🧠 Обработка текста с помощью LLM")

    # Пути к файлам
    INPUT_FILE = os.path.join(DATA_FOLDER, "output.txt")
    OUTPUT_FILE = os.path.join(DATA_FOLDER, "result.txt")

    # Чтение содержимого файла
    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            content = f.read().strip()
    except FileNotFoundError:
        logger.error(f"Файл {INPUT_FILE} не найден")
        raise
    except Exception as e:
        logger.error(f"Ошибка при чтении файла: {e}")
        raise

    if not content:
        logger.error("Файл пуст")
        raise ValueError("Файл пуст")

    logger.info(f"Прочитано из файла: {len(content)} символов")

    # Формирование промпта
    prompt = f" Составь резюме, приведи основные экономические показатели сравнивая их по годам, дай прогноз.:\n\n{content}"

    # Отправка запроса к LLM
    headers = {
        "X-Auth-Token": f"Bearer {LLM_API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "model": LLM_MODEL,
        "messages": [
            {
                "role": "user",
                "text": prompt
            }
        ]
    }

    try:
        logger.info("Отправка запроса к LLM...")
        with httpx.Client(timeout=120.0) as client:
            response = client.post(LLM_BASE_URL, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()

            # Извлекаем ответ из правильной структуры
            summary = data["result"]["alternatives"][0]["message"]["text"]

            # Сохранение результата
            with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
                f.write(summary)

            logger.info(f"Результат сохранен в файл: {OUTPUT_FILE}")
            logger.info(f"Длина ответа: {len(summary)} символов")
            return OUTPUT_FILE

    except Exception as e:
        logger.error(f"Ошибка при запросе к LLM: {e}")
        raise

def split_long_paragraph(paragraph: str, max_len: int = 4000) -&gt; list:
    """Разбивает длинный абзац на части по словам"""
    chunks = []
    while paragraph:
        if len(paragraph) &lt;= max_len:
            chunks.append(paragraph)
            break

        # Ищем место для разбиения (последний пробел перед лимитом)
        split_index = paragraph.rfind(' ', 0, max_len)
        if split_index == -1:
            # Если нет пробелов - вынужденное разбиение
            split_index = max_len

        chunk = paragraph[:split_index].strip()
        if chunk:
            chunks.append(chunk)
        paragraph = paragraph[split_index:].strip()

    return chunks

async def send_to_channel(content: str):
    """Отправка текстового контента в канал с сохранением структуры абзацев"""
    bot_client = TelegramClient(
        StringSession(BOT_SESSION_STRING) if BOT_SESSION_STRING else 'session_bot.session',
        API_ID, API_HASH
    )
    await bot_client.start(bot_token=BOT_TOKEN)

    try:
        entity = await bot_client.get_entity(CHANNEL_LINK)

        # Разбиваем текст на абзацы (разделитель - 2+ переноса строки)
        paragraphs = re.split(r'\n{2,}', content)

        # Формируем сообщения, сохраняя абзацы
        messages = []
        current_message = ""

        for paragraph in paragraphs:
            paragraph = paragraph.strip()
            if not paragraph:
                continue

            # Если абзац слишком длинный, разбиваем по словам
            if len(paragraph) &gt; 4000:
                chunks = split_long_paragraph(paragraph)
                for chunk in chunks:
                    # Добавляем два переноса для сохранения форматирования
                    chunk_text = chunk + "\n\n"
                    # Проверяем, влезет ли абзац в текущее сообщение
                    if len(current_message) + len(chunk_text) &gt; 4000 and current_message:
                        messages.append(current_message.strip())
                        current_message = ""
                    current_message += chunk_text
            else:
                # Добавляем два переноса для сохранения форматирования
                paragraph_text = paragraph + "\n\n"
                # Проверяем, влезет ли абзац в текущее сообщение
                if len(current_message) + len(paragraph_text) &gt; 4000 and current_message:
                    messages.append(current_message.strip())
                    current_message = ""
                current_message += paragraph_text

        # Добавляем последнее сообщение
        if current_message.strip():
            messages.append(current_message.strip())

        # Отправляем сформированные сообщения
        for i, message in enumerate(messages, 1):
            logger.info(f"📨 Отправка части {i}/{len(messages)} ({len(message)} символов)")
            await bot_client.send_message(
                entity, message, link_preview=False, parse_mode='markdown'
            )
            await asyncio.sleep(1)  # Задержка между сообщениями

    except Exception as e:
        logger.exception("Ошибка при отправке текста в канал")
        raise
    finally:
        await bot_client.disconnect()

async def main():
    """Главная функция, объединяющая все этапы"""
    logger.info(" Запуск полного процесса")

    # Проверка обязательных переменных
    if not all([API_ID, API_HASH, BOT_TOKEN]):
        logger.error(" Отсутствуют обязательные переменные: API_ID, API_HASH или BOT_TOKEN")
        return

    try:
        # 1. Скачивание PDF
        pdf_path = download_pdf()

        # 2. Конвертация PDF в текст
        txt_path = pdf_to_text(pdf_path)

        # 3. Обработка текста с помощью LLM
        result_path = process_text()

        # 4. Отправка результата в Telegram
        with open(result_path, 'r', encoding='utf-8') as f:
            content = f.read()

        if content:
            logger.info(f"📨 Подготовка текстового содержимого ({len(content)} символов)")
            await send_to_channel(content)
        else:
            logger.warning(" Файл result.txt пуст")

    except Exception as e:
        logger.exception(" Произошла ошибка в процессе выполнения")

if __name__ == "__main__":
    asyncio.run(main())
