import logging
import os
import hashlib

import requests

from telegram import Update
from telegram.ext import Application, ContextTypes, MessageHandler, filters


TOKEN = os.environ["TOKEN"]
FLUENTD_ADDRESS = os.environ["FLUENTD_ADDRESS"]
FLUENTD_PORT = os.environ["FLUENTD_PORT"]

LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DOWNLOAD_PATH = "./storage"

MESSAGES = {
    "upload": lambda x: f"Processing file with MD5 `{x}`",
    "error": "Error while processing file.\n"
    "Try again later or upload file less tha 20MB",
}


def safe_markdown(text):
    return text.replace(".", "\.")


logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)

logger = logging.getLogger(__name__)


def send_to_fluentd(filename: str, userid: int):
    with open(os.path.join(DOWNLOAD_PATH, filename), "rb") as f:
        md5 = hashlib.md5(f.read()).hexdigest()

    try:
        requests.post(
            f"http://{FLUENTD_ADDRESS}:{FLUENTD_PORT}/apkAnalysis",
            json={"userid": userid, "filename": filename, "md5": md5},
        )

        logger.info(f"Event for {filename} sent to fluentd from {userid}")

    except Exception as e:
        logger.error(f"Error while sending event to fluentd: {e}")
        return MESSAGES["error"]

    return MESSAGES["upload"](md5)


async def handle(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    filename = os.urandom(10).hex()
    userid = update.message.from_user.id

    with open(os.path.join(DOWNLOAD_PATH, filename), "wb") as f:
        a = await context.bot.get_file(update.message.document)
        await a.download(out=f)

    message = send_to_fluentd(filename, userid)

    await update.message.reply_text(safe_markdown(message), parse_mode="MarkdownV2")


async def download_bypass(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    This function is used to bypass the file size limits in the telegram bot.
    """

    filename = os.urandom(10).hex()
    userid = update.message.from_user.id

    url = update.message.text.split("-kapline")[0]

    try:
        content = requests.get(url).content

        with open(os.path.join(DOWNLOAD_PATH, filename), "wb") as f:
            f.write(content)

        logger.info(f"Downloaded {filename} from {url}")

    except Exception as e:
        logger.error(f"Error while downloading file: {e}")
        return MESSAGES["error"]

    message = send_to_fluentd(filename, userid)

    await update.message.reply_text(safe_markdown(message), parse_mode="MarkdownV2")


if not os.path.exists(DOWNLOAD_PATH):
    os.makedirs(DOWNLOAD_PATH)

bot = Application.builder().token(TOKEN).build()

bot.add_handler(MessageHandler(filters.Document.APPLICATION, handle))

bot.add_handler(
    MessageHandler(filters.Regex(r"https?:\/\/.+-kapline"), download_bypass)
)

bot.run_polling()
