import logging
import os

import requests

from telegram import Update
from telegram.ext import Application, ContextTypes, MessageHandler, filters


TOKEN = os.environ["TOKEN"]
FLUENTD_ADDRESS = os.environ["FLUENTD_ADDRESS"]
FLUENTD_PORT = os.environ["FLUENTD_PORT"]

LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DOWNLOAD_PATH = "./storage"

MESSAGES = {"upload": "Processing..."}


logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)

logger = logging.getLogger(__name__)


async def handle(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    filename = os.urandom(10).hex()

    with open(os.path.join(DOWNLOAD_PATH, filename), "wb") as f:
        a = await context.bot.get_file(update.message.document)
        await a.download(out=f)

    try:
        requests.post(
            f"http://{FLUENTD_ADDRESS}:{FLUENTD_PORT}/apkAnalysis",
            json={"filename": filename},
        )

        logger.info(f"Event for {filename} sent to fluentd")

    except Exception as e:
        logger.error(f"Error while sending event to fluentd: {e}")

    await update.message.reply_text(MESSAGES["upload"])


if not os.path.exists(DOWNLOAD_PATH):
    os.makedirs(DOWNLOAD_PATH)

bot = Application.builder().token(TOKEN).build()

bot.add_handler(MessageHandler(filters.Document.APPLICATION, handle))

bot.run_polling()
