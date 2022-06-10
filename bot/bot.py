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

MESSAGES = {"upload": lambda x: f"Processing file with md5 {x}"}


logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)

logger = logging.getLogger(__name__)


async def handle(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    filename = os.urandom(10).hex()

    with open(os.path.join(DOWNLOAD_PATH, filename), "wb+") as f:
        a = await context.bot.get_file(update.message.document)
        await a.download(out=f)

        md5 = hashlib.md5(f.read()).hexdigest()

    try:
        userid = update.message.from_user.id

        requests.post(
            f"http://{FLUENTD_ADDRESS}:{FLUENTD_PORT}/apkAnalysis",
            json={"userid": userid, "filename": filename, "md5": md5},
        )

        logger.info(f"Event for {filename} sent to fluentd from {userid}")

    except Exception as e:
        logger.error(f"Error while sending event to fluentd: {e}")

    await update.message.reply_text(MESSAGES["upload"](md5))


if not os.path.exists(DOWNLOAD_PATH):
    os.makedirs(DOWNLOAD_PATH)

bot = Application.builder().token(TOKEN).build()

bot.add_handler(MessageHandler(filters.Document.APPLICATION, handle))

bot.run_polling()
