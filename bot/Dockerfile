FROM python:slim

WORKDIR /frontend
RUN pip install -U https://github.com/python-telegram-bot/python-telegram-bot/archive/refs/heads/master.zip
RUN pip install requests

COPY . .

CMD ["python3", "bot.py"]