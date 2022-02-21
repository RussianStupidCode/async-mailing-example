import asyncio
from email.message import EmailMessage
import aiosmtplib
import math
import os
from databases import Database
import time


class Contacts:
    def __init__(self, uri: str):
        self.database = Database(uri)

    async def connect(self):
        return await self.database.connect()

    async def get_count(self) -> int:
        query = f"""SELECT COUNT(*) FROM contacts """
        return await self.database.fetch_val(query=query)

    async def get_contacts(self, limit: int, offset: int) -> list:
        query = f"""SELECT * FROM contacts LIMIT {limit} OFFSET {offset}"""
        return await self.database.fetch_all(query=query)


class MessageSender:
    def __init__(self, login, app_password):
        self.login = login
        self.app_password = app_password

    async def send_message(self, address: str, first_name: str, last_name: str) -> None:
        message = EmailMessage()
        message["From"] = self.login
        message["To"] = address
        message["Subject"] = "Hello!"
        message.set_content(f"Hello {first_name}, {last_name}")

        await aiosmtplib.send(
            message,
            username=self.login,
            password=self.app_password,
            hostname="smtp.gmail.com",
            port=587,
            start_tls=True)


async def send_message_for_contacts(contacts: Contacts, sender: MessageSender, limit: int, offset: int):
    contacts_list = await contacts.get_contacts(limit, offset)
    coro_list = [sender.send_message(i["email"], i["first_name"], i["last_name"]) for i in contacts_list]
    await asyncio.gather(*coro_list)


async def main(chunk_size=10):
    contacts = Contacts('sqlite+aiosqlite:///contacts.db')
    sender = MessageSender(os.getenv("login"), os.getenv("password"))
    await contacts.connect()

    contacts_count = await contacts.get_count()
    chunk_count = math.ceil(contacts_count / chunk_size)

    for i in range(chunk_count):
        await send_message_for_contacts(contacts, sender, chunk_size, chunk_size * i)


if __name__ == "__main__":
    asyncio.run(main())
