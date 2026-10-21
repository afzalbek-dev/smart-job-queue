# 📦 Smart Job Queue

[![Python Version](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Zero Dependencies](https://img.shields.io/badge/dependencies-zero-brightgreen.svg)]()

Redis yoki RabbitMQ kabi qo'shimcha og'ir infratuzilmani talab qilmaydigan, to'liq **SQLite** asosida chidamli (persistent), asinxron vazifalar navbati (**Priority Task Queue**), ko'p oqimli Worker Pool, eksponentsial qayta urinish (Retry Backoff) va Nosozliklar qutisi (**Dead-Letter Queue / DLQ**).

Kichik va o'rta loyihalar, botlar, skraperlar va mikroservislar uchun ideal yechim.

---

## 🌟 Imkoniyatlari

- **Prioritetlar Tizimi:** `LOW`, `NORMAL`, `HIGH`, `CRITICAL` darajalari bo'yicha muhim vazifalarni oldin bajarish.
- **Doimiy Saqlash (Persistence):** SQLite tufayli server o'chib-yonsa ham birorta topshiriq yo'qolmaydi.
- **Asinxron Worker Pool:** Bir nechta parallel workerlar orqali fon vazifalarini asinxron boshqarish.
- **Eksponentsial Qayta Urinish (Exponential Backoff):** Xatolik berganda vazifani `2^urinish` soniya kechiktirib qayta sinash.
- **Dead-Letter Queue (DLQ):** Maksimal urinishlar sonidan oshgach, vazifani alohida DLQ holatiga o'tkazish va xatolik sababini saqlash.

---

## 🚀 O'rnatish

```bash
git clone https://github.com/afzalbek-dev/smart-job-queue.git
cd smart-job-queue
pip install .
```

---

## 🛠️ Foydalanish Namunasi

```python
import asyncio
from smart_job_queue import JobQueue, WorkerPool, JobPriority, Job

# 1. Navbatni yaratish (fayl ko'rinishida saqlanadi)
queue = JobQueue(db_path="tasks.db")

# 2. Worker Pool yaratish
pool = WorkerPool(queue, concurrency=3)

# 3. Mavzular (Topics) uchun handler funksiyalarini ulash
@pool.register("send_telegram_notification")
async def telegram_worker(job: Job):
    chat_id = job.payload["chat_id"]
    text = job.payload["text"]
    print(f"[{job.id}] Xabar yuborildi: {chat_id} -> {text}")

# 4. Navbatga vazifalar qo'shish
queue.enqueue(
    topic="send_telegram_notification",
    payload={"chat_id": 99890123, "text": "Buyurtmangiz qabul qilindi!"},
    priority=JobPriority.HIGH
)

async def main():
    pool.start()
    await asyncio.sleep(2.0)
    print("Navbat statistikasi:", queue.stats())
    await pool.stop()

asyncio.run(main())
```

---

## 🧪 Testlash

```bash
python3 -m unittest discover tests -v
```

---

## 📄 Litsenziya

MIT License. Muallif: [Afzalbek](https://github.com/afzalbek-dev).
