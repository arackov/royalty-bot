import os
import sqlite3
import asyncio
import threading
import logging
import uuid
from flask import Flask
from aiogram import Bot, Dispatcher
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, FSInputFile
from aiogram.filters import Command
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from dotenv import load_dotenv
import openpyxl
from datetime import datetime

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

load_dotenv()

# ОСТАВЛЕНО КАК ПРОСИЛ
API_TOKEN = "8556100624:AAEjTGUaj3P5xS0fTtlYRj5DeDw9j5pZdU8"

ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
ALLOWED_USERS = [int(x.strip()) for x in os.getenv("ALLOWED_USERS", "").split(",") if x.strip()]

bot = Bot(token=API_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

flask_app = Flask(__name__)

@flask_app.route('/')
@flask_app.route('/health')
def health():
    return "Bot is running", 200


# ---------- FSM ----------
class ReportState(StatesGroup):
    waiting_contracts = State()
    waiting_quarters = State()
    waiting_years = State()
    waiting_types = State()
    waiting_songs = State()
    waiting_author_percent = State()
    waiting_related_percent = State()


# ---------- ACCESS ----------
def is_allowed(user_id: int):
    # если ничего не задано — доступ открыт
    if not ALLOWED_USERS and ADMIN_ID == 0:
        return True
    return user_id in ALLOWED_USERS or user_id == ADMIN_ID


# ---------- DB ----------
ALLOWED_COLUMNS = {"contract", "year"}

def get_db_connection():
    try:
        conn = sqlite3.connect('royalties.db', check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception:
        logger.exception("DB connection error")
        return None


def get_unique_values(column):
    if column not in ALLOWED_COLUMNS:
        return []

    conn = get_db_connection()
    if not conn:
        return []

    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT DISTINCT {column}
            FROM royalties
            WHERE {column} IS NOT NULL AND {column} != ''
            ORDER BY {column}
        """)
        return [row[0] for row in cursor.fetchall()]
    except Exception:
        logger.exception("get_unique_values error")
        return []
    finally:
        conn.close()


def get_songs():
    conn = get_db_connection()
    if not conn:
        return []

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT display_name FROM royalties ORDER BY display_name")
        return [row[0] for row in cursor.fetchall()]
    except Exception:
        logger.exception("get_songs error")
        return []
    finally:
        conn.close()


# ---------- KEYBOARD ----------
def build_multi_select_keyboard(items, selected_items, prefix, page=0, items_per_page=10):
    keyboard = []
    start_idx = page * items_per_page
    end_idx = min(start_idx + items_per_page, len(items))

    for idx, item in enumerate(items[start_idx:end_idx], start=start_idx):
        is_selected = idx in selected_items
        emoji = "✅ " if is_selected else "⬜ "
        callback_data = f"{prefix}_toggle_{idx}"
        keyboard.append([InlineKeyboardButton(text=f"{emoji}{item}", callback_data=callback_data)])

    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="◀️", callback_data=f"{prefix}_page_{page-1}"))
    if end_idx < len(items):
        nav_buttons.append(InlineKeyboardButton(text="▶️", callback_data=f"{prefix}_page_{page+1}"))
    if nav_buttons:
        keyboard.append(nav_buttons)

    keyboard.append([InlineKeyboardButton(text="✅ Готово", callback_data=f"{prefix}_done")])

    return InlineKeyboardMarkup(inline_keyboard=keyboard)


# ---------- COMMANDS ----------
@dp.message(Command("start"))
async def cmd_start(message: Message):
    logger.info(f"user id: {message.from_user.id}")

    if not is_allowed(message.from_user.id):
        await message.answer("⛔ У вас нет доступа.")
        return

    await message.answer("Бот работает. Используй /report")


@dp.message(Command("report"))
async def cmd_report(message: Message, state: FSMContext):
    if not is_allowed(message.from_user.id):
        return

    contracts = get_unique_values("contract")

    if not contracts:
        await message.answer("❌ База данных пуста или нет таблицы royalties")
        return

    await state.clear()
    await state.update_data(
        contracts=contracts,
        selected_contracts=[],
        contract_page=0
    )

    keyboard = build_multi_select_keyboard(contracts, [], "contract")

    await state.set_state(ReportState.waiting_contracts)
    await message.answer("📋 Выберите договоры:", reply_markup=keyboard)


# ---------- CALLBACK ----------
@dp.callback_query()
async def handle_callback(callback: CallbackQuery, state: FSMContext):
    data = callback.data
    user_data = await state.get_data()

    try:
        # CONTRACTS
        if data.startswith("contract_"):
            items = user_data["contracts"]
            selected = user_data.get("selected_contracts", [])

            if data == "contract_done":
                if not selected:
                    await callback.answer("Выберите хотя бы один", show_alert=True)
                    return

                await state.update_data(
                    selected_contracts=[items[i] for i in selected]
                )

                quarters = ["I", "II", "III", "IV"]

                await state.update_data(
                    quarters=quarters,
                    selected_quarters=[],
                    quarter_page=0
                )

                keyboard = build_multi_select_keyboard(quarters, [], "quarter")

                await state.set_state(ReportState.waiting_quarters)
                await callback.message.edit_text("📅 Кварталы:", reply_markup=keyboard)

            elif data.startswith("contract_toggle_"):
                idx = int(data.split("_")[-1])
                if idx in selected:
                    selected.remove(idx)
                else:
                    selected.append(idx)

                await state.update_data(selected_contracts=selected)

                keyboard = build_multi_select_keyboard(items, selected, "contract")
                await callback.message.edit_reply_markup(reply_markup=keyboard)

        # QUARTERS
        elif data.startswith("quarter_"):
            items = user_data["quarters"]
            selected = user_data.get("selected_quarters", [])

            if data == "quarter_done":
                if not selected:
                    await callback.answer("Выберите квартал", show_alert=True)
                    return

                await state.update_data(
                    selected_quarters=[items[i] for i in selected]
                )

                years = get_unique_values("year")

                await state.update_data(
                    years=years,
                    selected_years=[]
                )

                keyboard = build_multi_select_keyboard(years, [], "year")

                await state.set_state(ReportState.waiting_years)
                await callback.message.edit_text("📆 Годы:", reply_markup=keyboard)

            elif data.startswith("quarter_toggle_"):
                idx = int(data.split("_")[-1])
                if idx in selected:
                    selected.remove(idx)
                else:
                    selected.append(idx)

                await state.update_data(selected_quarters=selected)

                keyboard = build_multi_select_keyboard(items, selected, "quarter")
                await callback.message.edit_reply_markup(reply_markup=keyboard)

        # YEARS
        elif data.startswith("year_"):
            items = user_data["years"]
            selected = user_data.get("selected_years", [])

            if data == "year_done":
                if not selected:
                    await callback.answer("Выберите год", show_alert=True)
                    return

                await state.update_data(
                    selected_years=[items[i] for i in selected]
                )

                types = ["Авторские", "Смежные"]

                await state.update_data(
                    types=types,
                    selected_types=[]
                )

                keyboard = build_multi_select_keyboard(types, [], "type")

                await state.set_state(ReportState.waiting_types)
                await callback.message.edit_text("⚖️ Типы:", reply_markup=keyboard)

            elif data.startswith("year_toggle_"):
                idx = int(data.split("_")[-1])
                if idx in selected:
                    selected.remove(idx)
                else:
                    selected.append(idx)

                await state.update_data(selected_years=selected)

                keyboard = build_multi_select_keyboard(items, selected, "year")
                await callback.message.edit_reply_markup(reply_markup=keyboard)

    except Exception:
        logger.exception("callback error")

    await callback.answer()


# ---------- MAIN ----------
def run_flask():
    port = int(os.environ.get('PORT', 10000))
    flask_app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)


async def main():
    logger.info("Bot started")
    await dp.start_polling(bot, skip_updates=True)


if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()

    try:
        asyncio.run(main())
    except Exception:
        logger.exception("fatal error") 