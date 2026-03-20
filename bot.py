import os
import sqlite3
import asyncio
import threading
import logging
from flask import Flask
from aiogram import Bot, Dispatcher, types
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, FSInputFile
from aiogram.filters import Command
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
import openpyxl
from datetime import datetime

# ========== НАСТРОЙКИ (замените своими) ==========
API_TOKEN = "8556100624:AAEjTGUaj3P5xS0fTtlYRj5DeDw9j5pZdU8"   # ваш токен
ADMIN_ID = 491501244                                         # ваш Telegram ID
ALLOWED_USERS = [491501244,332160136,655546417]                                   # список разрешённых ID
# =================================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=API_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
flask_app = Flask(__name__)

@flask_app.route('/')
@flask_app.route('/health')
def health():
    return "Bot is running", 200

class ReportState(StatesGroup):
    waiting_contracts = State()
    waiting_quarters = State()
    waiting_years = State()
    waiting_types = State()
    waiting_songs = State()
    waiting_author_percent = State()
    waiting_related_percent = State()

def get_db():
    return sqlite3.connect('royalties.db')

def get_unique(column):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(f"SELECT DISTINCT {column} FROM royalties WHERE {column} IS NOT NULL AND {column} != '' ORDER BY {column}")
    vals = [row[0] for row in cur.fetchall()]
    conn.close()
    return vals

def get_songs():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT display_name FROM royalties ORDER BY display_name")
    songs = [row[0] for row in cur.fetchall()]
    conn.close()
    return songs

def build_keyboard(items, selected, prefix, page=0, per_page=10):
    keyboard = []
    start = page * per_page
    end = min(start + per_page, len(items))
    for i in range(start, end):
        item = str(items[i])
        emoji = "✅ " if item in selected else "⬜ "
        cb = f"{prefix}_toggle_{item.replace(' ', '_')}"
        keyboard.append([InlineKeyboardButton(text=f"{emoji}{item}", callback_data=cb)])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"{prefix}_page_{page-1}"))
    if end < len(items):
        nav.append(InlineKeyboardButton(text="Вперед ▶️", callback_data=f"{prefix}_page_{page+1}"))
    if nav:
        keyboard.append(nav)
    keyboard.append([InlineKeyboardButton(text="✅ Готово", callback_data=f"{prefix}_done")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

@dp.message(Command("start"))
async def start_cmd(message: Message):
    if message.from_user.id not in ALLOWED_USERS and message.from_user.id != ADMIN_ID:
        await message.answer("⛔ Доступ запрещён.")
        return
    await message.answer("📊 *Система расчёта роялти*\n\n/report — сформировать отчёт", parse_mode="Markdown")

@dp.message(Command("help"))
async def help_cmd(message: Message):
    await message.answer("/report — пошаговое формирование отчёта", parse_mode="Markdown")

@dp.message(Command("report"))
async def report_cmd(message: Message, state: FSMContext):
    if message.from_user.id not in ALLOWED_USERS and message.from_user.id != ADMIN_ID:
        await message.answer("⛔ Доступ запрещён.")
        return
    contracts = get_unique("contract")
    if not contracts:
        await message.answer("❌ База данных пуста. Загрузите данные через upload_data.py")
        return
    await state.update_data(
        selected_contracts=[],
        selected_quarters=[],
        selected_years=[],
        selected_types=[],
        selected_songs=[]
    )
    await state.set_state(ReportState.waiting_contracts)
    kb = build_keyboard(contracts, [], "contract")
    await message.answer("📋 *Выберите договоры* (можно несколько)", parse_mode="Markdown", reply_markup=kb)

@dp.callback_query()
async def handle_callback(cb: CallbackQuery, state: FSMContext):
    data = cb.data
    user_data = await state.get_data()

    # ----- ДОГОВОРЫ -----
    if data.startswith("contract_"):
        if data == "contract_done":
            sel = user_data.get("selected_contracts", [])
            if not sel:
                await cb.answer("Выберите хотя бы один договор!", show_alert=True)
                return
            await state.update_data(selected_quarters=[])
            await state.set_state(ReportState.waiting_quarters)
            kb = build_keyboard(["I", "II", "III", "IV"], [], "quarter")
            await cb.message.edit_text("📅 *Выберите кварталы*", parse_mode="Markdown", reply_markup=kb)
        elif data.startswith("contract_toggle_"):
            item = data.replace("contract_toggle_", "").replace("_", " ")
            sel = user_data.get("selected_contracts", [])
            if item in sel:
                sel.remove(item)
            else:
                sel.append(item)
            await state.update_data(selected_contracts=sel)
            contracts = get_unique("contract")
            page = user_data.get("contract_page", 0)
            kb = build_keyboard(contracts, sel, "contract", page)
            await cb.message.edit_reply_markup(reply_markup=kb)
        elif data.startswith("contract_page_"):
            page = int(data.split("_")[-1])
            await state.update_data(contract_page=page)
            contracts = get_unique("contract")
            sel = user_data.get("selected_contracts", [])
            kb = build_keyboard(contracts, sel, "contract", page)
            await cb.message.edit_reply_markup(reply_markup=kb)

    # ----- КВАРТАЛЫ -----
    elif data.startswith("quarter_"):
        if data == "quarter_done":
            sel = user_data.get("selected_quarters", [])
            if not sel:
                await cb.answer("Выберите хотя бы один квартал!", show_alert=True)
                return
            years = get_unique("year")
            await state.update_data(selected_years=[])
            await state.set_state(ReportState.waiting_years)
            kb = build_keyboard(years, [], "year")
            await cb.message.edit_text("📆 *Выберите годы*", parse_mode="Markdown", reply_markup=kb)
        elif data.startswith("quarter_toggle_"):
            item = data.replace("quarter_toggle_", "").replace("_", " ")
            sel = user_data.get("selected_quarters", [])
            if item in sel:
                sel.remove(item)
            else:
                sel.append(item)
            await state.update_data(selected_quarters=sel)
            quarters = ["I", "II", "III", "IV"]
            page = user_data.get("quarter_page", 0)
            kb = build_keyboard(quarters, sel, "quarter", page)
            await cb.message.edit_reply_markup(reply_markup=kb)
        elif data.startswith("quarter_page_"):
            page = int(data.split("_")[-1])
            await state.update_data(quarter_page=page)
            quarters = ["I", "II", "III", "IV"]
            sel = user_data.get("selected_quarters", [])
            kb = build_keyboard(quarters, sel, "quarter", page)
            await cb.message.edit_reply_markup(reply_markup=kb)

    # ----- ГОДЫ -----
    elif data.startswith("year_"):
        if data == "year_done":
            sel = user_data.get("selected_years", [])
            if not sel:
                await cb.answer("Выберите хотя бы один год!", show_alert=True)
                return
            types_ = ["Авторские", "Смежные"]
            await state.update_data(selected_types=[])
            await state.set_state(ReportState.waiting_types)
            kb = build_keyboard(types_, [], "type")
            await cb.message.edit_text("⚖️ *Выберите типы прав*", parse_mode="Markdown", reply_markup=kb)
        elif data.startswith("year_toggle_"):
            item = data.replace("year_toggle_", "").replace("_", " ")
            sel = user_data.get("selected_years", [])
            if item in sel:
                sel.remove(item)
            else:
                sel.append(item)
            await state.update_data(selected_years=sel)
            years = get_unique("year")
            page = user_data.get("year_page", 0)
            kb = build_keyboard(years, sel, "year", page)
            try:
                await cb.message.edit_reply_markup(reply_markup=kb)
            except Exception:
                pass  # если клавиатура не изменилась — игнорируем
        elif data.startswith("year_page_"):
            page = int(data.split("_")[-1])
            await state.update_data(year_page=page)
            years = get_unique("year")
            sel = user_data.get("selected_years", [])
            kb = build_keyboard(years, sel, "year", page)
            await cb.message.edit_reply_markup(reply_markup=kb)

    # ----- ТИПЫ ПРАВ -----
    elif data.startswith("type_"):
        if data == "type_done":
            sel = user_data.get("selected_types", [])
            if not sel:
                await cb.answer("Выберите хотя бы один тип!", show_alert=True)
                return
            if "Авторские" in sel and "Смежные" in sel:
                await state.set_state(ReportState.waiting_author_percent)
                await cb.message.edit_text("💰 *Укажите процент для АВТОРСКИХ прав* (например: 50)", parse_mode="Markdown")
            elif "Авторские" in sel:
                await state.update_data(author_percent=0, related_percent=0)
                await state.set_state(ReportState.waiting_songs)
                songs = get_songs()
                kb = build_keyboard(songs, [], "song")
                await cb.message.edit_text("🎵 *Выберите песни* (можно несколько, или нажмите Готово для всех)", parse_mode="Markdown", reply_markup=kb)
            else:  # только смежные
                await state.update_data(author_percent=0, related_percent=0)
                await state.set_state(ReportState.waiting_related_percent)
                await cb.message.edit_text("💰 *Укажите процент для СМЕЖНЫХ прав* (например: 30)", parse_mode="Markdown")
        elif data.startswith("type_toggle_"):
            item = data.replace("type_toggle_", "").replace("_", " ")
            sel = user_data.get("selected_types", [])
            if item in sel:
                sel.remove(item)
            else:
                sel.append(item)
            await state.update_data(selected_types=sel)
            types_ = ["Авторские", "Смежные"]
            page = user_data.get("type_page", 0)
            kb = build_keyboard(types_, sel, "type", page)
            await cb.message.edit_reply_markup(reply_markup=kb)

    # ----- ПЕСНИ -----
    elif data.startswith("song_"):
        if data == "song_done":
            await state.set_state(None)
            user_data = await state.get_data()
            await generate_report(cb.message, user_data)
        elif data.startswith("song_toggle_"):
            item = data.replace("song_toggle_", "").replace("_", " ")
            sel = user_data.get("selected_songs", [])
            if item in sel:
                sel.remove(item)
            else:
                sel.append(item)
            await state.update_data(selected_songs=sel)
            songs = get_songs()
            page = user_data.get("song_page", 0)
            kb = build_keyboard(songs, sel, "song", page)
            await cb.message.edit_reply_markup(reply_markup=kb)
        elif data.startswith("song_page_"):
            page = int(data.split("_")[-1])
            await state.update_data(song_page=page)
            songs = get_songs()
            sel = user_data.get("selected_songs", [])
            kb = build_keyboard(songs, sel, "song", page)
            await cb.message.edit_reply_markup(reply_markup=kb)

    await cb.answer()

# ----- ГЕНЕРАЦИЯ ОТЧЁТА -----
async def generate_report(message, user_data):
    await message.answer("📊 Формирую отчёт...")
    selected_contracts = user_data.get("selected_contracts", [])
    selected_quarters = user_data.get("selected_quarters", [])
    selected_years = user_data.get("selected_years", [])
    selected_types = user_data.get("selected_types", [])
    selected_songs = user_data.get("selected_songs", [])
    author_percent = user_data.get("author_percent", 0)
    related_percent = user_data.get("related_percent", 0)

    conn = get_db()
    cur = conn.cursor()
    query = "SELECT * FROM royalties WHERE 1=1"
    params = []
    if selected_contracts:
        query += f" AND contract IN ({','.join(['?']*len(selected_contracts))})"
        params.extend(selected_contracts)
    if selected_quarters:
        query += f" AND quarter IN ({','.join(['?']*len(selected_quarters))})"
        params.extend(selected_quarters)
    if selected_years:
        query += f" AND year IN ({','.join(['?']*len(selected_years))})"
        params.extend(selected_years)
    if selected_types:
        query += f" AND type IN ({','.join(['?']*len(selected_types))})"
        params.extend(selected_types)
    if selected_songs:
        query += f" AND display_name IN ({','.join(['?']*len(selected_songs))})"
        params.extend(selected_songs)

    cur.execute(query, params)
    rows = cur.fetchall()
    conn.close()

    if not rows:
        await message.answer("❌ Нет данных по выбранным фильтрам.")
        return

    TAX = 0.06
    author_rev = 0
    related_rev = 0
    for r in rows:
        if r[4] == "Авторские":  # type
            author_rev += r[9]   # sum
        else:
            related_rev += r[9]

    author_net = author_rev * (1 - TAX)
    related_net = related_rev * (1 - TAX)
    author_payout = author_net * (author_percent / 100) if author_percent else 0
    related_payout = related_net * (related_percent / 100) if related_percent else 0
    total_payout = author_payout + related_payout

    # Текстовый отчёт
    report_text = f"📊 *ОТЧЁТ ПО РОЯЛТИ*\n\n"
    report_text += f"📋 *Договоры:* {', '.join(selected_contracts)}\n"
    report_text += f"📅 *Кварталы:* {', '.join(selected_quarters)}\n"
    report_text += f"📆 *Годы:* {', '.join(map(str, selected_years))}\n"
    report_text += f"⚖️ *Типы прав:* {', '.join(selected_types)}\n\n"

    if "Авторские" in selected_types:
        report_text += f"💰 *АВТОРСКИЕ ПРАВА*" + (f" (процент: {author_percent}%)\n" if author_percent else "\n")
        report_text += f"  Общий доход: {author_rev:,.2f} ₽\n"
        report_text += f"  Налог (6%): {author_rev * TAX:,.2f} ₽\n"
        report_text += f"  Чистая выручка: {author_net:,.2f} ₽\n"
        if author_percent:
            report_text += f"  К выплате: {author_payout:,.2f} ₽\n"
        report_text += "\n"

    if "Смежные" in selected_types:
        report_text += f"🎵 *СМЕЖНЫЕ ПРАВА*" + (f" (процент: {related_percent}%)\n" if related_percent else "\n")
        report_text += f"  Общий доход: {related_rev:,.2f} ₽\n"
        report_text += f"  Налог (6%): {related_rev * TAX:,.2f} ₽\n"
        report_text += f"  Чистая выручка: {related_net:,.2f} ₽\n"
        if related_percent:
            report_text += f"  К выплате: {related_payout:,.2f} ₽\n"
        report_text += "\n"

    report_text += f"📌 *ИТОГО К ВЫПЛАТЕ:* {total_payout:,.2f} ₽\n\n"
    report_text += "📎 *Детализация в Excel-файле*"

    # Excel
    filename = f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Детализация"
    ws.append(["Тип", "Договор", "Квартал", "Год", "Название", "Авторы/Исполнители", "Сумма", "Налог", "Чистая выручка", "Процент блогера", "К выплате"])

    for r in rows:
        tax = r[9] * TAX
        net = r[9] - tax
        if r[4] == "Авторские":
            percent = author_percent
            payout = net * (percent / 100) if percent else 0
        else:
            percent = related_percent
            payout = net * (percent / 100) if percent else 0
        additional = r[10] if r[10] else ""  # additional_info
        ws.append([r[4], r[1], r[2], r[3], r[5], additional, r[9], tax, net, percent, payout])

    ws_sum = wb.create_sheet("Сводка")
    ws_sum.append(["Показатель", "Значение"])
    ws_sum.append(["Договоры", ", ".join(selected_contracts)])
    ws_sum.append(["Кварталы", ", ".join(selected_quarters)])
    ws_sum.append(["Годы", ", ".join(map(str, selected_years))])
    ws_sum.append(["Типы прав", ", ".join(selected_types)])
    ws_sum.append([])
    if "Авторские" in selected_types:
        ws_sum.append(["АВТОРСКИЕ ПРАВА", ""])
        ws_sum.append(["Общий доход", f"{author_rev:,.2f} ₽"])
        ws_sum.append(["Налог (6%)", f"{author_rev * TAX:,.2f} ₽"])
        ws_sum.append(["Чистая выручка", f"{author_net:,.2f} ₽"])
        ws_sum.append(["Процент блогера", f"{author_percent}%"])
        ws_sum.append(["К выплате", f"{author_payout:,.2f} ₽"])
        ws_sum.append([])
    if "Смежные" in selected_types:
        ws_sum.append(["СМЕЖНЫЕ ПРАВА", ""])
        ws_sum.append(["Общий доход", f"{related_rev:,.2f} ₽"])
        ws_sum.append(["Налог (6%)", f"{related_rev * TAX:,.2f} ₽"])
        ws_sum.append(["Чистая выручка", f"{related_net:,.2f} ₽"])
        ws_sum.append(["Процент блогера", f"{related_percent}%"])
        ws_sum.append(["К выплате", f"{related_payout:,.2f} ₽"])
        ws_sum.append([])
    ws_sum.append(["ИТОГО К ВЫПЛАТЕ", f"{total_payout:,.2f} ₽"])

    wb.save(filename)
    await message.answer(report_text, parse_mode="Markdown")
    await message.answer_document(FSInputFile(filename), caption="📎 Отчёт")
    os.remove(filename)

# ----- ОБРАБОТЧИКИ ПРОЦЕНТОВ -----
@dp.message(ReportState.waiting_author_percent)
async def process_author_percent(message: Message, state: FSMContext):
    try:
        percent = float(message.text.replace(",", "."))
        if not 0 <= percent <= 100:
            raise ValueError
        await state.update_data(author_percent=percent)
        selected_types = (await state.get_data()).get("selected_types", [])
        if "Смежные" in selected_types:
            await state.set_state(ReportState.waiting_related_percent)
            await message.answer("💰 *Укажите процент для СМЕЖНЫХ прав* (например: 30)", parse_mode="Markdown")
        else:
            await state.update_data(related_percent=0)
            await state.set_state(ReportState.waiting_songs)
            songs = get_songs()
            kb = build_keyboard(songs, [], "song")
            await message.answer("🎵 *Выберите песни* (можно несколько)", parse_mode="Markdown", reply_markup=kb)
    except ValueError:
        await message.answer("❌ Введите число от 0 до 100")

@dp.message(ReportState.waiting_related_percent)
async def process_related_percent(message: Message, state: FSMContext):
    try:
        percent = float(message.text.replace(",", "."))
        if not 0 <= percent <= 100:
            raise ValueError
        await state.update_data(related_percent=percent)
        await state.set_state(ReportState.waiting_songs)
        songs = get_songs()
        kb = build_keyboard(songs, [], "song")
        await message.answer("🎵 *Выберите песни* (можно несколько)", parse_mode="Markdown", reply_markup=kb)
    except ValueError:
        await message.answer("❌ Введите число от 0 до 100")

# ----- ЗАПУСК -----
def run_flask():
    port = int(os.environ.get('PORT', 10000))
    flask_app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

async def main():
    logger.info("Starting bot...")
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    logger.info("Initializing...")
    threading.Thread(target=run_flask, daemon=True).start()
    asyncio.run(main())