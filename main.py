import asyncio
import calendar
import os
import re
import secrets
import sqlite3
from contextvars import ContextVar
from datetime import datetime, timedelta
from html import escape
from zoneinfo import ZoneInfo

from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    KeyboardButton,
    LabeledPrice,
    Message,
    PreCheckoutQuery,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    WebAppInfo,
)
from apscheduler.jobstores.base import JobLookupError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv


# =====================================================
# 1) Конфигурация и глобальные константы
# =====================================================
load_dotenv()
BOT_TOKEN = (os.getenv("BOT_TOKEN") or "").strip().strip("\"'")
ADMIN_ID = (os.getenv("ADMIN_ID") or "").strip().strip("\"'")
DEVELOPER_ID = (os.getenv("DEVELOPER_ID") or "").strip().strip("\"'")
STAFF_ID = (os.getenv("STAFF_ID") or "").strip().strip("\"'")
OWNER_ID = (os.getenv("OWNER_ID") or "").strip().strip("\"'")
MANAGER_IDS = (os.getenv("MANAGER_IDS") or "").strip().strip("\"'")
PAYMENT_PROVIDER_TOKEN = (os.getenv("PAYMENT_PROVIDER_TOKEN") or "").strip().strip("\"'")
PAYMENT_CURRENCY = (os.getenv("PAYMENT_CURRENCY") or "USD").strip().upper().strip("\"'")
MINI_APP_URL = (os.getenv("MINI_APP_URL") or "").strip().strip("\"'")

if not BOT_TOKEN:
    raise RuntimeError("Не найден BOT_TOKEN в .env")
if BOT_TOKEN == "your_bot_token_here" or ":" not in BOT_TOKEN:
    raise RuntimeError(
        "BOT_TOKEN в .env некорректен. Укажите реальный токен от @BotFather в формате 123456:ABC..."
    )
if not ADMIN_ID:
    raise RuntimeError("Не найден ADMIN_ID в .env")

try:
    ADMIN_ID_INT = int(ADMIN_ID)
except ValueError as exc:
    raise RuntimeError("ADMIN_ID должен быть целым числом") from exc

if DEVELOPER_ID:
    try:
        DEVELOPER_ID_INT = int(DEVELOPER_ID)
    except ValueError as exc:
        raise RuntimeError("DEVELOPER_ID должен быть целым числом") from exc
else:
    # Обратная совместимость: если DEVELOPER_ID не задан, разработчиком считается ADMIN_ID.
    DEVELOPER_ID_INT = ADMIN_ID_INT

if STAFF_ID:
    try:
        STAFF_ID_INT = int(STAFF_ID)
    except ValueError as exc:
        raise RuntimeError("STAFF_ID должен быть целым числом") from exc
else:
    STAFF_ID_INT = None

if OWNER_ID:
    try:
        OWNER_ID_INT = int(OWNER_ID)
    except ValueError as exc:
        raise RuntimeError("OWNER_ID должен быть целым числом") from exc
else:
    OWNER_ID_INT = DEVELOPER_ID_INT


def parse_manager_ids(raw: str) -> set[int]:
    result: set[int] = set()
    if not raw:
        return result
    for part in raw.split(","):
        token = part.strip()
        if not token:
            continue
        if not token.isdigit():
            raise RuntimeError("MANAGER_IDS должен содержать только числа через запятую")
        result.add(int(token))
    return result


MANAGER_IDS_SET = parse_manager_ids(MANAGER_IDS)

DB_PATH = (os.getenv("DB_PATH") or "bot.db").strip().strip("\"'")
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
LOCAL_TZ = ZoneInfo("Europe/Minsk")
ALL_TIME_SLOTS = ("09:00", "12:00", "15:00", "18:00")
SYSTEM_DEMO_OWNER_ID = 0
CURRENT_DEMO_OWNER_ID: ContextVar[int] = ContextVar("current_demo_owner_id", default=SYSTEM_DEMO_OWNER_ID)
DEFAULT_DEMO_REFERENCE_IMAGE = os.path.join("assets", "demo_reference.jpg")
DEFAULT_MASTER_NAME = "Основной мастер"
DEMO_MASTER_TEMPLATES = ("Анна", "Мария", "София")
DEFAULT_MASTER_SPECIALIZATION = "Бьюти-мастер"
DEFAULT_MASTER_DESCRIPTION = "Специалист, к которому клиенты могут удобно записаться через этого бота."
DEMO_MASTER_PROFILE_TEMPLATES = {
    "Анна": {
        "specialization": "Экспресс-услуги",
        "description": "Помогает быстро привести образ в порядок и подобрать удобное время без лишней переписки.",
    },
    "Мария": {
        "specialization": "Комплексный уход",
        "description": "Подходит клиентам, которым важны комфорт, аккуратный сервис и понятная запись на несколько шагов вперед.",
    },
    "София": {
        "specialization": "Премиум-процедуры",
        "description": "Для клиентов, которые ценят персональный подход, красивую подачу и уверенное сопровождение записи.",
    },
}
WORKSPACE_MODE_SOLO = "solo"
WORKSPACE_MODE_TEAM = "team"
WORKSPACE_SETTING_BOOKING_MODE = "booking_mode"

SALE_TARIFFS = {
    "lite": {
        "title": "Solo Start",
        "description": "Для одного мастера: готовая основа, запись, услуги, портфолио и спокойный старт без лишней сложности.",
        "price_minor": 3900,
    },
    "standard": {
        "title": "Solo Pro",
        "description": "Для одного мастера: более полный запуск, больше наполнения и меньше ручной подготовки с вашей стороны.",
        "price_minor": 4900,
    },
    "pro": {
        "title": "Team",
        "description": "Для команды специалистов: выбор мастера, раздельные услуги, портфолио и настройка под командный формат.",
        "price_minor": 7900,
    },
}

SALE_OPTIONS = {
    "extra_revision": {
        "title": "Доп. раунд правок",
        "description": "Если после запуска захотите спокойно внести ещё один блок изменений.",
        "price_minor": 700,
    },
    "photos_10": {
        "title": "+10 фото в портфолио",
        "description": "Подходит, если хотите показать больше работ уже на старте.",
        "price_minor": 500,
    },
    "data_migration": {
        "title": "Перенос данных",
        "description": "Перенесём основной контент из старого бота или текущей базы в новый запуск.",
        "price_minor": 1500,
    },
    "extra_language": {
        "title": "Второй язык интерфейса",
        "description": "Если бот нужен сразу на двух языках.",
        "price_minor": 2000,
    },
}

DEMO_FEATURES = {
    "today": {
        "title": "📅 Записи на сегодня",
        "description": (
            "Показывает все записи на текущую дату: имя клиента, телефон, услугу и время.\n"
            "Удобно для быстрого старта рабочего дня."
        ),
        "open_callback": "admin:today",
    },
    "all": {
        "title": "🗓 Все записи",
        "description": (
            "Сводный список будущих записей по времени.\n"
            "Помогает контролировать загрузку и планировать окно между клиентами."
        ),
        "open_callback": "admin:all",
    },
    "services": {
        "title": "🛠 Услуги и цены",
        "description": (
            "Полное управление услугами: добавить, изменить цену, скрыть или удалить позицию.\n"
            "Изменения сразу применяются в записи клиента."
        ),
        "open_callback": "admin:services",
    },
    "portfolio": {
        "title": "🖼 Портфолио",
        "description": (
            "Редактирование категорий и фото без клавиатуры: включение, скрытие и удаление.\n"
            "Клиент видит актуальные работы прямо в боте."
        ),
        "open_callback": "admin:portfolio",
    },
}

SERVICES = {
    "basic": "Базовая процедура",
    "express": "Экспресс-услуга",
    "premium": "Премиум-услуга",
}

SERVICE_PRICES = {
    "Базовая процедура": 40,
    "Экспресс-услуга": 55,
    "Премиум-услуга": 70,
}

SERVICE_PRICES_BY_CODE = {
    "basic": 40,
    "express": 55,
    "premium": 70,
}

PORTFOLIO = {
    "before_after": {
        "title": "До / После",
        "urls": [
            DEFAULT_DEMO_REFERENCE_IMAGE,
            DEFAULT_DEMO_REFERENCE_IMAGE,
            DEFAULT_DEMO_REFERENCE_IMAGE,
        ],
    },
    "natural": {
        "title": "Натуральный образ",
        "urls": [
            DEFAULT_DEMO_REFERENCE_IMAGE,
            DEFAULT_DEMO_REFERENCE_IMAGE,
            DEFAULT_DEMO_REFERENCE_IMAGE,
        ],
    },
    "signature": {
        "title": "Авторские работы",
        "urls": [
            DEFAULT_DEMO_REFERENCE_IMAGE,
            DEFAULT_DEMO_REFERENCE_IMAGE,
            DEFAULT_DEMO_REFERENCE_IMAGE,
        ],
    },
}

LEGACY_SERVICE_RENAMES = {
    "Классический маникюр": "Базовая процедура",
    "Покрытие гель-лаком": "Экспресс-услуга",
    "Маникюр с дизайном": "Премиум-услуга",
}

LEGACY_PORTFOLIO_RENAMES = {
    "french": ("before_after", "До / После"),
    "design": ("natural", "Натуральный образ"),
    "solid": ("signature", "Авторские работы"),
}

MONTH_NAMES_RU = {
    1: "Январь",
    2: "Февраль",
    3: "Март",
    4: "Апрель",
    5: "Май",
    6: "Июнь",
    7: "Июль",
    8: "Август",
    9: "Сентябрь",
    10: "Октябрь",
    11: "Ноябрь",
    12: "Декабрь",
}

MONTH_NAMES_RU_GEN = {
    1: "января",
    2: "февраля",
    3: "марта",
    4: "апреля",
    5: "мая",
    6: "июня",
    7: "июля",
    8: "августа",
    9: "сентября",
    10: "октября",
    11: "ноября",
    12: "декабря",
}


# =====================================================
# 2) ????????????? aiogram ? ????????????
# =====================================================
router = Router()
dp = Dispatcher()
dp.include_router(router)

scheduler = AsyncIOScheduler(timezone=LOCAL_TZ)
bot_instance: Bot | None = None
LAST_SCREEN_MESSAGE_IDS: dict[int, list[int]] = {}
CURRENT_SCREEN: dict[int, tuple[str, object | None]] = {}
SCREEN_HISTORY: dict[int, list[tuple[str, object | None]]] = {}
AUX_MESSAGE_IDS: dict[int, list[int]] = {}
PORTFOLIO_PREVIEW_MESSAGE_IDS: dict[int, int] = {}
REPLY_KEYBOARD_ACTIVE: dict[int, bool] = {}
INITIALIZED_WORKSPACES: set[int] = set()
# =====================================================
# 3) FSM состояния
# =====================================================
class BookingFSM(StatesGroup):
    choosing_master = State()
    choosing_service = State()
    choosing_date = State()
    choosing_time = State()
    waiting_phone = State()
    waiting_client_name = State()
    waiting_comment = State()
    waiting_source = State()
    confirming = State()


class AdminFSM(StatesGroup):
    waiting_broadcast_message = State()
    waiting_broadcast_confirm = State()
    waiting_slot_date = State()
    waiting_slot_custom_time = State()
    waiting_new_master_name = State()
    waiting_edit_master_name = State()
    waiting_edit_master_specialization = State()
    waiting_edit_master_description = State()
    waiting_edit_master_photo = State()
    waiting_new_service_name = State()
    waiting_new_service_price = State()
    waiting_edit_service_name = State()
    waiting_edit_service_price = State()
    waiting_portfolio_add_url = State()
    waiting_edit_portfolio_category_title = State()
    waiting_new_portfolio_category_title = State()


class DeveloperFSM(StatesGroup):
    waiting_new_admin_id = State()


# =====================================================
# 4) Вспомогательные функции БД (SQLite3)
# =====================================================
def get_connection() -> sqlite3.Connection:
    db_dir = os.path.dirname(os.path.abspath(DB_PATH))
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_default_master_profile(name: str, index: int = 0) -> tuple[str, str]:
    template = DEMO_MASTER_PROFILE_TEMPLATES.get(name)
    if template is not None:
        return str(template["specialization"]), str(template["description"])
    if name == DEFAULT_MASTER_NAME:
        return DEFAULT_MASTER_SPECIALIZATION, DEFAULT_MASTER_DESCRIPTION
    if index < len(DEMO_MASTER_TEMPLATES):
        fallback_template = DEMO_MASTER_PROFILE_TEMPLATES.get(DEMO_MASTER_TEMPLATES[index])
        if fallback_template is not None:
            return str(fallback_template["specialization"]), str(fallback_template["description"])
    return DEFAULT_MASTER_SPECIALIZATION, DEFAULT_MASTER_DESCRIPTION


def migrate_legacy_demo_seed_data(conn: sqlite3.Connection) -> None:
    owner_ids: set[int] = {SYSTEM_DEMO_OWNER_ID}

    for table_name in ("services", "portfolio_categories", "portfolio_items", "appointments"):
        rows = conn.execute(
            f"SELECT DISTINCT demo_owner_id FROM {table_name}"
        ).fetchall()
        owner_ids.update(int(row["demo_owner_id"]) for row in rows)

    for owner_id in sorted(owner_ids):
        service_rows = conn.execute(
            """
            SELECT id, name
            FROM services
            WHERE demo_owner_id = ?
            ORDER BY sort_order, id
            """,
            (owner_id,),
        ).fetchall()
        service_names = {str(row["name"]) for row in service_rows}
        if service_names == set(LEGACY_SERVICE_RENAMES):
            for row in service_rows:
                old_name = str(row["name"])
                conn.execute(
                    "UPDATE services SET name = ? WHERE id = ?",
                    (LEGACY_SERVICE_RENAMES[old_name], int(row["id"])),
                )
                conn.execute(
                    """
                    UPDATE appointments
                    SET service = ?
                    WHERE demo_owner_id = ? AND service = ?
                    """,
                    (LEGACY_SERVICE_RENAMES[old_name], owner_id, old_name),
                )

        category_rows = conn.execute(
            """
            SELECT id, code, title
            FROM portfolio_categories
            WHERE demo_owner_id = ?
            ORDER BY sort_order, id
            """,
            (owner_id,),
        ).fetchall()
        category_codes = {str(row["code"]) for row in category_rows}
        if category_codes == set(LEGACY_PORTFOLIO_RENAMES):
            for row in category_rows:
                old_code = str(row["code"])
                new_code, new_title = LEGACY_PORTFOLIO_RENAMES[old_code]
                conn.execute(
                    """
                    UPDATE portfolio_categories
                    SET code = ?, title = ?
                    WHERE id = ?
                    """,
                    (new_code, new_title, int(row["id"])),
                )
                conn.execute(
                    """
                    UPDATE portfolio_items
                    SET category = ?
                    WHERE demo_owner_id = ? AND category = ?
                    """,
                    (new_code, owner_id, old_code),
                )

def init_db() -> None:
    with get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER UNIQUE NOT NULL,
                full_name TEXT NOT NULL,
                username TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS workspace_users (
                demo_owner_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                full_name TEXT NOT NULL,
                username TEXT,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                PRIMARY KEY (demo_owner_id, user_id)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_workspace_users_owner_last_seen
            ON workspace_users (demo_owner_id, last_seen_at DESC, user_id)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS workspace_settings (
                demo_owner_id INTEGER NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY (demo_owner_id, key)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_workspace_settings_owner_key
            ON workspace_settings (demo_owner_id, key)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS masters (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                demo_owner_id INTEGER NOT NULL DEFAULT 0,
                name TEXT NOT NULL,
                specialization TEXT NOT NULL DEFAULT '',
                description TEXT NOT NULL DEFAULT '',
                photo TEXT,
                is_active INTEGER NOT NULL DEFAULT 1,
                sort_order INTEGER NOT NULL DEFAULT 0,
                UNIQUE(demo_owner_id, name)
            )
            """
        )
        master_columns = {row["name"] for row in conn.execute("PRAGMA table_info(masters)").fetchall()}
        if "demo_owner_id" not in master_columns:
            old_master_rows = conn.execute("SELECT * FROM masters").fetchall()
            conn.execute(
                """
                CREATE TABLE masters_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    demo_owner_id INTEGER NOT NULL DEFAULT 0,
                    name TEXT NOT NULL,
                    specialization TEXT NOT NULL DEFAULT '',
                    description TEXT NOT NULL DEFAULT '',
                    photo TEXT,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    sort_order INTEGER NOT NULL DEFAULT 0,
                    UNIQUE(demo_owner_id, name)
                )
                """
            )
            conn.executemany(
                """
                INSERT INTO masters_new (id, demo_owner_id, name, specialization, description, photo, is_active, sort_order)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        int(row["id"]),
                        0,
                        str(row["name"]),
                        DEFAULT_MASTER_SPECIALIZATION,
                        DEFAULT_MASTER_DESCRIPTION,
                        None,
                        int(row["is_active"]),
                        int(row["sort_order"]),
                    )
                    for row in old_master_rows
                ],
            )
            conn.execute("DROP TABLE masters")
            conn.execute("ALTER TABLE masters_new RENAME TO masters")
        master_columns = {row["name"] for row in conn.execute("PRAGMA table_info(masters)").fetchall()}
        if "specialization" not in master_columns:
            conn.execute("ALTER TABLE masters ADD COLUMN specialization TEXT NOT NULL DEFAULT ''")
        if "description" not in master_columns:
            conn.execute("ALTER TABLE masters ADD COLUMN description TEXT NOT NULL DEFAULT ''")
        if "photo" not in master_columns:
            conn.execute("ALTER TABLE masters ADD COLUMN photo TEXT")
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_masters_owner_sort
            ON masters (demo_owner_id, sort_order, id)
            """
        )

        owner_candidates: set[int] = {SYSTEM_DEMO_OWNER_ID}
        for owner_table in (
            "services",
            "portfolio_categories",
            "portfolio_items",
            "appointments",
            "workspace_users",
            "closed_dates",
            "slot_overrides",
        ):
            try:
                rows = conn.execute(
                    f"SELECT DISTINCT demo_owner_id FROM {owner_table}"
                ).fetchall()
            except sqlite3.OperationalError:
                continue
            for row in rows:
                owner_candidates.add(int(row["demo_owner_id"]))

        for owner_id in sorted(owner_candidates):
            exists = conn.execute(
                "SELECT 1 FROM masters WHERE demo_owner_id = ? LIMIT 1",
                (owner_id,),
            ).fetchone()
            if exists is not None:
                continue

            if owner_id == SYSTEM_DEMO_OWNER_ID:
                seed_rows = [
                    (
                        owner_id,
                        name,
                        get_default_master_profile(name, index)[0],
                        get_default_master_profile(name, index)[1],
                        None,
                        1,
                        index,
                    )
                    for index, name in enumerate(DEMO_MASTER_TEMPLATES)
                ]
            else:
                template_rows = conn.execute(
                    """
                    SELECT name, specialization, description, photo, is_active, sort_order
                    FROM masters
                    WHERE demo_owner_id = ?
                    ORDER BY sort_order, id
                    """,
                    (SYSTEM_DEMO_OWNER_ID,),
                ).fetchall()
                if template_rows:
                    seed_rows = [
                        (
                            owner_id,
                            str(row["name"]),
                            str(row["specialization"] or ""),
                            str(row["description"] or ""),
                            row["photo"],
                            int(row["is_active"]),
                            int(row["sort_order"]),
                        )
                        for row in template_rows
                    ]
                else:
                    specialization, description = get_default_master_profile(DEFAULT_MASTER_NAME)
                    seed_rows = [(owner_id, DEFAULT_MASTER_NAME, specialization, description, None, 1, 0)]

            conn.executemany(
                """
                INSERT INTO masters (demo_owner_id, name, specialization, description, photo, is_active, sort_order)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                seed_rows,
            )

            existing_masters = conn.execute(
                """
                SELECT id, name, specialization, description
                FROM masters
                WHERE demo_owner_id = ?
                ORDER BY sort_order, id
                """,
                (owner_id,),
            ).fetchall()
            for idx, row in enumerate(existing_masters):
                default_specialization, default_description = get_default_master_profile(str(row["name"]), idx)
                specialization = (row["specialization"] or "").strip()
                description = (row["description"] or "").strip()
                if not specialization or not description:
                    conn.execute(
                        """
                        UPDATE masters
                        SET specialization = ?, description = ?
                        WHERE id = ? AND demo_owner_id = ?
                        """,
                        (
                            specialization or default_specialization,
                            description or default_description,
                            int(row["id"]),
                            owner_id,
                        ),
                    )

        for owner_id in sorted(owner_candidates):
            existing_masters = conn.execute(
                """
                SELECT id, name, specialization, description
                FROM masters
                WHERE demo_owner_id = ?
                ORDER BY sort_order, id
                """,
                (owner_id,),
            ).fetchall()
            for idx, row in enumerate(existing_masters):
                default_specialization, default_description = get_default_master_profile(str(row["name"]), idx)
                specialization = (row["specialization"] or "").strip()
                description = (row["description"] or "").strip()
                if not specialization or not description:
                    conn.execute(
                        """
                        UPDATE masters
                        SET specialization = ?, description = ?
                        WHERE id = ? AND demo_owner_id = ?
                        """,
                        (
                            specialization or default_specialization,
                            description or default_description,
                            int(row["id"]),
                            owner_id,
                        ),
                    )

        for owner_id in sorted(owner_candidates):
            mode_row = conn.execute(
                """
                SELECT value
                FROM workspace_settings
                WHERE demo_owner_id = ? AND key = ?
                """,
                (owner_id, WORKSPACE_SETTING_BOOKING_MODE),
            ).fetchone()
            if mode_row is not None:
                continue
            active_count = conn.execute(
                "SELECT COUNT(*) AS cnt FROM masters WHERE demo_owner_id = ? AND is_active = 1",
                (owner_id,),
            ).fetchone()
            default_mode = (
                WORKSPACE_MODE_TEAM
                if active_count is not None and int(active_count["cnt"]) > 1
                else WORKSPACE_MODE_SOLO
            )
            conn.execute(
                """
                INSERT INTO workspace_settings (demo_owner_id, key, value)
                VALUES (?, ?, ?)
                """,
                (owner_id, WORKSPACE_SETTING_BOOKING_MODE, default_mode),
            )

        default_master_by_owner: dict[int, tuple[int, str]] = {}
        for owner_id in sorted(owner_candidates):
            row = conn.execute(
                """
                SELECT id, name
                FROM masters
                WHERE demo_owner_id = ?
                ORDER BY sort_order, id
                LIMIT 1
                """,
                (owner_id,),
            ).fetchone()
            if row is not None:
                default_master_by_owner[owner_id] = (int(row["id"]), str(row["name"]))

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS appointments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                demo_owner_id INTEGER NOT NULL DEFAULT 0,
                master_id INTEGER NOT NULL,
                master_name TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                client_name TEXT NOT NULL,
                phone TEXT NOT NULL,
                service TEXT NOT NULL,
                client_comment TEXT,
                source_file_id TEXT,
                source_file_type TEXT,
                source_file_name TEXT,
                appointment_date TEXT NOT NULL,
                appointment_time TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(demo_owner_id, master_id, appointment_date, appointment_time)
            )
            """
        )
        try:
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_appointments_owner_date_time
                ON appointments (demo_owner_id, appointment_date, appointment_time, master_id)
                """
            )
        except sqlite3.OperationalError:
            pass
        appointment_columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(appointments)").fetchall()
        }
        if "client_comment" not in appointment_columns:
            conn.execute("ALTER TABLE appointments ADD COLUMN client_comment TEXT")
        if "source_file_id" not in appointment_columns:
            conn.execute("ALTER TABLE appointments ADD COLUMN source_file_id TEXT")
        if "source_file_type" not in appointment_columns:
            conn.execute("ALTER TABLE appointments ADD COLUMN source_file_type TEXT")
        if "source_file_name" not in appointment_columns:
            conn.execute("ALTER TABLE appointments ADD COLUMN source_file_name TEXT")
        appointment_columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(appointments)").fetchall()
        }
        if (
            "demo_owner_id" not in appointment_columns
            or "master_id" not in appointment_columns
            or "master_name" not in appointment_columns
        ):
            old_appointment_rows = conn.execute("SELECT * FROM appointments").fetchall()
            conn.execute(
                """
                CREATE TABLE appointments_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    demo_owner_id INTEGER NOT NULL DEFAULT 0,
                    master_id INTEGER NOT NULL,
                    master_name TEXT NOT NULL,
                    user_id INTEGER NOT NULL,
                    client_name TEXT NOT NULL,
                    phone TEXT NOT NULL,
                    service TEXT NOT NULL,
                    client_comment TEXT,
                    source_file_id TEXT,
                    source_file_type TEXT,
                    source_file_name TEXT,
                    appointment_date TEXT NOT NULL,
                    appointment_time TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(demo_owner_id, master_id, appointment_date, appointment_time)
                )
                """
            )
            migrated_rows: list[tuple[object, ...]] = []
            for row in old_appointment_rows:
                owner_id = int(row["demo_owner_id"]) if "demo_owner_id" in appointment_columns else SYSTEM_DEMO_OWNER_ID
                master_id, master_name = default_master_by_owner.get(
                    owner_id,
                    (0, DEFAULT_MASTER_NAME),
                )
                if "master_id" in appointment_columns and row["master_id"]:
                    master_id = int(row["master_id"])
                if "master_name" in appointment_columns and row["master_name"]:
                    master_name = str(row["master_name"])
                migrated_rows.append(
                    (
                        int(row["id"]),
                        owner_id,
                        master_id,
                        master_name,
                        int(row["user_id"]),
                        str(row["client_name"]),
                        str(row["phone"]),
                        str(row["service"]),
                        row["client_comment"] if "client_comment" in appointment_columns else None,
                        row["source_file_id"] if "source_file_id" in appointment_columns else None,
                        row["source_file_type"] if "source_file_type" in appointment_columns else None,
                        row["source_file_name"] if "source_file_name" in appointment_columns else None,
                        str(row["appointment_date"]),
                        str(row["appointment_time"]),
                        str(row["created_at"]),
                    )
                )
            conn.executemany(
                """
                INSERT INTO appointments_new (
                    id, demo_owner_id, master_id, master_name, user_id, client_name, phone, service,
                    client_comment, source_file_id, source_file_type, source_file_name,
                    appointment_date, appointment_time, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                migrated_rows,
            )
            conn.execute("DROP TABLE appointments")
            conn.execute("ALTER TABLE appointments_new RENAME TO appointments")
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_appointments_owner_date_time
                ON appointments (demo_owner_id, appointment_date, appointment_time, master_id)
                """
            )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS services (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                demo_owner_id INTEGER NOT NULL DEFAULT 0,
                master_id INTEGER NOT NULL DEFAULT 0,
                name TEXT NOT NULL,
                price INTEGER NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                sort_order INTEGER NOT NULL DEFAULT 0,
                UNIQUE(demo_owner_id, master_id, name)
            )
            """
        )
        services_columns = {row["name"] for row in conn.execute("PRAGMA table_info(services)").fetchall()}
        if "demo_owner_id" not in services_columns or "master_id" not in services_columns:
            old_service_rows = conn.execute("SELECT * FROM services").fetchall()
            conn.execute(
                """
                CREATE TABLE services_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    demo_owner_id INTEGER NOT NULL DEFAULT 0,
                    master_id INTEGER NOT NULL DEFAULT 0,
                    name TEXT NOT NULL,
                    price INTEGER NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    sort_order INTEGER NOT NULL DEFAULT 0,
                    UNIQUE(demo_owner_id, master_id, name)
                )
                """
            )
            migrated_service_rows: list[tuple[object, ...]] = []
            next_service_id = 1
            for row in old_service_rows:
                owner_id = int(row["demo_owner_id"]) if "demo_owner_id" in services_columns else SYSTEM_DEMO_OWNER_ID
                if "master_id" in services_columns and row["master_id"]:
                    migrated_service_rows.append(
                        (
                            int(row["id"]),
                            owner_id,
                            int(row["master_id"]),
                            str(row["name"]),
                            int(row["price"]),
                            int(row["is_active"]),
                            int(row["sort_order"]),
                        )
                    )
                    next_service_id = max(next_service_id, int(row["id"]) + 1)
                    continue

                owner_masters = conn.execute(
                    """
                    SELECT id
                    FROM masters
                    WHERE demo_owner_id = ?
                    ORDER BY sort_order, id
                    """,
                    (owner_id,),
                ).fetchall()
                if not owner_masters:
                    default_master_id = default_master_by_owner.get(owner_id, (0, DEFAULT_MASTER_NAME))[0]
                    owner_masters = [{"id": default_master_id}]

                for master_row in owner_masters:
                    migrated_service_rows.append(
                        (
                            next_service_id,
                            owner_id,
                            int(master_row["id"]),
                            str(row["name"]),
                            int(row["price"]),
                            int(row["is_active"]),
                            int(row["sort_order"]),
                        )
                    )
                    next_service_id += 1

            conn.executemany(
                """
                INSERT INTO services_new (id, demo_owner_id, master_id, name, price, is_active, sort_order)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                migrated_service_rows,
            )
            conn.execute("DROP TABLE services")
            conn.execute("ALTER TABLE services_new RENAME TO services")
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_services_owner_sort
            ON services (demo_owner_id, master_id, sort_order, id)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS portfolio_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                demo_owner_id INTEGER NOT NULL DEFAULT 0,
                master_id INTEGER NOT NULL DEFAULT 0,
                category TEXT NOT NULL,
                url TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                sort_order INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        portfolio_item_columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(portfolio_items)").fetchall()
        }
        if "demo_owner_id" not in portfolio_item_columns or "master_id" not in portfolio_item_columns:
            old_portfolio_item_rows = conn.execute("SELECT * FROM portfolio_items").fetchall()
            conn.execute(
                """
                CREATE TABLE portfolio_items_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    demo_owner_id INTEGER NOT NULL DEFAULT 0,
                    master_id INTEGER NOT NULL DEFAULT 0,
                    category TEXT NOT NULL,
                    url TEXT NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    sort_order INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            migrated_item_rows: list[tuple[object, ...]] = []
            next_item_id = 1
            for row in old_portfolio_item_rows:
                owner_id = int(row["demo_owner_id"]) if "demo_owner_id" in portfolio_item_columns else SYSTEM_DEMO_OWNER_ID
                if "master_id" in portfolio_item_columns and row["master_id"]:
                    migrated_item_rows.append(
                        (
                            int(row["id"]),
                            owner_id,
                            int(row["master_id"]),
                            str(row["category"]),
                            str(row["url"]),
                            int(row["is_active"]),
                            int(row["sort_order"]),
                        )
                    )
                    next_item_id = max(next_item_id, int(row["id"]) + 1)
                    continue
                owner_masters = conn.execute(
                    """
                    SELECT id
                    FROM masters
                    WHERE demo_owner_id = ?
                    ORDER BY sort_order, id
                    """,
                    (owner_id,),
                ).fetchall()
                if not owner_masters:
                    default_master_id = default_master_by_owner.get(owner_id, (0, DEFAULT_MASTER_NAME))[0]
                    owner_masters = [{"id": default_master_id}]
                for master_row in owner_masters:
                    migrated_item_rows.append(
                        (
                            next_item_id,
                            owner_id,
                            int(master_row["id"]),
                            str(row["category"]),
                            str(row["url"]),
                            int(row["is_active"]),
                            int(row["sort_order"]),
                        )
                    )
                    next_item_id += 1
            conn.executemany(
                """
                INSERT INTO portfolio_items_new (id, demo_owner_id, master_id, category, url, is_active, sort_order)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                migrated_item_rows,
            )
            conn.execute("DROP TABLE portfolio_items")
            conn.execute("ALTER TABLE portfolio_items_new RENAME TO portfolio_items")
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_portfolio_items_owner_category_sort
            ON portfolio_items (demo_owner_id, master_id, category, sort_order, id)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS portfolio_categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                demo_owner_id INTEGER NOT NULL DEFAULT 0,
                master_id INTEGER NOT NULL DEFAULT 0,
                code TEXT NOT NULL,
                title TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                sort_order INTEGER NOT NULL DEFAULT 0,
                UNIQUE(demo_owner_id, master_id, code)
            )
            """
        )
        portfolio_category_columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(portfolio_categories)").fetchall()
        }
        if "demo_owner_id" not in portfolio_category_columns or "master_id" not in portfolio_category_columns:
            old_category_rows = conn.execute("SELECT * FROM portfolio_categories").fetchall()
            conn.execute(
                """
                CREATE TABLE portfolio_categories_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    demo_owner_id INTEGER NOT NULL DEFAULT 0,
                    master_id INTEGER NOT NULL DEFAULT 0,
                    code TEXT NOT NULL,
                    title TEXT NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    sort_order INTEGER NOT NULL DEFAULT 0,
                    UNIQUE(demo_owner_id, master_id, code)
                )
                """
            )
            migrated_category_rows: list[tuple[object, ...]] = []
            next_category_id = 1
            for row in old_category_rows:
                owner_id = int(row["demo_owner_id"]) if "demo_owner_id" in portfolio_category_columns else SYSTEM_DEMO_OWNER_ID
                if "master_id" in portfolio_category_columns and row["master_id"]:
                    migrated_category_rows.append(
                        (
                            int(row["id"]),
                            owner_id,
                            int(row["master_id"]),
                            str(row["code"]),
                            str(row["title"]),
                            int(row["is_active"]),
                            int(row["sort_order"]),
                        )
                    )
                    next_category_id = max(next_category_id, int(row["id"]) + 1)
                    continue
                owner_masters = conn.execute(
                    """
                    SELECT id
                    FROM masters
                    WHERE demo_owner_id = ?
                    ORDER BY sort_order, id
                    """,
                    (owner_id,),
                ).fetchall()
                if not owner_masters:
                    default_master_id = default_master_by_owner.get(owner_id, (0, DEFAULT_MASTER_NAME))[0]
                    owner_masters = [{"id": default_master_id}]
                for master_row in owner_masters:
                    migrated_category_rows.append(
                        (
                            next_category_id,
                            owner_id,
                            int(master_row["id"]),
                            str(row["code"]),
                            str(row["title"]),
                            int(row["is_active"]),
                            int(row["sort_order"]),
                        )
                    )
                    next_category_id += 1
            conn.executemany(
                """
                INSERT INTO portfolio_categories_new (
                    id, demo_owner_id, master_id, code, title, is_active, sort_order
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """
                ,
                migrated_category_rows,
            )
            conn.execute("DROP TABLE portfolio_categories")
            conn.execute("ALTER TABLE portfolio_categories_new RENAME TO portfolio_categories")
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_portfolio_categories_owner_sort
            ON portfolio_categories (demo_owner_id, master_id, sort_order, code)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS closed_dates (
                demo_owner_id INTEGER NOT NULL DEFAULT 0,
                master_id INTEGER NOT NULL,
                date TEXT NOT NULL,
                PRIMARY KEY(demo_owner_id, master_id, date)
            )
            """
        )
        closed_dates_columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(closed_dates)").fetchall()
        }
        if "demo_owner_id" not in closed_dates_columns or "master_id" not in closed_dates_columns:
            old_closed_date_rows = conn.execute("SELECT * FROM closed_dates").fetchall()
            conn.execute(
                """
                CREATE TABLE closed_dates_new (
                    demo_owner_id INTEGER NOT NULL DEFAULT 0,
                    master_id INTEGER NOT NULL,
                    date TEXT NOT NULL,
                    PRIMARY KEY(demo_owner_id, master_id, date)
                )
                """
            )
            conn.executemany(
                """
                INSERT INTO closed_dates_new (demo_owner_id, master_id, date)
                VALUES (?, ?, ?)
                """,
                [
                    (
                        int(row["demo_owner_id"]) if "demo_owner_id" in closed_dates_columns else SYSTEM_DEMO_OWNER_ID,
                        int(row["master_id"]) if "master_id" in closed_dates_columns and row["master_id"] else default_master_by_owner.get(
                            int(row["demo_owner_id"]) if "demo_owner_id" in closed_dates_columns else SYSTEM_DEMO_OWNER_ID,
                            (0, DEFAULT_MASTER_NAME),
                        )[0],
                        str(row["date"]),
                    )
                    for row in old_closed_date_rows
                ],
            )
            conn.execute("DROP TABLE closed_dates")
            conn.execute("ALTER TABLE closed_dates_new RENAME TO closed_dates")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS slot_overrides (
                demo_owner_id INTEGER NOT NULL DEFAULT 0,
                master_id INTEGER NOT NULL,
                date TEXT NOT NULL,
                time TEXT NOT NULL,
                is_available INTEGER NOT NULL CHECK(is_available IN (0, 1)),
                PRIMARY KEY(demo_owner_id, master_id, date, time)
            )
            """
        )
        slot_override_columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(slot_overrides)").fetchall()
        }
        if "demo_owner_id" not in slot_override_columns or "master_id" not in slot_override_columns:
            old_slot_override_rows = conn.execute("SELECT * FROM slot_overrides").fetchall()
            conn.execute(
                """
                CREATE TABLE slot_overrides_new (
                    demo_owner_id INTEGER NOT NULL DEFAULT 0,
                    master_id INTEGER NOT NULL,
                    date TEXT NOT NULL,
                    time TEXT NOT NULL,
                    is_available INTEGER NOT NULL CHECK(is_available IN (0, 1)),
                    PRIMARY KEY(demo_owner_id, master_id, date, time)
                )
                """
            )
            conn.executemany(
                """
                INSERT INTO slot_overrides_new (demo_owner_id, master_id, date, time, is_available)
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    (
                        int(row["demo_owner_id"]) if "demo_owner_id" in slot_override_columns else SYSTEM_DEMO_OWNER_ID,
                        int(row["master_id"]) if "master_id" in slot_override_columns and row["master_id"] else default_master_by_owner.get(
                            int(row["demo_owner_id"]) if "demo_owner_id" in slot_override_columns else SYSTEM_DEMO_OWNER_ID,
                            (0, DEFAULT_MASTER_NAME),
                        )[0],
                        str(row["date"]),
                        str(row["time"]),
                        int(row["is_available"]),
                    )
                    for row in old_slot_override_rows
                ],
            )
            conn.execute("DROP TABLE slot_overrides")
            conn.execute("ALTER TABLE slot_overrides_new RENAME TO slot_overrides")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS app_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS admin_users (
                user_id INTEGER PRIMARY KEY,
                created_at TEXT NOT NULL,
                added_by INTEGER
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS demo_leads (
                user_id INTEGER PRIMARY KEY,
                status TEXT NOT NULL DEFAULT 'lead',
                assigned_by INTEGER,
                assigned_at TEXT NOT NULL,
                paid_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sales_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                tariff_code TEXT NOT NULL,
                tariff_title TEXT NOT NULL,
                amount_minor INTEGER NOT NULL,
                currency TEXT NOT NULL,
                payload TEXT UNIQUE NOT NULL,
                status TEXT NOT NULL,
                provider_payment_charge_id TEXT,
                telegram_payment_charge_id TEXT,
                created_at TEXT NOT NULL,
                paid_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS lead_invites (
                token TEXT PRIMARY KEY,
                manager_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_sales_orders_user_status
            ON sales_orders (user_id, status)
            """
        )

        services_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM services WHERE demo_owner_id = ?",
            (SYSTEM_DEMO_OWNER_ID,),
        ).fetchone()["cnt"]
        if services_count == 0:
            default_services = [
                ("Базовая процедура", 40, 0),
                ("Экспресс-услуга", 55, 1),
                ("Премиум-услуга", 70, 2),
            ]
            system_masters = conn.execute(
                """
                SELECT id
                FROM masters
                WHERE demo_owner_id = ?
                ORDER BY sort_order, id
                """,
                (SYSTEM_DEMO_OWNER_ID,),
            ).fetchall()
            if not system_masters:
                system_masters = [{"id": 0}]
            conn.executemany(
                "INSERT INTO services (demo_owner_id, master_id, name, price, sort_order) VALUES (?, ?, ?, ?, ?)",
                [
                    (SYSTEM_DEMO_OWNER_ID, int(master["id"]), name, price, sort_order)
                    for master in system_masters
                    for name, price, sort_order in default_services
                ],
            )

        categories_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM portfolio_categories WHERE demo_owner_id = ?",
            (SYSTEM_DEMO_OWNER_ID,),
        ).fetchone()["cnt"]
        if categories_count == 0:
            system_masters = conn.execute(
                """
                SELECT id
                FROM masters
                WHERE demo_owner_id = ?
                ORDER BY sort_order, id
                """,
                (SYSTEM_DEMO_OWNER_ID,),
            ).fetchall()
            if not system_masters:
                system_masters = [{"id": 0}]
            category_seed = []
            for master in system_masters:
                for sort_order, (code, data) in enumerate(PORTFOLIO.items()):
                    category_seed.append((SYSTEM_DEMO_OWNER_ID, int(master["id"]), code, str(data["title"]), sort_order))
            conn.executemany(
                "INSERT INTO portfolio_categories (demo_owner_id, master_id, code, title, sort_order) VALUES (?, ?, ?, ?, ?)",
                category_seed,
            )

        portfolio_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM portfolio_items WHERE demo_owner_id = ?",
            (SYSTEM_DEMO_OWNER_ID,),
        ).fetchone()["cnt"]
        if portfolio_count == 0:
            seed = []
            system_masters = conn.execute(
                """
                SELECT id
                FROM masters
                WHERE demo_owner_id = ?
                ORDER BY sort_order, id
                """,
                (SYSTEM_DEMO_OWNER_ID,),
            ).fetchall()
            if not system_masters:
                system_masters = [{"id": 0}]
            for master in system_masters:
                sort_order = 0
                for category, data in PORTFOLIO.items():
                    for url in data["urls"]:
                        seed.append((SYSTEM_DEMO_OWNER_ID, int(master["id"]), category, url, sort_order))
                        sort_order += 1
            conn.executemany(
                "INSERT INTO portfolio_items (demo_owner_id, master_id, category, url, sort_order) VALUES (?, ?, ?, ?, ?)",
                seed,
            )

        # Синхронизация: если в portfolio_items есть категории, которых нет в справочнике.
        conn.execute(
            "UPDATE portfolio_items SET url = ? WHERE url <> ?",
            (DEFAULT_DEMO_REFERENCE_IMAGE, DEFAULT_DEMO_REFERENCE_IMAGE),
        )

        known_codes = {
            row["code"]
            for row in conn.execute(
                "SELECT code FROM portfolio_categories WHERE demo_owner_id = ?",
                (SYSTEM_DEMO_OWNER_ID,),
            ).fetchall()
        }
        existing_item_categories = {
            row["category"]
            for row in conn.execute(
                "SELECT DISTINCT category FROM portfolio_items WHERE demo_owner_id = ?",
                (SYSTEM_DEMO_OWNER_ID,),
            ).fetchall()
        }
        missing_codes = sorted(existing_item_categories - known_codes)
        if missing_codes:
            system_masters = conn.execute(
                """
                SELECT id
                FROM masters
                WHERE demo_owner_id = ?
                ORDER BY sort_order, id
                """,
                (SYSTEM_DEMO_OWNER_ID,),
            ).fetchall()
            if not system_masters:
                system_masters = [{"id": 0}]
            next_sort = conn.execute(
                "SELECT COALESCE(MAX(sort_order), -1) AS m FROM portfolio_categories WHERE demo_owner_id = ?",
                (SYSTEM_DEMO_OWNER_ID,),
            ).fetchone()["m"]
            payload = []
            for idx, code in enumerate(missing_codes, start=1):
                for master in system_masters:
                    payload.append((SYSTEM_DEMO_OWNER_ID, int(master["id"]), code, code, int(next_sort) + idx))
            conn.executemany(
                "INSERT INTO portfolio_categories (demo_owner_id, master_id, code, title, sort_order) VALUES (?, ?, ?, ?, ?)",
                payload,
            )

        migrate_legacy_demo_seed_data(conn)

        # Текущий админ хранится в БД, чтобы разработчик мог менять его без правок кода.
        conn.execute(
            """
            INSERT INTO app_settings (key, value)
            VALUES ('admin_user_id', ?)
            ON CONFLICT(key) DO NOTHING
            """,
            (str(ADMIN_ID_INT),),
        )
        # Базовый админ из .env
        now_iso = datetime.now(LOCAL_TZ).isoformat()
        conn.execute(
            """
            INSERT OR IGNORE INTO admin_users (user_id, created_at, added_by)
            VALUES (?, ?, ?)
            """,
            (ADMIN_ID_INT, now_iso, DEVELOPER_ID_INT),
        )
        # Миграция со старого режима "один админ" в app_settings.
        old_admin_row = conn.execute(
            "SELECT value FROM app_settings WHERE key = 'admin_user_id'"
        ).fetchone()
        if old_admin_row is not None:
            old_admin_raw = str(old_admin_row["value"]).strip()
            if old_admin_raw.isdigit():
                conn.execute(
                    """
                    INSERT OR IGNORE INTO admin_users (user_id, created_at, added_by)
                    VALUES (?, ?, ?)
                    """,
                    (int(old_admin_raw), now_iso, DEVELOPER_ID_INT),
                )


def get_current_demo_owner_id() -> int:
    return int(CURRENT_DEMO_OWNER_ID.get())


def resolve_db_demo_owner_id(demo_owner_id: int | None = None) -> int:
    if demo_owner_id is not None:
        return int(demo_owner_id)
    current = get_current_demo_owner_id()
    return SYSTEM_DEMO_OWNER_ID if current <= 0 else current


def ensure_demo_workspace(demo_owner_id: int) -> None:
    owner_id = int(demo_owner_id)
    if owner_id <= 0:
        return
    if owner_id in INITIALIZED_WORKSPACES:
        return

    with get_connection() as conn:
        masters_exist = conn.execute(
            "SELECT 1 FROM masters WHERE demo_owner_id = ? LIMIT 1",
            (owner_id,),
        ).fetchone()
        if masters_exist is None:
            template_masters = conn.execute(
                """
                SELECT name, specialization, description, photo, is_active, sort_order
                FROM masters
                WHERE demo_owner_id = ?
                ORDER BY sort_order, id
                """,
                (SYSTEM_DEMO_OWNER_ID,),
            ).fetchall()
            if template_masters:
                conn.executemany(
                    """
                    INSERT INTO masters (demo_owner_id, name, specialization, description, photo, is_active, sort_order)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            owner_id,
                            str(row["name"]),
                            str(row["specialization"] or ""),
                            str(row["description"] or ""),
                            row["photo"],
                            int(row["is_active"]),
                            int(row["sort_order"]),
                        )
                        for row in template_masters
                    ],
                )
            else:
                specialization, description = get_default_master_profile(DEFAULT_MASTER_NAME)
                conn.execute(
                    """
                    INSERT INTO masters (demo_owner_id, name, specialization, description, photo, is_active, sort_order)
                    VALUES (?, ?, ?, ?, NULL, 1, 0)
                    """,
                    (owner_id, DEFAULT_MASTER_NAME, specialization, description),
                )

        mode_exists = conn.execute(
            """
            SELECT 1
            FROM workspace_settings
            WHERE demo_owner_id = ? AND key = ?
            LIMIT 1
            """,
            (owner_id, WORKSPACE_SETTING_BOOKING_MODE),
        ).fetchone()
        if mode_exists is None:
            template_mode = conn.execute(
                """
                SELECT value
                FROM workspace_settings
                WHERE demo_owner_id = ? AND key = ?
                LIMIT 1
                """,
                (SYSTEM_DEMO_OWNER_ID, WORKSPACE_SETTING_BOOKING_MODE),
            ).fetchone()
            if template_mode is not None and str(template_mode["value"]) in {WORKSPACE_MODE_SOLO, WORKSPACE_MODE_TEAM}:
                default_mode = str(template_mode["value"])
            else:
                active_count = conn.execute(
                    "SELECT COUNT(*) AS cnt FROM masters WHERE demo_owner_id = ? AND is_active = 1",
                    (owner_id,),
                ).fetchone()
                default_mode = (
                    WORKSPACE_MODE_TEAM
                    if active_count is not None and int(active_count["cnt"]) > 1
                    else WORKSPACE_MODE_SOLO
                )
            conn.execute(
                """
                INSERT INTO workspace_settings (demo_owner_id, key, value)
                VALUES (?, ?, ?)
                """,
                (owner_id, WORKSPACE_SETTING_BOOKING_MODE, default_mode),
            )

        service_exists = conn.execute(
            "SELECT 1 FROM services WHERE demo_owner_id = ? LIMIT 1",
            (owner_id,),
        ).fetchone()
        if service_exists is None:
            owner_masters = conn.execute(
                """
                SELECT id, name
                FROM masters
                WHERE demo_owner_id = ?
                ORDER BY sort_order, id
                """,
                (owner_id,),
            ).fetchall()
            owner_master_by_name = {str(row["name"]): int(row["id"]) for row in owner_masters}
            fallback_master_id = int(owner_masters[0]["id"]) if owner_masters else 0
            template_master_rows = conn.execute(
                """
                SELECT id, name
                FROM masters
                WHERE demo_owner_id = ?
                """,
                (SYSTEM_DEMO_OWNER_ID,),
            ).fetchall()
            template_master_by_id = {int(row["id"]): str(row["name"]) for row in template_master_rows}
            template_services = conn.execute(
                """
                SELECT master_id, name, price, is_active, sort_order
                FROM services
                WHERE demo_owner_id = ?
                ORDER BY master_id, sort_order, id
                """,
                (SYSTEM_DEMO_OWNER_ID,),
            ).fetchall()
            conn.executemany(
                """
                INSERT INTO services (demo_owner_id, master_id, name, price, is_active, sort_order)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        owner_id,
                        owner_master_by_name.get(template_master_by_id.get(int(row["master_id"]), ""), fallback_master_id),
                        str(row["name"]),
                        int(row["price"]),
                        int(row["is_active"]),
                        int(row["sort_order"]),
                    )
                    for row in template_services
                ],
            )

        category_exists = conn.execute(
            "SELECT 1 FROM portfolio_categories WHERE demo_owner_id = ? LIMIT 1",
            (owner_id,),
        ).fetchone()
        if category_exists is None:
            owner_masters = conn.execute(
                """
                SELECT id, name
                FROM masters
                WHERE demo_owner_id = ?
                ORDER BY sort_order, id
                """,
                (owner_id,),
            ).fetchall()
            owner_master_by_name = {str(row["name"]): int(row["id"]) for row in owner_masters}
            fallback_master_id = int(owner_masters[0]["id"]) if owner_masters else 0
            template_master_rows = conn.execute(
                """
                SELECT id, name
                FROM masters
                WHERE demo_owner_id = ?
                """,
                (SYSTEM_DEMO_OWNER_ID,),
            ).fetchall()
            template_master_by_id = {int(row["id"]): str(row["name"]) for row in template_master_rows}
            template_categories = conn.execute(
                """
                SELECT master_id, code, title, is_active, sort_order
                FROM portfolio_categories
                WHERE demo_owner_id = ?
                ORDER BY master_id, sort_order, code
                """,
                (SYSTEM_DEMO_OWNER_ID,),
            ).fetchall()
            conn.executemany(
                """
                INSERT INTO portfolio_categories (demo_owner_id, master_id, code, title, is_active, sort_order)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        owner_id,
                        owner_master_by_name.get(template_master_by_id.get(int(row["master_id"]), ""), fallback_master_id),
                        str(row["code"]),
                        str(row["title"]),
                        int(row["is_active"]),
                        int(row["sort_order"]),
                    )
                    for row in template_categories
                ],
            )

        item_exists = conn.execute(
            "SELECT 1 FROM portfolio_items WHERE demo_owner_id = ? LIMIT 1",
            (owner_id,),
        ).fetchone()
        if item_exists is None:
            owner_masters = conn.execute(
                """
                SELECT id, name
                FROM masters
                WHERE demo_owner_id = ?
                ORDER BY sort_order, id
                """,
                (owner_id,),
            ).fetchall()
            owner_master_by_name = {str(row["name"]): int(row["id"]) for row in owner_masters}
            fallback_master_id = int(owner_masters[0]["id"]) if owner_masters else 0
            template_master_rows = conn.execute(
                """
                SELECT id, name
                FROM masters
                WHERE demo_owner_id = ?
                """,
                (SYSTEM_DEMO_OWNER_ID,),
            ).fetchall()
            template_master_by_id = {int(row["id"]): str(row["name"]) for row in template_master_rows}
            template_items = conn.execute(
                """
                SELECT master_id, category, url, is_active, sort_order
                FROM portfolio_items
                WHERE demo_owner_id = ?
                ORDER BY master_id, sort_order, id
                """,
                (SYSTEM_DEMO_OWNER_ID,),
            ).fetchall()
            conn.executemany(
                """
                INSERT INTO portfolio_items (demo_owner_id, master_id, category, url, is_active, sort_order)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        owner_id,
                        owner_master_by_name.get(template_master_by_id.get(int(row["master_id"]), ""), fallback_master_id),
                        str(row["category"]),
                        str(row["url"]),
                        int(row["is_active"]),
                        int(row["sort_order"]),
                    )
                    for row in template_items
                ],
            )
    INITIALIZED_WORKSPACES.add(owner_id)


def get_all_masters(demo_owner_id: int | None = None) -> list[sqlite3.Row]:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, name, specialization, description, photo, is_active, sort_order
            FROM masters
            WHERE demo_owner_id = ?
            ORDER BY sort_order, id
            """,
            (owner_id,),
        ).fetchall()
    return rows


def get_workspace_setting(key: str, demo_owner_id: int | None = None) -> str | None:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT value
            FROM workspace_settings
            WHERE demo_owner_id = ? AND key = ?
            """,
            (owner_id, key),
        ).fetchone()
    return str(row["value"]) if row is not None else None


def set_workspace_setting(key: str, value: str, demo_owner_id: int | None = None) -> None:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO workspace_settings (demo_owner_id, key, value)
            VALUES (?, ?, ?)
            ON CONFLICT(demo_owner_id, key) DO UPDATE SET value = excluded.value
            """,
            (owner_id, key, value),
        )


def get_workspace_booking_mode(demo_owner_id: int | None = None) -> str:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    value = get_workspace_setting(WORKSPACE_SETTING_BOOKING_MODE, demo_owner_id=owner_id)
    if value in {WORKSPACE_MODE_SOLO, WORKSPACE_MODE_TEAM}:
        return value
    return WORKSPACE_MODE_TEAM if has_multiple_active_masters(demo_owner_id=owner_id) else WORKSPACE_MODE_SOLO


def set_workspace_booking_mode(mode: str, demo_owner_id: int | None = None) -> str:
    normalized = WORKSPACE_MODE_TEAM if mode == WORKSPACE_MODE_TEAM else WORKSPACE_MODE_SOLO
    set_workspace_setting(WORKSPACE_SETTING_BOOKING_MODE, normalized, demo_owner_id=demo_owner_id)
    return normalized


def is_team_mode_enabled(demo_owner_id: int | None = None) -> bool:
    return get_workspace_booking_mode(demo_owner_id=demo_owner_id) == WORKSPACE_MODE_TEAM


def is_master_choice_enabled(demo_owner_id: int | None = None) -> bool:
    return is_team_mode_enabled(demo_owner_id=demo_owner_id) and has_multiple_active_masters(demo_owner_id=demo_owner_id)


def get_active_masters(demo_owner_id: int | None = None) -> list[sqlite3.Row]:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, name, specialization, description, photo, is_active, sort_order
            FROM masters
            WHERE demo_owner_id = ? AND is_active = 1
            ORDER BY sort_order, id
            """,
            (owner_id,),
        ).fetchall()
    return rows


def get_master_by_id(master_id: int, demo_owner_id: int | None = None) -> sqlite3.Row | None:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT id, name, specialization, description, photo, is_active, sort_order
            FROM masters
            WHERE id = ? AND demo_owner_id = ?
            """,
            (master_id, owner_id),
        ).fetchone()
    return row


def get_primary_master(demo_owner_id: int | None = None, active_only: bool = False) -> sqlite3.Row | None:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    where_sql = "WHERE demo_owner_id = ? AND is_active = 1" if active_only else "WHERE demo_owner_id = ?"
    with get_connection() as conn:
        row = conn.execute(
            f"""
            SELECT id, name, specialization, description, photo, is_active, sort_order
            FROM masters
            {where_sql}
            ORDER BY sort_order, id
            LIMIT 1
            """,
            (owner_id,),
        ).fetchone()
    return row


def has_multiple_active_masters(demo_owner_id: int | None = None) -> bool:
    return len(get_active_masters(demo_owner_id=demo_owner_id)) > 1


def set_primary_master(master_id: int, demo_owner_id: int | None = None) -> bool:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    master = get_master_by_id(master_id, demo_owner_id=owner_id)
    if master is None:
        return False
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id
            FROM masters
            WHERE demo_owner_id = ?
            ORDER BY CASE WHEN id = ? THEN 0 ELSE 1 END, sort_order, id
            """,
            (owner_id, master_id),
        ).fetchall()
        for sort_order, row in enumerate(rows):
            conn.execute(
                "UPDATE masters SET sort_order = ? WHERE id = ? AND demo_owner_id = ?",
                (sort_order, int(row["id"]), owner_id),
            )
    return True


def get_master_sort_position(master_id: int, demo_owner_id: int | None = None) -> tuple[int, int] | None:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    rows = get_all_masters(demo_owner_id=owner_id)
    for index, row in enumerate(rows, start=1):
        if int(row["id"]) == int(master_id):
            return index, len(rows)
    return None


def move_master(master_id: int, direction: str, demo_owner_id: int | None = None) -> tuple[bool, str]:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    rows = list(get_all_masters(demo_owner_id=owner_id))
    current_index = next((idx for idx, row in enumerate(rows) if int(row["id"]) == int(master_id)), None)
    if current_index is None:
        return False, "Мастер не найден."

    if direction == "up":
        target_index = current_index - 1
        if target_index < 0:
            return False, "Мастер уже находится выше всех."
    elif direction == "down":
        target_index = current_index + 1
        if target_index >= len(rows):
            return False, "Мастер уже находится внизу списка."
    else:
        return False, "Неизвестное направление сортировки."

    rows[current_index], rows[target_index] = rows[target_index], rows[current_index]
    with get_connection() as conn:
        for sort_order, row in enumerate(rows):
            conn.execute(
                "UPDATE masters SET sort_order = ? WHERE id = ? AND demo_owner_id = ?",
                (sort_order, int(row["id"]), owner_id),
            )
    return True, "Порядок мастеров обновлен."


def create_master(
    name: str,
    demo_owner_id: int | None = None,
    specialization: str | None = None,
    description: str | None = None,
    photo: str | None = None,
) -> bool:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    default_specialization, default_description = get_default_master_profile(name)
    with get_connection() as conn:
        try:
            max_sort = conn.execute(
                "SELECT COALESCE(MAX(sort_order), -1) AS m FROM masters WHERE demo_owner_id = ?",
                (owner_id,),
            ).fetchone()["m"]
            conn.execute(
                """
                INSERT INTO masters (demo_owner_id, name, specialization, description, photo, sort_order)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    owner_id,
                    name,
                    (specialization or "").strip() or default_specialization,
                    (description or "").strip() or default_description,
                    photo,
                    int(max_sort) + 1,
                ),
            )
            return True
        except sqlite3.IntegrityError:
            return False


def update_master_name(master_id: int, name: str, demo_owner_id: int | None = None) -> bool:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    with get_connection() as conn:
        try:
            cur = conn.execute(
                "UPDATE masters SET name = ? WHERE id = ? AND demo_owner_id = ?",
                (name, master_id, owner_id),
            )
            if cur.rowcount == 0:
                return False
            conn.execute(
                "UPDATE appointments SET master_name = ? WHERE master_id = ? AND demo_owner_id = ?",
                (name, master_id, owner_id),
            )
            return True
        except sqlite3.IntegrityError:
            return False


def update_master_specialization(master_id: int, specialization: str, demo_owner_id: int | None = None) -> bool:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    with get_connection() as conn:
        cur = conn.execute(
            "UPDATE masters SET specialization = ? WHERE id = ? AND demo_owner_id = ?",
            (specialization, master_id, owner_id),
        )
    return cur.rowcount > 0


def update_master_description(master_id: int, description: str, demo_owner_id: int | None = None) -> bool:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    with get_connection() as conn:
        cur = conn.execute(
            "UPDATE masters SET description = ? WHERE id = ? AND demo_owner_id = ?",
            (description, master_id, owner_id),
        )
    return cur.rowcount > 0


def update_master_photo(master_id: int, photo: str | None, demo_owner_id: int | None = None) -> bool:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    with get_connection() as conn:
        cur = conn.execute(
            "UPDATE masters SET photo = ? WHERE id = ? AND demo_owner_id = ?",
            (photo, master_id, owner_id),
        )
    return cur.rowcount > 0


def toggle_master_active(master_id: int, demo_owner_id: int | None = None) -> tuple[bool, str]:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    master = get_master_by_id(master_id, demo_owner_id=owner_id)
    if master is None:
        return False, "Мастер не найден."

    current_active = int(master["is_active"]) == 1
    if current_active and len(get_active_masters(demo_owner_id=owner_id)) <= 1:
        return False, "Нельзя скрыть последнего активного мастера."

    with get_connection() as conn:
        conn.execute(
            "UPDATE masters SET is_active = ? WHERE id = ? AND demo_owner_id = ?",
            (0 if current_active else 1, master_id, owner_id),
        )
    return True, "Статус мастера обновлен."


def delete_master(master_id: int, demo_owner_id: int | None = None) -> tuple[bool, str]:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    master = get_master_by_id(master_id, demo_owner_id=owner_id)
    if master is None:
        return False, "Мастер не найден."
    if len(get_all_masters(demo_owner_id=owner_id)) <= 1:
        return False, "Нельзя удалить последнего мастера."
    if int(master["is_active"]) == 1 and len(get_active_masters(demo_owner_id=owner_id)) <= 1:
        return False, "Нельзя удалить последнего активного мастера."

    now = datetime.now(LOCAL_TZ)
    today = now.strftime("%Y-%m-%d")
    current_time = now.strftime("%H:%M")
    with get_connection() as conn:
        future_row = conn.execute(
            """
            SELECT 1
            FROM appointments
            WHERE demo_owner_id = ?
              AND master_id = ?
              AND (
                    appointment_date > ?
                 OR (appointment_date = ? AND appointment_time >= ?)
              )
            LIMIT 1
            """,
            (owner_id, master_id, today, today, current_time),
        ).fetchone()
        if future_row is not None:
            return False, "У мастера есть будущие записи. Сначала перенесите или отмените их."

        conn.execute(
            "DELETE FROM slot_overrides WHERE demo_owner_id = ? AND master_id = ?",
            (owner_id, master_id),
        )
        conn.execute(
            "DELETE FROM closed_dates WHERE demo_owner_id = ? AND master_id = ?",
            (owner_id, master_id),
        )
        cur = conn.execute(
            "DELETE FROM masters WHERE id = ? AND demo_owner_id = ?",
            (master_id, owner_id),
        )
    if cur.rowcount == 0:
        return False, "Не удалось удалить мастера."
    return True, "Мастер удален."


def upsert_user(user_id: int, full_name: str, username: str | None) -> None:
    now_iso = datetime.now(LOCAL_TZ).isoformat()
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO users (user_id, full_name, username, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                full_name = excluded.full_name,
                username = excluded.username
            """,
            (user_id, full_name, username, now_iso),
        )


def upsert_workspace_user(
    demo_owner_id: int,
    user_id: int,
    full_name: str,
    username: str | None,
) -> None:
    owner_id = int(demo_owner_id)
    now_iso = datetime.now(LOCAL_TZ).isoformat()
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO workspace_users (
                demo_owner_id, user_id, full_name, username, first_seen_at, last_seen_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(demo_owner_id, user_id) DO UPDATE SET
                full_name = excluded.full_name,
                username = excluded.username,
                last_seen_at = excluded.last_seen_at
            """,
            (owner_id, user_id, full_name, username, now_iso, now_iso),
        )


def get_all_user_ids() -> list[int]:
    with get_connection() as conn:
        rows = conn.execute("SELECT user_id FROM users").fetchall()
    return [int(row["user_id"]) for row in rows]


def get_workspace_user_ids(demo_owner_id: int | None = None) -> list[int]:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    user_ids: set[int] = set()
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT user_id FROM workspace_users WHERE demo_owner_id = ?",
            (owner_id,),
        ).fetchall()
        user_ids.update(int(row["user_id"]) for row in rows)

        rows = conn.execute(
            "SELECT DISTINCT user_id FROM appointments WHERE demo_owner_id = ?",
            (owner_id,),
        ).fetchall()
        user_ids.update(int(row["user_id"]) for row in rows)

    if owner_id > 0:
        user_ids.add(owner_id)
    return sorted(user_ids)


def get_user_by_user_id(user_id: int) -> sqlite3.Row | None:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT user_id, full_name, username, created_at FROM users WHERE user_id = ?",
            (user_id,),
        ).fetchone()
    return row


def get_recent_users(limit: int = 50) -> list[sqlite3.Row]:
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT user_id, full_name, username, created_at
            FROM users
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return rows


def get_demo_lead_row(user_id: int) -> sqlite3.Row | None:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT user_id, status, assigned_by, assigned_at, paid_at FROM demo_leads WHERE user_id = ?",
            (user_id,),
        ).fetchone()
    return row


def assign_demo_lead(user_id: int, assigned_by: int | None) -> tuple[bool, bool]:
    previous = get_demo_lead_row(user_id)
    add_demo_lead(user_id, assigned_by)
    current = get_demo_lead_row(user_id)
    is_new = previous is None
    previous_assigned = int(previous["assigned_by"]) if previous is not None and previous["assigned_by"] is not None else None
    current_assigned = int(current["assigned_by"]) if current is not None and current["assigned_by"] is not None else None
    manager_changed = previous_assigned != current_assigned
    return is_new, manager_changed


def get_demo_leads(limit: int = 50, assigned_by: int | None = None) -> list[sqlite3.Row]:
    where_sql = ""
    params: list[object] = []
    if assigned_by is not None:
        where_sql = "WHERE l.assigned_by = ?"
        params.append(int(assigned_by))
    params.append(int(limit))

    with get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT l.user_id, l.status, l.assigned_by, l.assigned_at, l.paid_at, u.full_name, u.username
            FROM demo_leads l
            LEFT JOIN users u ON u.user_id = l.user_id
            {where_sql}
            ORDER BY l.assigned_at DESC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()
    return rows


def add_demo_lead(user_id: int, assigned_by: int | None) -> bool:
    now_iso = datetime.now(LOCAL_TZ).isoformat()
    with get_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO demo_leads (user_id, status, assigned_by, assigned_at)
            VALUES (?, 'lead', ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                status = 'lead',
                assigned_by = excluded.assigned_by,
                assigned_at = excluded.assigned_at,
                paid_at = NULL
            """,
            (user_id, assigned_by, now_iso),
        )
    return cur.rowcount > 0


def remove_demo_lead(user_id: int) -> bool:
    with get_connection() as conn:
        cur = conn.execute("DELETE FROM demo_leads WHERE user_id = ?", (user_id,))
    return cur.rowcount > 0


def mark_demo_lead_paid(user_id: int) -> None:
    now_iso = datetime.now(LOCAL_TZ).isoformat()
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO demo_leads (user_id, status, assigned_by, assigned_at, paid_at)
            VALUES (?, 'paid', NULL, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                status = 'paid',
                paid_at = excluded.paid_at
            """,
            (user_id, now_iso, now_iso),
        )


def create_lead_invite(manager_id: int) -> str:
    now_iso = datetime.now(LOCAL_TZ).isoformat()
    token = secrets.token_urlsafe(9).replace("-", "").replace("_", "")
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO lead_invites (token, manager_id, created_at, is_active)
            VALUES (?, ?, ?, 1)
            """,
            (token, manager_id, now_iso),
        )
    return token


def get_lead_invite(token: str) -> sqlite3.Row | None:
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT token, manager_id, created_at, is_active
            FROM lead_invites
            WHERE token = ? AND is_active = 1
            """,
            (token,),
        ).fetchone()
    return row


def create_sales_order(
    user_id: int,
    tariff_code: str,
    tariff_title: str,
    amount_minor: int,
    currency: str,
) -> sqlite3.Row:
    now_iso = datetime.now(LOCAL_TZ).isoformat()
    tmp_payload = f"pending:{user_id}:{int(datetime.now(LOCAL_TZ).timestamp() * 1_000_000)}"
    with get_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO sales_orders (
                user_id, tariff_code, tariff_title, amount_minor, currency,
                payload, status, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)
            """,
            (user_id, tariff_code, tariff_title, amount_minor, currency, tmp_payload, now_iso),
        )
        order_id = int(cur.lastrowid)
        payload = f"sale:{order_id}:{user_id}:{tariff_code}"
        conn.execute(
            "UPDATE sales_orders SET payload = ? WHERE id = ?",
            (payload, order_id),
        )
        row = conn.execute(
            "SELECT * FROM sales_orders WHERE id = ?",
            (order_id,),
        ).fetchone()
    if row is None:
        raise RuntimeError("Не удалось создать заказ")
    return row


def get_sales_order_by_payload(payload: str) -> sqlite3.Row | None:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM sales_orders WHERE payload = ?",
            (payload,),
        ).fetchone()
    return row


def mark_sales_order_paid(
    payload: str,
    provider_payment_charge_id: str,
    telegram_payment_charge_id: str,
) -> sqlite3.Row | None:
    now_iso = datetime.now(LOCAL_TZ).isoformat()
    with get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM sales_orders WHERE payload = ?",
            (payload,),
        ).fetchone()
        if row is None:
            return None
        if str(row["status"]) != "paid":
            conn.execute(
                """
                UPDATE sales_orders
                SET status = 'paid',
                    provider_payment_charge_id = ?,
                    telegram_payment_charge_id = ?,
                    paid_at = ?
                WHERE id = ?
                """,
                (provider_payment_charge_id, telegram_payment_charge_id, now_iso, int(row["id"])),
            )
            row = conn.execute(
                "SELECT * FROM sales_orders WHERE id = ?",
                (int(row["id"]),),
            ).fetchone()
    return row


def get_booked_slots(
    date_str: str,
    demo_owner_id: int | None = None,
    master_id: int | None = None,
) -> set[str]:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    with get_connection() as conn:
        if master_id is None:
            rows = conn.execute(
                "SELECT appointment_time FROM appointments WHERE demo_owner_id = ? AND appointment_date = ?",
                (owner_id, date_str),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT appointment_time
                FROM appointments
                WHERE demo_owner_id = ? AND master_id = ? AND appointment_date = ?
                """,
                (owner_id, int(master_id), date_str),
            ).fetchall()
    return {row["appointment_time"] for row in rows}


def get_active_services(
    demo_owner_id: int | None = None,
    master_id: int | None = None,
) -> list[sqlite3.Row]:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    target_master_id = int(master_id or 0)
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, master_id, name, price, is_active, sort_order
            FROM services
            WHERE demo_owner_id = ? AND master_id = ? AND is_active = 1
            ORDER BY sort_order, id
            """,
            (owner_id, target_master_id),
        ).fetchall()
    return rows


def get_all_services(
    demo_owner_id: int | None = None,
    master_id: int | None = None,
) -> list[sqlite3.Row]:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    target_master_id = master_id if master_id is None else int(master_id)
    with get_connection() as conn:
        if target_master_id is None:
            rows = conn.execute(
                """
                SELECT id, master_id, name, price, is_active, sort_order
                FROM services
                WHERE demo_owner_id = ?
                ORDER BY master_id, sort_order, id
                """,
                (owner_id,),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, master_id, name, price, is_active, sort_order
                FROM services
                WHERE demo_owner_id = ? AND master_id = ?
                ORDER BY sort_order, id
                """,
                (owner_id, target_master_id),
            ).fetchall()
    return rows


def get_service_by_id(
    service_id: int,
    demo_owner_id: int | None = None,
    master_id: int | None = None,
) -> sqlite3.Row | None:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    with get_connection() as conn:
        if master_id is None:
            row = conn.execute(
                """
                SELECT id, master_id, name, price, is_active, sort_order
                FROM services
                WHERE id = ? AND demo_owner_id = ?
                """,
                (service_id, owner_id),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT id, master_id, name, price, is_active, sort_order
                FROM services
                WHERE id = ? AND demo_owner_id = ? AND master_id = ?
                """,
                (service_id, owner_id, int(master_id)),
            ).fetchone()
    return row


def create_service(
    name: str,
    price: int,
    demo_owner_id: int | None = None,
    master_id: int | None = None,
) -> bool:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    target_master_id = int(master_id or 0)
    with get_connection() as conn:
        try:
            max_sort = conn.execute(
                "SELECT COALESCE(MAX(sort_order), -1) AS m FROM services WHERE demo_owner_id = ? AND master_id = ?",
                (owner_id, target_master_id),
            ).fetchone()["m"]
            conn.execute(
                "INSERT INTO services (demo_owner_id, master_id, name, price, sort_order) VALUES (?, ?, ?, ?, ?)",
                (owner_id, target_master_id, name, price, int(max_sort) + 1),
            )
            return True
        except sqlite3.IntegrityError:
            return False


def update_service_name(service_id: int, name: str, demo_owner_id: int | None = None) -> bool:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    with get_connection() as conn:
        try:
            cur = conn.execute(
                "UPDATE services SET name = ? WHERE id = ? AND demo_owner_id = ?",
                (name, service_id, owner_id),
            )
            return cur.rowcount > 0
        except sqlite3.IntegrityError:
            return False


def update_service_price(service_id: int, price: int, demo_owner_id: int | None = None) -> bool:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    with get_connection() as conn:
        cur = conn.execute(
            "UPDATE services SET price = ? WHERE id = ? AND demo_owner_id = ?",
            (price, service_id, owner_id),
        )
        return cur.rowcount > 0


def toggle_service_active(service_id: int, demo_owner_id: int | None = None) -> bool:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    with get_connection() as conn:
        row = conn.execute(
            "SELECT is_active FROM services WHERE id = ? AND demo_owner_id = ?",
            (service_id, owner_id),
        ).fetchone()
        if row is None:
            return False
        new_value = 0 if int(row["is_active"]) == 1 else 1
        conn.execute(
            "UPDATE services SET is_active = ? WHERE id = ? AND demo_owner_id = ?",
            (new_value, service_id, owner_id),
        )
        return True


def delete_service(service_id: int, demo_owner_id: int | None = None) -> bool:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    with get_connection() as conn:
        cur = conn.execute(
            "DELETE FROM services WHERE id = ? AND demo_owner_id = ?",
            (service_id, owner_id),
        )
        return cur.rowcount > 0


def get_service_sort_position(service_id: int, demo_owner_id: int | None = None) -> tuple[int, int] | None:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    service = get_service_by_id(service_id, demo_owner_id=owner_id)
    if service is None:
        return None
    rows = get_all_services(demo_owner_id=owner_id, master_id=int(service["master_id"]))
    for index, row in enumerate(rows, start=1):
        if int(row["id"]) == int(service_id):
            return index, len(rows)
    return None


def move_service(service_id: int, direction: str, demo_owner_id: int | None = None) -> tuple[bool, str]:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    service = get_service_by_id(service_id, demo_owner_id=owner_id)
    if service is None:
        return False, "Услуга не найдена."
    master_id = int(service["master_id"])
    rows = list(get_all_services(demo_owner_id=owner_id, master_id=master_id))
    current_index = next((idx for idx, row in enumerate(rows) if int(row["id"]) == int(service_id)), None)
    if current_index is None:
        return False, "Услуга не найдена."
    if direction == "up":
        target_index = current_index - 1
        if target_index < 0:
            return False, "Услуга уже находится выше всех."
    elif direction == "down":
        target_index = current_index + 1
        if target_index >= len(rows):
            return False, "Услуга уже находится внизу списка."
    else:
        return False, "Неизвестное направление сортировки."

    rows[current_index], rows[target_index] = rows[target_index], rows[current_index]
    with get_connection() as conn:
        for sort_order, row in enumerate(rows):
            conn.execute(
                "UPDATE services SET sort_order = ? WHERE id = ? AND demo_owner_id = ?",
                (sort_order, int(row["id"]), owner_id),
            )
    return True, "Порядок услуг обновлен."


def get_portfolio_items(
    category: str | None = None,
    active_only: bool = True,
    demo_owner_id: int | None = None,
    master_id: int | None = None,
) -> list[sqlite3.Row]:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    target_master_id = int(master_id or 0)
    where_parts = ["demo_owner_id = ?", "master_id = ?"]
    params: list[object] = [owner_id, target_master_id]
    if category is not None:
        where_parts.append("category = ?")
        params.append(category)
    if active_only:
        where_parts.append("is_active = 1")
    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    with get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT id, master_id, category, url, is_active, sort_order
            FROM portfolio_items
            {where_sql}
            ORDER BY sort_order, id
            """,
            tuple(params),
        ).fetchall()
    return rows


def add_portfolio_item(
    category: str,
    url: str,
    demo_owner_id: int | None = None,
    master_id: int | None = None,
) -> int:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    target_master_id = int(master_id or 0)
    with get_connection() as conn:
        max_sort = conn.execute(
            """
            SELECT COALESCE(MAX(sort_order), -1) AS m
            FROM portfolio_items
            WHERE demo_owner_id = ? AND master_id = ? AND category = ?
            """,
            (owner_id, target_master_id, category),
        ).fetchone()["m"]
        cur = conn.execute(
            "INSERT INTO portfolio_items (demo_owner_id, master_id, category, url, sort_order) VALUES (?, ?, ?, ?, ?)",
            (owner_id, target_master_id, category, url, int(max_sort) + 1),
        )
        return int(cur.lastrowid)


def get_portfolio_item_by_id(
    item_id: int,
    demo_owner_id: int | None = None,
    master_id: int | None = None,
) -> sqlite3.Row | None:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    with get_connection() as conn:
        if master_id is None:
            row = conn.execute(
                """
                SELECT id, master_id, category, url, is_active, sort_order
                FROM portfolio_items
                WHERE id = ? AND demo_owner_id = ?
                """,
                (item_id, owner_id),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT id, master_id, category, url, is_active, sort_order
                FROM portfolio_items
                WHERE id = ? AND demo_owner_id = ? AND master_id = ?
                """,
                (item_id, owner_id, int(master_id)),
            ).fetchone()
    return row


def delete_portfolio_item(item_id: int, demo_owner_id: int | None = None) -> bool:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    with get_connection() as conn:
        cur = conn.execute(
            "DELETE FROM portfolio_items WHERE id = ? AND demo_owner_id = ?",
            (item_id, owner_id),
        )
        return cur.rowcount > 0


def get_portfolio_categories(
    active_only: bool = False,
    demo_owner_id: int | None = None,
    master_id: int | None = None,
) -> list[sqlite3.Row]:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    target_master_id = int(master_id or 0)
    where_sql = (
        "WHERE demo_owner_id = ? AND master_id = ? AND is_active = 1"
        if active_only
        else "WHERE demo_owner_id = ? AND master_id = ?"
    )
    with get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT code, master_id, title, is_active, sort_order
            FROM portfolio_categories
            {where_sql}
            ORDER BY sort_order, code
            """,
            (owner_id, target_master_id),
        ).fetchall()
    return rows


def get_portfolio_category_by_code(
    code: str,
    demo_owner_id: int | None = None,
    master_id: int | None = None,
) -> sqlite3.Row | None:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    with get_connection() as conn:
        if master_id is None:
            row = conn.execute(
                """
                SELECT code, master_id, title, is_active, sort_order
                FROM portfolio_categories
                WHERE code = ? AND demo_owner_id = ?
                ORDER BY master_id, sort_order, code
                LIMIT 1
                """,
                (code, owner_id),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT code, master_id, title, is_active, sort_order
                FROM portfolio_categories
                WHERE code = ? AND demo_owner_id = ? AND master_id = ?
                """,
                (code, owner_id, int(master_id)),
            ).fetchone()
    return row


def get_portfolio_category_title(
    code: str,
    demo_owner_id: int | None = None,
    master_id: int | None = None,
) -> str:
    row = get_portfolio_category_by_code(code, demo_owner_id=demo_owner_id, master_id=master_id)
    if row is not None:
        return str(row["title"])
    fallback = PORTFOLIO.get(code)
    if fallback is not None:
        return str(fallback["title"])
    return code


def slugify_portfolio_category_code(title: str) -> str:
    translit_map = {
        "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "e", "ж": "zh",
        "з": "z", "и": "i", "й": "y", "к": "k", "л": "l", "м": "m", "н": "n", "о": "o",
        "п": "p", "р": "r", "с": "s", "т": "t", "у": "u", "ф": "f", "х": "h", "ц": "ts",
        "ч": "ch", "ш": "sh", "щ": "sch", "ъ": "", "ы": "y", "ь": "", "э": "e", "ю": "yu",
        "я": "ya",
    }
    normalized = "".join(translit_map.get(ch, ch) for ch in title.lower())
    slug = re.sub(r"[^a-z0-9]+", "_", normalized).strip("_")
    if not slug:
        slug = "category"
    return slug[:32]


def update_portfolio_category_title(
    code: str,
    title: str,
    demo_owner_id: int | None = None,
    master_id: int | None = None,
) -> bool:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    with get_connection() as conn:
        cur = conn.execute(
            "UPDATE portfolio_categories SET title = ? WHERE code = ? AND demo_owner_id = ? AND master_id = ?",
            (title, code, owner_id, int(master_id or 0)),
        )
        return cur.rowcount > 0


def create_portfolio_category(
    title: str,
    demo_owner_id: int | None = None,
    master_id: int | None = None,
) -> str | None:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    target_master_id = int(master_id or 0)
    base_code = slugify_portfolio_category_code(title)
    with get_connection() as conn:
        code = base_code
        suffix = 2
        while conn.execute(
            "SELECT 1 FROM portfolio_categories WHERE code = ? AND demo_owner_id = ? AND master_id = ?",
            (code, owner_id, target_master_id),
        ).fetchone():
            suffix_text = f"_{suffix}"
            code = f"{base_code[:32 - len(suffix_text)]}{suffix_text}"
            suffix += 1

        max_sort = conn.execute(
            "SELECT COALESCE(MAX(sort_order), -1) AS m FROM portfolio_categories WHERE demo_owner_id = ? AND master_id = ?",
            (owner_id, target_master_id),
        ).fetchone()["m"]
        try:
            conn.execute(
                "INSERT INTO portfolio_categories (demo_owner_id, master_id, code, title, sort_order) VALUES (?, ?, ?, ?, ?)",
                (owner_id, target_master_id, code, title, int(max_sort) + 1),
            )
            return code
        except sqlite3.IntegrityError:
            return None


def get_portfolio_category_items_count(
    code: str,
    demo_owner_id: int | None = None,
    master_id: int | None = None,
) -> int:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    with get_connection() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM portfolio_items WHERE category = ? AND demo_owner_id = ? AND master_id = ?",
            (code, owner_id, int(master_id or 0)),
        ).fetchone()
    return int(row["cnt"]) if row is not None else 0


def toggle_portfolio_category_active(
    code: str,
    demo_owner_id: int | None = None,
    master_id: int | None = None,
) -> bool:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    target_master_id = int(master_id or 0)
    with get_connection() as conn:
        row = conn.execute(
            "SELECT is_active FROM portfolio_categories WHERE code = ? AND demo_owner_id = ? AND master_id = ?",
            (code, owner_id, target_master_id),
        ).fetchone()
        if row is None:
            return False
        new_value = 0 if int(row["is_active"]) == 1 else 1
        conn.execute(
            "UPDATE portfolio_categories SET is_active = ? WHERE code = ? AND demo_owner_id = ? AND master_id = ?",
            (new_value, code, owner_id, target_master_id),
        )
    return True


def delete_portfolio_category(
    code: str,
    demo_owner_id: int | None = None,
    master_id: int | None = None,
) -> tuple[bool, str]:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    target_master_id = int(master_id or 0)
    if get_portfolio_category_by_code(code, demo_owner_id=owner_id, master_id=target_master_id) is None:
        return False, "Категория не найдена."
    if get_portfolio_category_items_count(code, demo_owner_id=owner_id, master_id=target_master_id) > 0:
        return False, "Категория содержит фото. Сначала удалите фото из этой категории."
    with get_connection() as conn:
        cur = conn.execute(
            "DELETE FROM portfolio_categories WHERE code = ? AND demo_owner_id = ? AND master_id = ?",
            (code, owner_id, target_master_id),
        )
    if cur.rowcount == 0:
        return False, "Не удалось удалить категорию."
    return True, "Категория удалена."


def get_portfolio_category_sort_position(
    code: str,
    demo_owner_id: int | None = None,
    master_id: int | None = None,
) -> tuple[int, int] | None:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    target_master_id = int(master_id or 0)
    rows = get_portfolio_categories(active_only=False, demo_owner_id=owner_id, master_id=target_master_id)
    for index, row in enumerate(rows, start=1):
        if str(row["code"]) == code:
            return index, len(rows)
    return None


def move_portfolio_category(
    code: str,
    direction: str,
    demo_owner_id: int | None = None,
    master_id: int | None = None,
) -> tuple[bool, str]:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    target_master_id = int(master_id or 0)
    rows = list(get_portfolio_categories(active_only=False, demo_owner_id=owner_id, master_id=target_master_id))
    current_index = next((idx for idx, row in enumerate(rows) if str(row["code"]) == code), None)
    if current_index is None:
        return False, "Категория не найдена."
    if direction == "up":
        target_index = current_index - 1
        if target_index < 0:
            return False, "Категория уже находится выше всех."
    elif direction == "down":
        target_index = current_index + 1
        if target_index >= len(rows):
            return False, "Категория уже находится внизу списка."
    else:
        return False, "Неизвестное направление сортировки."

    rows[current_index], rows[target_index] = rows[target_index], rows[current_index]
    with get_connection() as conn:
        for sort_order, row in enumerate(rows):
            conn.execute(
                """
                UPDATE portfolio_categories
                SET sort_order = ?
                WHERE code = ? AND demo_owner_id = ? AND master_id = ?
                """,
                (sort_order, str(row["code"]), owner_id, target_master_id),
            )
    return True, "Порядок категорий обновлен."


def is_date_closed(
    date_str: str,
    demo_owner_id: int | None = None,
    master_id: int | None = None,
) -> bool:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    target_master_id = int(master_id or 0)
    with get_connection() as conn:
        row = conn.execute(
            "SELECT date FROM closed_dates WHERE demo_owner_id = ? AND master_id = ? AND date = ?",
            (owner_id, target_master_id, date_str),
        ).fetchone()
    return row is not None


def toggle_date_closed(
    date_str: str,
    demo_owner_id: int | None = None,
    master_id: int | None = None,
) -> bool:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    target_master_id = int(master_id or 0)
    with get_connection() as conn:
        if conn.execute(
            "SELECT date FROM closed_dates WHERE demo_owner_id = ? AND master_id = ? AND date = ?",
            (owner_id, target_master_id, date_str),
        ).fetchone():
            conn.execute(
                "DELETE FROM closed_dates WHERE demo_owner_id = ? AND master_id = ? AND date = ?",
                (owner_id, target_master_id, date_str),
            )
            return False
        conn.execute(
            "INSERT INTO closed_dates (demo_owner_id, master_id, date) VALUES (?, ?, ?)",
            (owner_id, target_master_id, date_str),
        )
        return True


def set_slot_override(
    date_str: str,
    time_str: str,
    is_available: int,
    demo_owner_id: int | None = None,
    master_id: int | None = None,
) -> None:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    target_master_id = int(master_id or 0)
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO slot_overrides (demo_owner_id, master_id, date, time, is_available)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(demo_owner_id, master_id, date, time) DO UPDATE SET is_available = excluded.is_available
            """,
            (owner_id, target_master_id, date_str, time_str, is_available),
        )


def clear_slot_settings_for_date(
    date_str: str,
    demo_owner_id: int | None = None,
    master_id: int | None = None,
) -> None:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    target_master_id = int(master_id or 0)
    with get_connection() as conn:
        conn.execute(
            "DELETE FROM slot_overrides WHERE demo_owner_id = ? AND master_id = ? AND date = ?",
            (owner_id, target_master_id, date_str),
        )
        conn.execute(
            "DELETE FROM closed_dates WHERE demo_owner_id = ? AND master_id = ? AND date = ?",
            (owner_id, target_master_id, date_str),
        )


def get_slot_overrides(
    date_str: str,
    demo_owner_id: int | None = None,
    master_id: int | None = None,
) -> dict[str, int]:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    target_master_id = int(master_id or 0)
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT time, is_available
            FROM slot_overrides
            WHERE demo_owner_id = ? AND master_id = ? AND date = ?
            """,
            (owner_id, target_master_id, date_str),
        ).fetchall()
    return {row["time"]: int(row["is_available"]) for row in rows}


def normalize_time_str(time_str: str) -> str | None:
    raw = (time_str or "").strip()
    try:
        dt = datetime.strptime(raw, "%H:%M")
        return dt.strftime("%H:%M")
    except ValueError:
        return None


def sort_time_slots(slots: set[str] | list[str]) -> list[str]:
    def _key(value: str) -> tuple[int, str]:
        parsed = normalize_time_str(value)
        if parsed is None:
            return (99, value)
        return (int(parsed.replace(":", "")), parsed)

    return sorted(set(slots), key=_key)


def get_allowed_slots(
    date_str: str,
    demo_owner_id: int | None = None,
    master_id: int | None = None,
) -> list[str]:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    target_master_id = int(master_id or 0)
    if is_date_closed(date_str, demo_owner_id=owner_id, master_id=target_master_id):
        return []
    slots = set(ALL_TIME_SLOTS)
    overrides = get_slot_overrides(date_str, demo_owner_id=owner_id, master_id=target_master_id)
    for time_str, is_available in overrides.items():
        if is_available == 1:
            slots.add(time_str)
        else:
            slots.discard(time_str)
    return sort_time_slots(slots)


def get_available_slots_for_booking(
    date_str: str,
    demo_owner_id: int | None = None,
    master_id: int | None = None,
) -> list[str]:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    target_master_id = int(master_id or 0)
    allowed = get_allowed_slots(date_str, demo_owner_id=owner_id, master_id=target_master_id)
    booked = get_booked_slots(date_str, demo_owner_id=owner_id, master_id=target_master_id)
    return [slot for slot in allowed if slot not in booked]


def is_day_fully_booked(
    date_str: str,
    demo_owner_id: int | None = None,
    master_id: int | None = None,
) -> bool:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    target_master_id = int(master_id or 0)
    allowed = get_allowed_slots(date_str, demo_owner_id=owner_id, master_id=target_master_id)
    if not allowed:
        return True
    booked = get_booked_slots(date_str, demo_owner_id=owner_id, master_id=target_master_id)
    return all(slot in booked for slot in allowed)


def is_slot_free(
    date_str: str,
    time_str: str,
    demo_owner_id: int | None = None,
    master_id: int | None = None,
) -> bool:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT id FROM appointments
            WHERE demo_owner_id = ? AND master_id = ? AND appointment_date = ? AND appointment_time = ?
            LIMIT 1
            """,
            (owner_id, int(master_id or 0), date_str, time_str),
        ).fetchone()
    return row is None


def create_appointment(
    demo_owner_id: int,
    master_id: int,
    master_name: str,
    user_id: int,
    client_name: str,
    phone: str,
    service: str,
    client_comment: str | None,
    source_file_id: str | None,
    source_file_type: str | None,
    source_file_name: str | None,
    appointment_date: str,
    appointment_time: str,
) -> int | None:
    now_iso = datetime.now(LOCAL_TZ).isoformat()
    owner_id = int(demo_owner_id)
    if owner_id <= 0:
        owner_id = int(user_id)
    try:
        with get_connection() as conn:
            cur = conn.execute(
                """
                INSERT INTO appointments (
                    demo_owner_id, master_id, master_name, user_id, client_name, phone, service,
                    client_comment, source_file_id, source_file_type, source_file_name,
                    appointment_date, appointment_time, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    owner_id,
                    int(master_id),
                    master_name,
                    user_id,
                    client_name,
                    phone,
                    service,
                    client_comment,
                    source_file_id,
                    source_file_type,
                    source_file_name,
                    appointment_date,
                    appointment_time,
                    now_iso,
                ),
            )
            return int(cur.lastrowid)
    except sqlite3.IntegrityError:
        return None


def get_appointment_by_id(
    appointment_id: int,
    demo_owner_id: int | None = None,
    include_all: bool = False,
) -> sqlite3.Row | None:
    with get_connection() as conn:
        if include_all:
            row = conn.execute(
                "SELECT * FROM appointments WHERE id = ?",
                (appointment_id,),
            ).fetchone()
        else:
            owner_id = resolve_db_demo_owner_id(demo_owner_id)
            row = conn.execute(
                "SELECT * FROM appointments WHERE id = ? AND demo_owner_id = ?",
                (appointment_id, owner_id),
            ).fetchone()
    return row


def get_appointments_for_date(date_str: str, demo_owner_id: int | None = None) -> list[sqlite3.Row]:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT * FROM appointments
            WHERE demo_owner_id = ? AND appointment_date = ?
            ORDER BY appointment_time, master_name, id
            """,
            (owner_id, date_str),
        ).fetchall()
    return rows


def get_future_appointments(demo_owner_id: int | None = None) -> list[sqlite3.Row]:
    now = datetime.now(LOCAL_TZ)
    today = now.strftime("%Y-%m-%d")
    current_time = now.strftime("%H:%M")
    owner_id = resolve_db_demo_owner_id(demo_owner_id)

    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT * FROM appointments
            WHERE demo_owner_id = ?
              AND (appointment_date > ?
               OR (appointment_date = ? AND appointment_time >= ?))
            ORDER BY appointment_date, appointment_time, master_name, id
            """,
            (owner_id, today, today, current_time),
        ).fetchall()
    return rows


def get_future_appointments_all() -> list[sqlite3.Row]:
    now = datetime.now(LOCAL_TZ)
    today = now.strftime("%Y-%m-%d")
    current_time = now.strftime("%H:%M")

    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT * FROM appointments
            WHERE appointment_date > ?
               OR (appointment_date = ? AND appointment_time >= ?)
            ORDER BY appointment_date, appointment_time, master_name, id
            """,
            (today, today, current_time),
        ).fetchall()
    return rows


def delete_appointment_by_id(appointment_id: int, demo_owner_id: int | None = None) -> bool:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    with get_connection() as conn:
        cur = conn.execute(
            "DELETE FROM appointments WHERE id = ? AND demo_owner_id = ?",
            (appointment_id, owner_id),
        )
        return cur.rowcount > 0


def get_app_setting(key: str) -> str | None:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT value FROM app_settings WHERE key = ?",
            (key,),
        ).fetchone()
    return str(row["value"]) if row is not None else None


def set_app_setting(key: str, value: str) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO app_settings (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )


def get_admin_ids() -> list[int]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT user_id FROM admin_users ORDER BY created_at, user_id"
        ).fetchall()
    ids = [int(row["user_id"]) for row in rows]
    if not ids:
        return [ADMIN_ID_INT]
    return ids


def add_admin_user(user_id: int, added_by: int | None = None) -> bool:
    with get_connection() as conn:
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO admin_users (user_id, created_at, added_by)
            VALUES (?, ?, ?)
            """,
            (user_id, datetime.now(LOCAL_TZ).isoformat(), added_by),
        )
    return cur.rowcount > 0


def remove_admin_user(user_id: int) -> bool:
    admin_ids = get_admin_ids()
    if user_id not in admin_ids:
        return False
    if len(admin_ids) <= 1:
        return False
    with get_connection() as conn:
        cur = conn.execute("DELETE FROM admin_users WHERE user_id = ?", (user_id,))
    return cur.rowcount > 0


def is_regular_admin(user_id: int) -> bool:
    return is_admin(user_id) and not is_owner(user_id)


def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID_INT


def is_manager(user_id: int) -> bool:
    return user_id in MANAGER_IDS_SET


def is_developer(user_id: int) -> bool:
    return user_id == DEVELOPER_ID_INT


def is_staff_manager(user_id: int) -> bool:
    return STAFF_ID_INT is not None and user_id == STAFF_ID_INT


def get_user_role(user_id: int) -> str:
    if is_owner(user_id):
        return "owner"
    if is_manager_only(user_id):
        return "manager"
    if is_admin(user_id):
        return "admin"
    return "demo_buyer"


def can_manage_admins(user_id: int) -> bool:
    # Управление админ-доступом доступно только владельцу демо-бота.
    return is_owner(user_id)


def can_manage_leads(user_id: int) -> bool:
    return is_owner(user_id) or is_manager(user_id)


def is_demo_lead(user_id: int) -> bool:
    row = get_demo_lead_row(user_id)
    return row is not None and str(row["status"]) in {"lead", "paid"}


def is_admin(user_id: int) -> bool:
    if is_owner(user_id):
        return True
    return user_id in set(get_admin_ids())


def has_admin_panel_access(user_id: int) -> bool:
    return is_admin(user_id) or is_demo_lead(user_id)


def is_manager_only(user_id: int) -> bool:
    return is_manager(user_id) and not is_owner(user_id)


def is_workspace_admin(user_id: int) -> bool:
    # Владелец и внутренние администраторы работают в основном контуре.
    # Демо-покупатель управляет только своим контуром данных.
    return is_owner(user_id) or is_admin(user_id) or is_demo_lead(user_id)


def resolve_demo_owner_id_for_user(user_id: int) -> int:
    # Внутренние роли работают в рабочем контуре владельца.
    if is_owner(user_id) or is_manager(user_id) or is_admin(user_id) or can_manage_admins(user_id):
        return OWNER_ID_INT
    # Любой внешний пользователь имеет собственный изолированный демо-контур.
    return user_id


def get_workspace_admin_notify_ids(demo_owner_id: int) -> list[int]:
    owner_id = OWNER_ID_INT if int(demo_owner_id) == SYSTEM_DEMO_OWNER_ID else int(demo_owner_id)
    if owner_id == OWNER_ID_INT:
        return sorted(set(get_admin_ids()))
    return [owner_id]


def sales_notify_ids() -> list[int]:
    ids = {OWNER_ID_INT}
    ids.update(MANAGER_IDS_SET)
    return sorted(ids)


def format_user_identity(user_id: int, full_name: str | None = None, username: str | None = None) -> str:
    name = escape(full_name) if full_name else str(user_id)
    username_part = f" (@{escape(username)})" if username else ""
    return f"{name}{username_part} [<code>{user_id}</code>]"


async def build_lead_link_for_manager(manager_id: int) -> str | None:
    if bot_instance is None:
        return None
    try:
        me = await bot_instance.get_me()
    except Exception:
        return None
    if not me.username:
        return None
    token = create_lead_invite(manager_id)
    return f"https://t.me/{me.username}?start=lead_{token}"


async def notify_owner_about_new_lead(
    manager_id: int,
    lead_user_id: int,
    lead_full_name: str,
    lead_username: str | None,
) -> None:
    if bot_instance is None:
        return
    manager_row = get_user_by_user_id(manager_id)
    manager_identity = format_user_identity(
        user_id=manager_id,
        full_name=str(manager_row["full_name"]) if manager_row is not None else None,
        username=str(manager_row["username"]) if manager_row is not None and manager_row["username"] else None,
    )
    lead_identity = format_user_identity(
        user_id=lead_user_id,
        full_name=lead_full_name,
        username=lead_username,
    )
    try:
        await bot_instance.send_message(
            chat_id=OWNER_ID_INT,
            text=(
                "🎯 Новый лид по ссылке менеджера\n\n"
                f"Кто привел: {manager_identity}\n"
                f"Клиент: {lead_identity}\n"
                f"Профиль клиента: <a href='tg://user?id={lead_user_id}'>Открыть</a>"
            ),
        )
    except Exception:
        pass


def appointment_datetime(date_str: str, time_str: str) -> datetime:
    date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
    time_obj = datetime.strptime(time_str, "%H:%M").time()
    return datetime.combine(date_obj, time_obj, tzinfo=LOCAL_TZ)


def format_date_human(date_str: str) -> str:
    dt = datetime.strptime(date_str, "%Y-%m-%d").date()
    return f"{dt.day} {MONTH_NAMES_RU_GEN[dt.month]}"


def profile_link_html(user_id: int) -> str:
    return f"<a href='tg://user?id={user_id}'>Профиль</a>"


def get_master_name_from_row(row: sqlite3.Row | dict) -> str:
    value = row["master_name"] if "master_name" in row.keys() else row.get("master_name")  # type: ignore[arg-type]
    return str(value or DEFAULT_MASTER_NAME)


def get_master_specialization_from_row(row: sqlite3.Row | dict) -> str:
    if hasattr(row, "keys") and "specialization" in row.keys():  # type: ignore[attr-defined]
        value = row["specialization"]  # type: ignore[index]
    else:
        value = row.get("specialization")  # type: ignore[union-attr]
    return str(value or DEFAULT_MASTER_SPECIALIZATION)


def get_master_description_from_row(row: sqlite3.Row | dict) -> str:
    if hasattr(row, "keys") and "description" in row.keys():  # type: ignore[attr-defined]
        value = row["description"]  # type: ignore[index]
    else:
        value = row.get("description")  # type: ignore[union-attr]
    return str(value or DEFAULT_MASTER_DESCRIPTION)


def shorten_text(value: str, limit: int = 38) -> str:
    clean = re.sub(r"\s+", " ", value.strip())
    if len(clean) <= limit:
        return clean
    return clean[: max(0, limit - 1)].rstrip() + "…"


def format_master_public_card_text(master_id: int, demo_owner_id: int | None = None) -> str:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    row = get_master_by_id(master_id, demo_owner_id=owner_id)
    if row is None:
        return "👤 Специалист пока недоступен."

    service_count = len(get_active_services(demo_owner_id=owner_id, master_id=master_id))
    category_count = len(get_portfolio_categories(active_only=True, demo_owner_id=owner_id, master_id=master_id))
    specialization = escape(get_master_specialization_from_row(row))
    description = escape(shorten_text(get_master_description_from_row(row), limit=150))
    return (
        f"👤 <b>{escape(str(row['name']))}</b>\n"
        f"✨ {specialization}\n\n"
        f"{description}\n\n"
        f"🧾 Услуги: {service_count}\n"
        f"📸 Портфолио: {category_count} катег."
    )


def format_appointments(rows: list[sqlite3.Row]) -> str:
    if not rows:
        return "📋 Записей пока нет.\n\nКогда клиенты начнут записываться, здесь появится актуальный список."

    items = []
    for row in rows:
        date_human = format_date_human(row["appointment_date"])
        profile_link = profile_link_html(int(row["user_id"]))
        comment = (row["client_comment"] or "").strip()
        comment_line = f"\n💬 Комментарий: {escape(comment)}" if comment else ""
        source_line = "\n📎 Исходник: прикреплён" if row["source_file_id"] else ""
        items.append(
            f"✨ Запись #{row['id']}\n"
            f"📅 {date_human} · {row['appointment_time']}\n"
            f"👤 Специалист: {escape(get_master_name_from_row(row))}\n"
            f"👤 Клиент: {escape(str(row['client_name']))} ({profile_link})\n"
            f"📱 Телефон: {escape(str(row['phone']))}\n"
            f"🧾 Услуга: {escape(str(row['service']))}"
            f"{comment_line}{source_line}"
        )
    return "\n\n".join(items)


def format_slots_admin_text(
    date_str: str,
    demo_owner_id: int | None = None,
    master_id: int | None = None,
) -> str:
    date_human = format_date_human(date_str)
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    target_master_id = int(master_id or 0)
    master_row = get_master_by_id(target_master_id, demo_owner_id=owner_id) if target_master_id else None
    master_title = str(master_row["name"]) if master_row is not None else DEFAULT_MASTER_NAME
    closed = is_date_closed(date_str, demo_owner_id=owner_id, master_id=target_master_id)
    allowed = get_allowed_slots(date_str, demo_owner_id=owner_id, master_id=target_master_id)
    booked = get_booked_slots(date_str, demo_owner_id=owner_id, master_id=target_master_id)
    free = [slot for slot in allowed if slot not in booked]

    if closed:
        status = "❌ Дата закрыта"
    elif not allowed:
        status = "⛔ Свободных слотов нет"
    else:
        status = f"✅ Открыта · свободно {len(free)}"

    return (
        f"🪟 Окошки на {date_human}\n"
        f"👤 Специалист: {master_title}\n\n"
        f"{status}\n"
        f"🕒 Доступно: {', '.join(allowed) if allowed else 'нет'}\n"
        f"📌 Занято: {', '.join(sort_time_slots(list(booked))) if booked else 'нет'}\n\n"
        "Нажмите на слот ниже, чтобы открыть или закрыть его для этой даты."
    )


def format_service_detail_text(service_id: int, demo_owner_id: int | None = None) -> str:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    row = get_service_by_id(service_id, demo_owner_id=owner_id)
    if row is None:
        return "🧾 Услуга пока недоступна."
    master = get_master_by_id(int(row["master_id"]), demo_owner_id=owner_id)
    master_name = str(master["name"]) if master is not None else DEFAULT_MASTER_NAME
    status = "Активна" if int(row["is_active"]) == 1 else "Скрыта"
    position = get_service_sort_position(service_id, demo_owner_id=owner_id)
    lines = [
        f"🧾 {row['name']}",
        "",
        f"ID: #{row['id']}",
        f"👤 Специалист: {master_name}",
        f"💳 Стоимость: {row['price']} USD",
    ]
    if position is not None:
        lines.append(f"↕️ Позиция: {position[0]} из {position[1]}")
    lines.append(f"👁 Статус: {status}")
    return "\n".join(lines)


def format_master_detail_text(master_id: int, demo_owner_id: int | None = None) -> str:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    row = get_master_by_id(master_id, demo_owner_id=owner_id)
    if row is None:
        return "👤 Специалист пока недоступен."
    status = "Активен" if int(row["is_active"]) == 1 else "Скрыт"
    mode = get_workspace_booking_mode(demo_owner_id=owner_id)
    primary_master = get_primary_master(demo_owner_id=owner_id, active_only=True)
    is_primary = primary_master is not None and int(primary_master["id"]) == int(row["id"])
    position = get_master_sort_position(master_id, demo_owner_id=owner_id)
    future_appointments_count = 0
    services_count = len(get_all_services(demo_owner_id=owner_id, master_id=master_id))
    portfolio_count = len(get_portfolio_items(active_only=False, demo_owner_id=owner_id, master_id=master_id))
    now = datetime.now(LOCAL_TZ)
    today = now.strftime("%Y-%m-%d")
    current_time = now.strftime("%H:%M")
    with get_connection() as conn:
        future_row = conn.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM appointments
            WHERE demo_owner_id = ?
              AND master_id = ?
              AND (
                    appointment_date > ?
                 OR (appointment_date = ? AND appointment_time >= ?)
              )
            """,
            (owner_id, master_id, today, today, current_time),
        ).fetchone()
        if future_row is not None:
            future_appointments_count = int(future_row["cnt"])
    description = escape(shorten_text(get_master_description_from_row(row), limit=160))
    lines = [
        f"👤 {escape(str(row['name']))}",
        "",
        f"ID: #{row['id']}",
        f"✨ Специализация: {escape(get_master_specialization_from_row(row))}",
        f"💬 Описание: {description}",
        f"👁 Статус: {status}",
        f"🖼 Фото профиля: {'добавлено' if (row['photo'] or '').strip() else 'не добавлено'}",
    ]
    if position is not None:
        lines.append(f"↕️ Позиция: {position[0]} из {position[1]}")
    lines.extend(
        [
            f"⭐ Роль: {'основной мастер' if is_primary else 'дополнительный мастер'}",
            f"🧭 Формат: {'команда' if mode == WORKSPACE_MODE_TEAM else 'один мастер'}",
            f"🧾 Услуг: {services_count}",
            f"📸 Фото в портфолио: {portfolio_count}",
            f"📅 Будущих записей: {future_appointments_count}",
        ]
    )
    return "\n".join(lines)


async def open_admin_master_detail_screen(
    chat_id: int,
    master_id: int,
    demo_owner_id: int | None = None,
) -> None:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    master = get_master_by_id(master_id, demo_owner_id=owner_id)
    await send_static_screen(
        chat_id,
        format_master_detail_text(master_id, demo_owner_id=owner_id),
        reply_markup=admin_master_detail_kb(master_id, demo_owner_id=owner_id),
    )
    await send_master_preview(chat_id, master)


async def refresh_admin_master_detail_callback(
    callback: CallbackQuery,
    master_id: int,
    demo_owner_id: int | None = None,
) -> None:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    master = get_master_by_id(master_id, demo_owner_id=owner_id)
    await update_static_screen_from_callback(
        callback,
        format_master_detail_text(master_id, demo_owner_id=owner_id),
        reply_markup=admin_master_detail_kb(master_id, demo_owner_id=owner_id),
    )
    await send_master_preview(callback.from_user.id, master)


def format_portfolio_category_admin_text(
    category: str,
    demo_owner_id: int | None = None,
    master_id: int | None = None,
) -> str:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    target_master_id = int(master_id or 0)
    row = get_portfolio_category_by_code(category, demo_owner_id=owner_id, master_id=target_master_id)
    if row is None:
        return "🖼 Категория пока недоступна."
    master = get_master_by_id(target_master_id, demo_owner_id=owner_id)
    master_name = str(master["name"]) if master is not None else DEFAULT_MASTER_NAME
    title = str(row["title"])
    status = "Активна" if int(row["is_active"]) == 1 else "Скрыта"
    position = get_portfolio_category_sort_position(category, demo_owner_id=owner_id, master_id=target_master_id)
    items = get_portfolio_items(category=category, active_only=False, demo_owner_id=owner_id, master_id=target_master_id)
    lines = [f"🖼 {title}", "", f"👤 Специалист: {master_name}", f"👁 Статус: {status}"]
    if position is not None:
        lines.append(f"↕️ Позиция: {position[0]} из {position[1]}")
    lines.append(f"📸 Фото: {len(items)}")
    lines.append("")
    if not items:
        lines.append("Категория пока пустая.")
        lines.append("Добавьте первые фото ниже.")
    else:
        lines.append("Добавляйте, скрывайте и удаляйте фото через кнопки ниже.")
    return "\n".join(lines)

def push_screen_history(chat_id: int, text: str, reply_markup=None) -> None:
    current = CURRENT_SCREEN.get(chat_id)
    if current is not None and current[0] != text:
        history = SCREEN_HISTORY.setdefault(chat_id, [])
        history.append(current)
        if len(history) > 30:
            history.pop(0)
    CURRENT_SCREEN[chat_id] = (text, reply_markup)


def clear_screen_history(chat_id: int) -> None:
    SCREEN_HISTORY.pop(chat_id, None)
    CURRENT_SCREEN.pop(chat_id, None)


def track_aux_message(chat_id: int, message_id: int) -> None:
    ids = AUX_MESSAGE_IDS.setdefault(chat_id, [])
    if message_id not in ids:
        ids.append(message_id)
    if len(ids) > 25:
        del ids[:-25]


def set_reply_keyboard_active(chat_id: int, active: bool) -> None:
    if active:
        REPLY_KEYBOARD_ACTIVE[chat_id] = True
    else:
        REPLY_KEYBOARD_ACTIVE.pop(chat_id, None)


def is_reply_keyboard_active(chat_id: int) -> bool:
    return bool(REPLY_KEYBOARD_ACTIVE.get(chat_id))


async def clear_aux_messages(chat_id: int) -> None:
    if bot_instance is None:
        return
    ids = list(AUX_MESSAGE_IDS.get(chat_id, []))
    preview_id = PORTFOLIO_PREVIEW_MESSAGE_IDS.pop(chat_id, None)
    if preview_id is not None and preview_id not in ids:
        ids.append(preview_id)
    if not ids:
        return
    for message_id in ids:
        try:
            await bot_instance.delete_message(chat_id=chat_id, message_id=message_id)
        except Exception:
            pass
    AUX_MESSAGE_IDS.pop(chat_id, None)


def resolve_local_media_path(media_ref: str) -> str | None:
    value = (media_ref or "").strip()
    if not value:
        return None
    candidate = value if os.path.isabs(value) else os.path.join(PROJECT_ROOT, value)
    if os.path.exists(candidate):
        return candidate
    return None


def build_photo_input(media_ref: str) -> str | FSInputFile:
    local_path = resolve_local_media_path(media_ref)
    if local_path:
        return FSInputFile(local_path)
    return media_ref


MOJIBAKE_SEGMENT_RE = re.compile(r"[\u00A0-\u04FF\u2010-\u203F\u20AC\u2116\u2122 ]{2,}")


def _repair_mojibake_segment(segment: str) -> str:
    try:
        fixed = segment.encode("cp1251").decode("utf-8")
    except Exception:
        return segment
    return fixed if fixed else segment


def normalize_display_text(value: str | None) -> str | None:
    if value is None or not isinstance(value, str) or not value:
        return value

    text = value
    for _ in range(2):
        repaired = MOJIBAKE_SEGMENT_RE.sub(lambda m: _repair_mojibake_segment(m.group(0)), text)
        if repaired == text:
            break
        text = repaired
    return text


def normalize_reply_markup(reply_markup):
    if reply_markup is None:
        return None

    if isinstance(reply_markup, InlineKeyboardMarkup):
        rows: list[list[InlineKeyboardButton]] = []
        for row in reply_markup.inline_keyboard:
            new_row: list[InlineKeyboardButton] = []
            for button in row:
                data = button.model_dump(exclude_none=True)
                data["text"] = normalize_display_text(str(data.get("text", "")))
                new_row.append(InlineKeyboardButton(**data))
            rows.append(new_row)
        return InlineKeyboardMarkup(inline_keyboard=rows)

    if isinstance(reply_markup, ReplyKeyboardMarkup):
        rows: list[list[KeyboardButton]] = []
        for row in reply_markup.keyboard:
            new_row: list[KeyboardButton] = []
            for button in row:
                data = button.model_dump(exclude_none=True)
                data["text"] = normalize_display_text(str(data.get("text", "")))
                new_row.append(KeyboardButton(**data))
            rows.append(new_row)
        return ReplyKeyboardMarkup(
            keyboard=rows,
            resize_keyboard=reply_markup.resize_keyboard,
            one_time_keyboard=reply_markup.one_time_keyboard,
            selective=reply_markup.selective,
            input_field_placeholder=normalize_display_text(reply_markup.input_field_placeholder),
            is_persistent=reply_markup.is_persistent,
        )

    return reply_markup


def patch_bot_text_output(bot: Bot) -> Bot:
    original_send_message = bot.send_message
    original_edit_message_text = bot.edit_message_text
    original_send_photo = bot.send_photo
    original_send_document = bot.send_document
    original_send_invoice = bot.send_invoice

    async def send_message(*args, **kwargs):
        if "text" in kwargs:
            kwargs["text"] = normalize_display_text(kwargs["text"])
        if "reply_markup" in kwargs:
            kwargs["reply_markup"] = normalize_reply_markup(kwargs["reply_markup"])
        return await original_send_message(*args, **kwargs)

    async def edit_message_text(*args, **kwargs):
        if "text" in kwargs:
            kwargs["text"] = normalize_display_text(kwargs["text"])
        if "reply_markup" in kwargs:
            kwargs["reply_markup"] = normalize_reply_markup(kwargs["reply_markup"])
        return await original_edit_message_text(*args, **kwargs)

    async def send_photo(*args, **kwargs):
        if "caption" in kwargs:
            kwargs["caption"] = normalize_display_text(kwargs["caption"])
        if "reply_markup" in kwargs:
            kwargs["reply_markup"] = normalize_reply_markup(kwargs["reply_markup"])
        return await original_send_photo(*args, **kwargs)

    async def send_document(*args, **kwargs):
        if "caption" in kwargs:
            kwargs["caption"] = normalize_display_text(kwargs["caption"])
        if "reply_markup" in kwargs:
            kwargs["reply_markup"] = normalize_reply_markup(kwargs["reply_markup"])
        return await original_send_document(*args, **kwargs)

    async def send_invoice(*args, **kwargs):
        if "title" in kwargs:
            kwargs["title"] = normalize_display_text(kwargs["title"])
        if "description" in kwargs:
            kwargs["description"] = normalize_display_text(kwargs["description"])
        if "reply_markup" in kwargs:
            kwargs["reply_markup"] = normalize_reply_markup(kwargs["reply_markup"])
        return await original_send_invoice(*args, **kwargs)

    bot.send_message = send_message
    bot.edit_message_text = edit_message_text
    bot.send_photo = send_photo
    bot.send_document = send_document
    bot.send_invoice = send_invoice
    return bot


_ORIGINAL_CALLBACK_ANSWER = CallbackQuery.answer
_ORIGINAL_MESSAGE_ANSWER = Message.answer


async def _patched_callback_answer(self, *args, **kwargs):
    args_list = list(args)
    if args_list and isinstance(args_list[0], str):
        args_list[0] = normalize_display_text(args_list[0]) or ""
    if "text" in kwargs:
        kwargs["text"] = normalize_display_text(kwargs["text"])
    return await _ORIGINAL_CALLBACK_ANSWER(self, *tuple(args_list), **kwargs)


async def _patched_message_answer(self, *args, **kwargs):
    args_list = list(args)
    if args_list and isinstance(args_list[0], str):
        args_list[0] = normalize_display_text(args_list[0]) or ""
    if "text" in kwargs:
        kwargs["text"] = normalize_display_text(kwargs["text"])
    if "reply_markup" in kwargs:
        kwargs["reply_markup"] = normalize_reply_markup(kwargs["reply_markup"])
    return await _ORIGINAL_MESSAGE_ANSWER(self, *tuple(args_list), **kwargs)


CallbackQuery.answer = _patched_callback_answer
Message.answer = _patched_message_answer


async def send_master_preview(chat_id: int, master_row: sqlite3.Row | None) -> None:
    if bot_instance is None or master_row is None:
        return
    photo = (master_row["photo"] or "").strip() if "photo" in master_row.keys() else ""
    if not photo:
        return
    try:
        msg = await bot_instance.send_photo(
            chat_id=chat_id,
            photo=build_photo_input(photo),
            disable_notification=True,
        )
        track_aux_message(chat_id, msg.message_id)
    except Exception:
        pass


async def show_or_update_portfolio_preview(chat_id: int, photo_url: str) -> None:
    if bot_instance is None or not photo_url.strip():
        return

    photo_input = build_photo_input(photo_url)

    preview_message_id = PORTFOLIO_PREVIEW_MESSAGE_IDS.get(chat_id)
    if preview_message_id is not None:
        try:
            await bot_instance.edit_message_media(
                chat_id=chat_id,
                message_id=preview_message_id,
                media=InputMediaPhoto(media=photo_input),
            )
            return
        except Exception:
            try:
                await bot_instance.delete_message(chat_id=chat_id, message_id=preview_message_id)
            except Exception:
                pass
            PORTFOLIO_PREVIEW_MESSAGE_IDS.pop(chat_id, None)

    try:
        msg = await bot_instance.send_photo(
            chat_id=chat_id,
            photo=photo_input,
            disable_notification=True,
        )
        PORTFOLIO_PREVIEW_MESSAGE_IDS[chat_id] = msg.message_id
    except Exception:
        pass


async def send_static_screen(
    chat_id: int,
    text: str,
    reply_markup=None,
    track_history: bool = True,
    clear_aux: bool = True,
) -> Message:
    if bot_instance is None:
        raise RuntimeError("Бот не инициализирован")

    text = normalize_display_text(text) or ""
    reply_markup = normalize_reply_markup(reply_markup)

    if clear_aux:
        await clear_aux_messages(chat_id)

    if track_history:
        push_screen_history(chat_id, text, reply_markup)
    else:
        CURRENT_SCREEN[chat_id] = (text, reply_markup)

    msg = await bot_instance.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup)
    if isinstance(reply_markup, ReplyKeyboardMarkup):
        set_reply_keyboard_active(chat_id, True)
    old_ids = LAST_SCREEN_MESSAGE_IDS.get(chat_id, [])
    LAST_SCREEN_MESSAGE_IDS[chat_id] = [msg.message_id]
    for msg_id in old_ids:
        if msg_id == msg.message_id:
            continue
        try:
            await bot_instance.delete_message(chat_id=chat_id, message_id=msg_id)
        except Exception:
            pass
    return msg


async def try_delete_user_message(message: Message) -> None:
    try:
        await message.delete()
    except Exception:
        pass


async def delete_message_later(chat_id: int, message_id: int, delay: float = 1.2) -> None:
    if bot_instance is None:
        return
    try:
        await asyncio.sleep(delay)
        await bot_instance.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass


async def hide_reply_keyboard(chat_id: int) -> None:
    if bot_instance is None:
        return
    try:
        marker = await bot_instance.send_message(
            chat_id=chat_id,
            text="\u2063",
            reply_markup=ReplyKeyboardRemove(),
            disable_notification=True,
        )
        set_reply_keyboard_active(chat_id, False)
        await bot_instance.delete_message(chat_id=chat_id, message_id=marker.message_id)
    except Exception:
        pass


async def update_current_static_screen(
    chat_id: int,
    text: str,
    reply_markup=None,
    track_history: bool = True,
    clear_aux: bool = True,
) -> Message | None:
    if bot_instance is None:
        raise RuntimeError("Бот не инициализирован")

    text = normalize_display_text(text) or ""
    reply_markup = normalize_reply_markup(reply_markup)

    if clear_aux:
        await clear_aux_messages(chat_id)

    if track_history:
        push_screen_history(chat_id, text, reply_markup)
    else:
        CURRENT_SCREEN[chat_id] = (text, reply_markup)

    last_message_ids = LAST_SCREEN_MESSAGE_IDS.get(chat_id, [])
    if len(last_message_ids) != 1:
        await send_static_screen(
            chat_id,
            text,
            reply_markup=reply_markup,
            track_history=False,
            clear_aux=False,
        )
        return None

    message_id = last_message_ids[0]
    try:
        await bot_instance.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            reply_markup=reply_markup,
        )
        LAST_SCREEN_MESSAGE_IDS[chat_id] = [message_id]
        return None
    except Exception:
        msg = await send_static_screen(
            chat_id,
            text,
            reply_markup=reply_markup,
            track_history=False,
            clear_aux=False,
        )
        return msg


async def send_static_screen_hiding_keyboard(
    chat_id: int,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
    track_history: bool = True,
    clear_aux: bool = True,
) -> Message:
    if bot_instance is None:
        raise RuntimeError("Бот не инициализирован")

    text = normalize_display_text(text) or ""
    reply_markup = normalize_reply_markup(reply_markup)

    if clear_aux:
        await clear_aux_messages(chat_id)

    if track_history:
        push_screen_history(chat_id, text, reply_markup)
    else:
        CURRENT_SCREEN[chat_id] = (text, reply_markup)

    msg = await bot_instance.send_message(
        chat_id=chat_id,
        text=text,
        reply_markup=ReplyKeyboardRemove(),
    )
    set_reply_keyboard_active(chat_id, False)

    if reply_markup is not None:
        try:
            await bot_instance.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=msg.message_id,
                reply_markup=reply_markup,
            )
        except Exception:
            try:
                await bot_instance.edit_message_text(
                    chat_id=chat_id,
                    message_id=msg.message_id,
                    text=text,
                    reply_markup=reply_markup,
                )
            except Exception:
                try:
                    await bot_instance.delete_message(chat_id=chat_id, message_id=msg.message_id)
                except Exception:
                    pass
                return await send_static_screen(
                    chat_id,
                    text,
                    reply_markup=reply_markup,
                    track_history=False,
                    clear_aux=False,
                )

    old_ids = LAST_SCREEN_MESSAGE_IDS.get(chat_id, [])
    LAST_SCREEN_MESSAGE_IDS[chat_id] = [msg.message_id]
    for msg_id in old_ids:
        if msg_id == msg.message_id:
            continue
        try:
            await bot_instance.delete_message(chat_id=chat_id, message_id=msg_id)
        except Exception:
            pass
    return msg


async def send_temporary_prompt(
    chat_id: int,
    text: str,
    reply_markup=None,
    clear_existing: bool = True,
) -> Message:
    if bot_instance is None:
        raise RuntimeError("Бот не инициализирован")
    if clear_existing:
        await clear_aux_messages(chat_id)
    msg = await bot_instance.send_message(
        chat_id=chat_id,
        text=text,
        reply_markup=reply_markup,
    )
    if isinstance(reply_markup, ReplyKeyboardMarkup):
        set_reply_keyboard_active(chat_id, True)
    track_aux_message(chat_id, msg.message_id)
    return msg


async def ack_callback(callback: CallbackQuery) -> None:
    try:
        await callback.answer()
    except Exception:
        pass


def booking_back_target(user_id: int | None) -> tuple[str, str]:
    if user_id is not None and get_user_role(int(user_id)) == "demo_buyer":
        return "demo:path:menu", "⬅️ К клиентскому пути"
    return "nav:main_menu", "⬅️ Назад в меню"


async def update_static_screen_from_callback(
    callback: CallbackQuery,
    text: str,
    reply_markup=None,
    track_history: bool = True,
    clear_aux: bool = True,
) -> None:
    chat_id = callback.from_user.id
    text = normalize_display_text(text) or ""
    reply_markup = normalize_reply_markup(reply_markup)
    can_edit_current_message = callback.message is not None and (
        reply_markup is None or isinstance(reply_markup, InlineKeyboardMarkup)
    )
    if clear_aux:
        await clear_aux_messages(chat_id)

    if track_history:
        push_screen_history(chat_id, text, reply_markup)
    else:
        CURRENT_SCREEN[chat_id] = (text, reply_markup)

    if not can_edit_current_message:
        await send_static_screen(
            chat_id,
            text,
            reply_markup=reply_markup,
            track_history=False,
            clear_aux=False,
        )
        return

    try:
        last_message_ids = LAST_SCREEN_MESSAGE_IDS.get(chat_id, [])
        for msg_id in last_message_ids:
            if msg_id == callback.message.message_id:
                continue
            try:
                await callback.bot.delete_message(chat_id=chat_id, message_id=msg_id)
            except Exception:
                pass

        await callback.message.edit_text(text, reply_markup=reply_markup)
        LAST_SCREEN_MESSAGE_IDS[chat_id] = [callback.message.message_id]
    except Exception:
        await send_static_screen(
            chat_id,
            text,
            reply_markup=reply_markup,
            track_history=False,
            clear_aux=False,
        )


async def render_inline_screen(
    chat_id: int,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
    track_history: bool = True,
    clear_aux: bool = True,
    prefer_edit: bool = True,
) -> Message | None:
    if is_reply_keyboard_active(chat_id):
        return await send_static_screen_hiding_keyboard(
            chat_id,
            text,
            reply_markup=reply_markup,
            track_history=track_history,
            clear_aux=clear_aux,
        )

    if prefer_edit:
        return await update_current_static_screen(
            chat_id,
            text,
            reply_markup=reply_markup,
            track_history=track_history,
            clear_aux=clear_aux,
        )

    return await send_static_screen(
        chat_id,
        text,
        reply_markup=reply_markup,
        track_history=track_history,
        clear_aux=clear_aux,
    )


async def render_inline_screen_from_callback(
    callback: CallbackQuery,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
    track_history: bool = True,
    clear_aux: bool = True,
) -> None:
    await update_static_screen_from_callback(
        callback,
        text,
        reply_markup=reply_markup,
        track_history=track_history,
        clear_aux=clear_aux,
    )


async def render_input_step(
    user_id: int,
    screen_text: str,
    screen_markup: InlineKeyboardMarkup,
    prompt_text: str,
    prompt_markup: ReplyKeyboardMarkup,
    callback: CallbackQuery | None = None,
) -> None:
    if callback is not None:
        await render_inline_screen_from_callback(
            callback,
            screen_text,
            reply_markup=screen_markup,
        )
    else:
        await render_inline_screen(
            user_id,
            screen_text,
            reply_markup=screen_markup,
        )

    await send_temporary_prompt(
        user_id,
        prompt_text,
        reply_markup=prompt_markup,
    )


# =====================================================
# 5) Клавиатуры (Reply + Inline)
# =====================================================
class DemoOwnerContextMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        user = getattr(event, "from_user", None)
        demo_owner_id = OWNER_ID_INT
        if user is not None and getattr(user, "id", None) is not None:
            demo_owner_id = resolve_demo_owner_id_for_user(int(user.id))
            ensure_demo_workspace(demo_owner_id)

        token = CURRENT_DEMO_OWNER_ID.set(int(demo_owner_id))
        try:
            return await handler(event, data)
        finally:
            CURRENT_DEMO_OWNER_ID.reset(token)


def demo_buyer_home_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🧭 Посмотреть путь клиента", callback_data="demo:path:menu")],
            [InlineKeyboardButton(text="⚙️ Внутри для мастера", callback_data="lead:open")],
            [InlineKeyboardButton(text="💳 Тарифы и запуск", callback_data="lead:sale_tariffs")],
            [InlineKeyboardButton(text="💬 Обсудить запуск", callback_data="sale:contact")],
        ]
    )


def buyer_home_text() -> str:
    return (
        "✨ Telegram-бот для beauty-мастера или студии\n\n"
        "Здесь можно за пару минут увидеть, как выглядит запись для клиента, как устроено управление и какой формат запуска подойдёт именно вам.\n\n"
        "Лучше всего начать с клиентского пути."
    )


def manager_home_text() -> str:
    return (
        "🎯 Режим менеджера\n\n"
        "Здесь только лиды и ваша lead-ссылка."
    )


def manager_home_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🎯 Лиды", callback_data="leadmgr:open")],
            [InlineKeyboardButton(text="🔗 Моя lead-ссылка", callback_data="leadmgr:link")],
        ]
    )


def admin_home_text() -> str:
    return (
        "⚙️ Рабочая панель\n\n"
        "Сверху — клиентские сценарии, ниже — рабочие разделы."
    )


def admin_home_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🗓 Запись", callback_data="home:booking")],
            [InlineKeyboardButton(text="📜 Прайс", callback_data="home:price")],
            [InlineKeyboardButton(text="📸 Портфолио", callback_data="home:portfolio")],
            [InlineKeyboardButton(text="💳 Тарифы и запуск", callback_data="home:sale")],
            [InlineKeyboardButton(text="⚙️ Рабочая панель", callback_data="admin:panel")],
        ]
    )


def owner_home_text() -> str:
    return (
        "👑 Панель владельца\n\n"
        "Здесь собраны демо, продажи, лиды и рабочие разделы."
    )


def owner_home_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🗓 Запись", callback_data="home:booking")],
            [InlineKeyboardButton(text="📜 Прайс", callback_data="home:price")],
            [InlineKeyboardButton(text="📸 Портфолио", callback_data="home:portfolio")],
            [InlineKeyboardButton(text="💳 Тарифы и запуск", callback_data="home:sale")],
            [InlineKeyboardButton(text="⚙️ Рабочая панель", callback_data="admin:panel")],
            [InlineKeyboardButton(text="🎯 Лиды", callback_data="leadmgr:open")],
            [InlineKeyboardButton(text="👥 Администраторы", callback_data="home:admins")],
        ]
    )


def role_home_text(user_id: int) -> str:
    role = get_user_role(user_id)
    if role == "manager":
        return manager_home_text()
    if role == "demo_buyer":
        return buyer_home_text()
    if role == "admin":
        return admin_home_text()
    return owner_home_text()


def main_menu_kb(user_id: int) -> ReplyKeyboardMarkup | InlineKeyboardMarkup:
    role = get_user_role(user_id)

    if role == "manager":
        return manager_home_kb()

    if role == "demo_buyer":
        return demo_buyer_home_kb()

    if role == "admin":
        return admin_home_kb()

    return owner_home_kb()


def phone_request_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📱 Взять номер из Telegram", request_contact=True)],
            [KeyboardButton(text="⬅️ Назад"), KeyboardButton(text="Отмена")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
        is_persistent=False,
    )


def booking_masters_kb(user_id: int) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    demo_owner_id = resolve_demo_owner_id_for_user(user_id)
    for master in get_active_masters(demo_owner_id=demo_owner_id):
        specialization = shorten_text(get_master_specialization_from_row(master), limit=22)
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"👤 {master['name']} · {specialization}",
                    callback_data=f"master_card:booking:{master['id']}",
                )
            ]
        )
    if not rows:
        rows.append([InlineKeyboardButton(text="Мастеров пока нет", callback_data="cal_ignore")])
    back_callback, back_text = booking_back_target(user_id)
    rows.append([InlineKeyboardButton(text=back_text, callback_data=back_callback)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def services_kb(
    user_id: int | None = None,
    master_id: int | None = None,
    back_callback: str | None = None,
    back_text: str | None = None,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    demo_owner_id = resolve_demo_owner_id_for_user(user_id) if user_id is not None else None
    target_master_id = int(master_id or 0)
    for service in get_active_services(demo_owner_id=demo_owner_id, master_id=target_master_id):
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{service['name']} - {service['price']} USD",
                    callback_data=f"service:{service['id']}",
                )
            ]
        )
    if not rows:
        rows.append([InlineKeyboardButton(text="Услуг пока нет", callback_data="cal_ignore")])
    resolved_back_callback, resolved_back_text = booking_back_target(user_id)
    rows.append(
        [
            InlineKeyboardButton(
                text=back_text or resolved_back_text,
                callback_data=back_callback or resolved_back_callback,
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def portfolio_categories_kb(
    user_id: int | None = None,
    master_id: int | None = None,
    back_callback: str | None = None,
    back_text: str | None = None,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    demo_owner_id = resolve_demo_owner_id_for_user(user_id) if user_id is not None else None
    target_master_id = int(master_id or 0)
    for row in get_portfolio_categories(active_only=True, demo_owner_id=demo_owner_id, master_id=target_master_id):
        rows.append(
            [
                InlineKeyboardButton(
                    text=str(row["title"]),
                    callback_data=f"portfolio:{target_master_id}:{row['code']}",
                )
            ]
        )
    if not rows:
        rows.append([InlineKeyboardButton(text="Категорий пока нет", callback_data="cal_ignore")])
    back_cb = back_callback or "nav:main_menu"
    back_label = back_text or "⬅️ Назад в меню"
    rows.append([InlineKeyboardButton(text=back_label, callback_data=back_cb)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def portfolio_master_select_kb(user_id: int, from_client_path: bool = False) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    demo_owner_id = resolve_demo_owner_id_for_user(user_id)
    context = "portfolio_demo" if from_client_path else "portfolio_menu"
    for master in get_active_masters(demo_owner_id=demo_owner_id):
        specialization = shorten_text(get_master_specialization_from_row(master), limit=22)
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"👤 {master['name']} · {specialization}",
                    callback_data=f"master_card:{context}:{master['id']}",
                )
            ]
        )
    if not rows:
        rows.append([InlineKeyboardButton(text="Мастеров пока нет", callback_data="cal_ignore")])
    rows.append(
        [
            InlineKeyboardButton(
                text="⬅️ К клиентскому пути" if from_client_path else "⬅️ Назад в меню",
                callback_data="demo:path:menu" if from_client_path else "nav:main_menu",
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def master_card_kb(user_id: int, master_id: int, context: str) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if context == "booking":
        rows.append([InlineKeyboardButton(text="✅ Выбрать мастера", callback_data=f"booking_master:{master_id}")])
        rows.append([InlineKeyboardButton(text="⬅️ К списку мастеров", callback_data="master_card_back:booking")])
    elif context == "portfolio_demo":
        rows.append([InlineKeyboardButton(text="🖼 Открыть портфолио", callback_data=f"portfolio_master:{master_id}")])
        rows.append([InlineKeyboardButton(text="🗓 Записаться", callback_data=f"booking_master:{master_id}")])
        rows.append([InlineKeyboardButton(text="⬅️ К списку мастеров", callback_data="master_card_back:portfolio_demo")])
    else:
        rows.append([InlineKeyboardButton(text="🖼 Открыть портфолио", callback_data=f"portfolio_master:{master_id}")])
        rows.append([InlineKeyboardButton(text="🗓 Записаться", callback_data=f"booking_master:{master_id}")])
        rows.append([InlineKeyboardButton(text="⬅️ К списку мастеров", callback_data="master_card_back:portfolio_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def demo_client_path_back_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ К клиентскому пути", callback_data="demo:path:menu")],
            [InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu")],
        ]
    )


def admin_panel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📋 Записи", callback_data="admin:section:appointments")],
            [InlineKeyboardButton(text="🪟 Окошки", callback_data="admin:slots")],
            [InlineKeyboardButton(text="🗂 Контент", callback_data="admin:section:content")],
            [InlineKeyboardButton(text="📢 Коммуникации", callback_data="admin:section:comms")],
            [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="nav:main_menu")],
        ]
    )


def admin_appointments_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📅 Сегодня", callback_data="admin:today")],
            [InlineKeyboardButton(text="🗓 Будущие записи", callback_data="admin:all")],
            [InlineKeyboardButton(text="❌ Отменить запись", callback_data="admin:cancel")],
            [InlineKeyboardButton(text="⬅️ Назад в админку", callback_data="admin:panel")],
            [InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu")],
        ]
    )


def admin_content_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="👥 Мастера", callback_data="admin:masters")],
            [InlineKeyboardButton(text="🧾 Услуги", callback_data="admin:services")],
            [InlineKeyboardButton(text="🖼 Портфолио", callback_data="admin:portfolio")],
            [InlineKeyboardButton(text="⬅️ Назад в админку", callback_data="admin:panel")],
            [InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu")],
        ]
    )


def admin_comms_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin:broadcast")],
            [InlineKeyboardButton(text="⬅️ Назад в админку", callback_data="admin:panel")],
            [InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu")],
        ]
    )


def demo_admin_panel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⚙️ Открыть демо-админку", callback_data="lead:live_admin")],
            [InlineKeyboardButton(text="✨ Что умеет бот", callback_data="lead:sale_inside")],
            [InlineKeyboardButton(text="💳 Тарифы", callback_data="lead:sale_tariffs")],
            [InlineKeyboardButton(text="🧩 Собрать тариф", callback_data="sale:open")],
            [InlineKeyboardButton(text="📅 Записи на сегодня", callback_data="lead:today")],
            [InlineKeyboardButton(text="🗓 Будущие записи", callback_data="lead:all")],
            [InlineKeyboardButton(text="🧾 Услуги", callback_data="lead:services")],
            [InlineKeyboardButton(text="🖼 Портфолио", callback_data="admin:portfolio")],
            [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="nav:main_menu")],
        ]
    )


def demo_client_path_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🗓 Попробовать запись", callback_data="demo:path:booking")],
            [InlineKeyboardButton(text="📋 Посмотреть прайс", callback_data="demo:path:price")],
            [InlineKeyboardButton(text="🖼 Посмотреть портфолио", callback_data="demo:path:portfolio")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:back")],
            [InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu")],
        ]
    )


def build_demo_feature_text(feature_code: str) -> str:
    feature = DEMO_FEATURES.get(feature_code)
    if feature is None:
        return "Блок демо пока недоступен."
    return (
        f"{feature['title']}\n\n"
        f"{feature['description']}\n\n"
        "✨ Что это даёт в работе:\n"
        "• меньше ручной переписки и потерь в диалоге\n"
        "• понятную структуру записи и контента\n"
        "• более аккуратный опыт для клиента"
    )


def build_demo_overview_text() -> str:
    return (
        "⚙️ Внутри для мастера\n\n"
        "Здесь можно посмотреть, как мастер управляет записью, услугами, портфолио и ежедневной работой без хаоса в переписке.\n\n"
        "Если хотите быстро понять ценность продукта, начните с блока «Что умеет бот»."
    )


def demo_sale_scenario_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✨ Что умеет этот бот", callback_data="lead:sale_inside")],
            [InlineKeyboardButton(text="💳 Тарифы", callback_data="lead:sale_tariffs")],
            [InlineKeyboardButton(text="🚀 Как проходит запуск", callback_data="sale:next_steps")],
            [InlineKeyboardButton(text="🧩 Собрать свой тариф", callback_data="sale:open")],
            [InlineKeyboardButton(text="💬 Обсудить запуск", callback_data="sale:contact")],
            [InlineKeyboardButton(text="⚙️ К демо-админке", callback_data="lead:open")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:back")],
            [InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu")],
        ]
    )


def build_sale_inside_text() -> str:
    return (
        "✨ Что умеет этот бот\n\n"
        "Это не просто бот для записи.\n"
        "Это аккуратный рабочий инструмент для beauty-мастера или команды 💼\n\n"
        "С его помощью можно:\n"
        "• 🗓 принимать записи без лишней переписки\n"
        "• 💳 показывать услуги и цены в аккуратном формате\n"
        "• 🖼 вести портфолио прямо в боте\n"
        "• 👤👥 работать в формате solo или team\n"
        "• 🔔 напоминать клиентам о визитах\n"
        "• ⚙️ управлять контентом и записями в одной админке\n\n"
        "В итоге бот помогает сделать запись для клиента удобнее, "
        "а работу мастера спокойнее и понятнее ✨"
    )


def build_sale_tariffs_text() -> str:
    lite_price = format_payment_amount(int(SALE_TARIFFS["lite"]["price_minor"]))
    standard_price = format_payment_amount(int(SALE_TARIFFS["standard"]["price_minor"]))
    team_price = format_payment_amount(int(SALE_TARIFFS["pro"]["price_minor"]))
    lines = [
        "💳 Тарифы и запуск",
        "",
        "Выберите формат запуска под свой сценарий работы.",
        "",
        f"🟢 <b>Solo Start</b> — {lite_price}",
        "Для мягкого старта одного мастера.",
        "• готовая основа для старта",
        "• запись, услуги, портфолио и админка в одном продукте",
        "• подойдёт, если часть наполнения вы готовы сделать самостоятельно",
        "",
        f"⭐ <b>Solo Pro</b> — {standard_price}",
        "✨ <b>Рекомендуемый тариф</b> для одного мастера.",
        "• более полный запуск и меньше ручной подготовки",
        "• более собранная подача и спокойный старт без лишней суеты",
        "• лучший баланс между ценой, готовностью и удобством",
        "",
        f"👥 <b>Team</b> — {team_price}",
        "Для команды специалистов или студии.",
        "• выбор специалиста внутри одного бота",
        "• раздельные услуги, портфолио и записи по мастерам",
        "• настройка под командный формат работы",
        "",
        "☁️ Размещение и запуск подбираем отдельно под ваш формат работы.",
        "",
        "Чтобы увидеть итог под свои задачи, откройте «Собрать свой тариф».",
    ]
    return "\n".join(lines)


def build_sale_next_steps_text() -> str:
    return (
        "🚀 Как проходит запуск\n\n"
        "1. Вы выбираете подходящий формат запуска.\n"
        "2. Мы связываемся и уточняем, как именно вы работаете: solo или team.\n"
        "3. Спокойно согласуем структуру, контент и формат запуска.\n"
        "4. Подготавливаем бота под вашу нишу и ваш стиль работы.\n"
        "5. Передаём уже собранный рабочий инструмент.\n\n"
        "✨ Если удобнее, можно сначала всё обсудить и только потом переходить к оплате."
    )


def demo_feature_detail_kb(feature_code: str) -> InlineKeyboardMarkup:
    feature = DEMO_FEATURES.get(feature_code)
    if feature is None:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⚙️ К демо-админке", callback_data="lead:open")],
                [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="nav:main_menu")],
            ]
        )
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔎 Открыть в боте", callback_data=str(feature["open_callback"]))],
            [InlineKeyboardButton(text="⚙️ К демо-админке", callback_data="lead:open")],
            [InlineKeyboardButton(text="🧩 Собрать тариф", callback_data="sale:open")],
            [InlineKeyboardButton(text="💬 Обсудить запуск", callback_data="sale:contact")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:back")],
            [InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu")],
        ]
    )


async def open_demo_feature_preview(chat_id: int, feature_code: str) -> None:
    await clear_aux_messages(chat_id)

    await send_static_screen(
        chat_id,
        build_demo_feature_text(feature_code),
        reply_markup=demo_feature_detail_kb(feature_code),
        clear_aux=False,
    )


def lead_manager_panel_kb(user_id: int) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text="🎯 Список лидов", callback_data="leadmgr:list")],
        [InlineKeyboardButton(text="🔗 Моя lead-ссылка", callback_data="leadmgr:link")],
    ]
    if is_owner(user_id):
        rows.append([InlineKeyboardButton(text="➕ Добавить из базы", callback_data="leadmgr:add_menu")])
    rows.append([InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def lead_manager_add_select_kb(limit: int = 25) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    lead_ids = {int(row["user_id"]) for row in get_demo_leads(limit=300)}
    for row in get_recent_users(limit=limit * 2):
        candidate_user_id = int(row["user_id"])
        if candidate_user_id in lead_ids:
            continue
        if is_owner(candidate_user_id) or is_manager(candidate_user_id):
            continue
        display_name = str(row["full_name"] or candidate_user_id)
        username = f" (@{row['username']})" if row["username"] else ""
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"➕ Добавить: {display_name}{username}",
                    callback_data=f"leadmgr:add:{candidate_user_id}",
                )
            ]
        )
        if len(rows) >= limit:
            break

    if not rows:
        rows.append([InlineKeyboardButton(text="Список пока пуст", callback_data="cal_ignore")])
    rows.append([InlineKeyboardButton(text="🎯 К лидам", callback_data="leadmgr:open")])
    rows.append([InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def lead_manager_list_kb(viewer_id: int, limit: int = 25) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    assigned_by = None if is_owner(viewer_id) else viewer_id
    for row in get_demo_leads(limit=limit, assigned_by=assigned_by):
        lead_user_id = int(row["user_id"])
        name = str(row["full_name"] or lead_user_id)
        status = "Оплачен" if str(row["status"]) == "paid" else "Лид"
        if is_owner(viewer_id):
            rows.append(
                [
                    InlineKeyboardButton(
                        text=f"❌ Удалить: {name} ({status})",
                        callback_data=f"leadmgr:remove:{lead_user_id}",
                    )
                ]
            )
        else:
            rows.append(
                [
                    InlineKeyboardButton(
                        text=f"• {name} ({status})",
                        callback_data="cal_ignore",
                    )
                ]
            )

    if not rows:
        rows.append([InlineKeyboardButton(text="🎯 Лидов пока нет", callback_data="cal_ignore")])
    rows.append([InlineKeyboardButton(text="🎯 К лидам", callback_data="leadmgr:open")])
    rows.append([InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def format_payment_amount(amount_minor: int) -> str:
    return f"{amount_minor / 100:.2f} {PAYMENT_CURRENCY}"


def normalize_sale_selection(
    tariff_code: str | None,
    selected_options: set[str] | list[str] | tuple[str, ...] | None = None,
) -> tuple[str, list[str]]:
    normalized_tariff = tariff_code if tariff_code in SALE_TARIFFS else "lite"
    options_iterable = selected_options or []
    normalized_options = sorted({code for code in options_iterable if code in SALE_OPTIONS})
    return normalized_tariff, normalized_options


def calculate_sale_total_minor(tariff_code: str, selected_options: list[str]) -> int:
    tariff = SALE_TARIFFS[tariff_code]
    total = int(tariff["price_minor"])
    total += sum(int(SALE_OPTIONS[code]["price_minor"]) for code in selected_options)
    return total


def get_sale_tariff_audience_text(tariff_code: str) -> str:
    mapping = {
        "lite": "Подходит, если хотите спокойно стартовать с готовой основой.",
        "standard": "Подходит, если хотите более собранный запуск и меньше ручной подготовки.",
        "pro": "Подходит, если у вас несколько специалистов и нужен один общий бот под команду.",
    }
    return mapping.get(tariff_code, "")


def build_sale_tariff_picker_text(tariff_code: str) -> str:
    tariff = SALE_TARIFFS[tariff_code]
    base_minor = int(tariff["price_minor"])
    lines = [
        "🧩 Сборка тарифа",
        "",
        "Шаг 1 из 2. Выберите базовую конфигурацию.",
        "",
        f"Основа: <b>{tariff['title']}</b> — {format_payment_amount(base_minor)}",
        "⭐ Это самый сбалансированный выбор для старта." if tariff_code == "standard" else "",
        get_sale_tariff_audience_text(tariff_code),
        f"Что входит: {tariff['description']}",
        "",
        "После выбора вы сразу перейдёте к дополнительным услугам. Их можно спокойно пропустить.",
    ]
    return "\n".join([line for line in lines if line])


def build_sale_constructor_text(tariff_code: str, selected_options: list[str]) -> str:
    tariff = SALE_TARIFFS[tariff_code]
    base_minor = int(tariff["price_minor"])
    total_minor = calculate_sale_total_minor(tariff_code, selected_options)

    lines = [
        "✨ Дополнительные услуги",
        "",
        "Шаг 2 из 2. При желании добавьте услуги к выбранной конфигурации.",
        "",
        f"Конфигурация: <b>{tariff['title']}</b> — {format_payment_amount(base_minor)}",
    ]
    if tariff_code == "standard":
        lines.append("⭐ Это рекомендуемый тариф.")

    if selected_options:
        lines.append("")
        lines.append("✨ Уже добавлено:")
        for code in selected_options:
            option = SALE_OPTIONS[code]
            lines.append(f"• {option['title']} (+{format_payment_amount(int(option['price_minor']))})")
            lines.append(f"  {option['description']}")
    else:
        lines.extend(
            [
                "",
                "🌿 Пока без дополнительных услуг.",
                "Можно оставить запуск в таком виде или усилить его нужными опциями ниже.",
            ]
        )

    lines.extend(
        [
            "",
            f"💳 Итог: {format_payment_amount(total_minor)}",
            "☁️ Хостинг и размещение подберём отдельно, без лишней технички.",
            "",
            "Дальше можно спокойно перейти к оплате или сначала обсудить детали запуска.",
        ]
    )
    return "\n".join(lines)


def sale_tariff_picker_kb(selected_tariff: str) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for code, data in SALE_TARIFFS.items():
        mark = "✅ " if code == selected_tariff else ""
        spotlight = "⭐ " if code == "standard" else ""
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{mark}{spotlight}{data['title']} · {format_payment_amount(int(data['price_minor']))}",
                    callback_data=f"sale:set:{code}",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="🚀 Как проходит запуск", callback_data="sale:next_steps")])
    rows.append([InlineKeyboardButton(text="💬 Обсудить запуск", callback_data="sale:contact")])
    rows.append([InlineKeyboardButton(text="⬅️ К тарифам", callback_data="lead:sale_tariffs")])
    rows.append([InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def sales_builder_kb(selected_tariff: str, selected_options: list[str]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    selected_options_set = set(selected_options)
    option_labels = {
        "extra_revision": "🪄 Раунд правок",
        "photos_10": "🖼 +10 фото",
        "data_migration": "🔄 Перенос данных",
        "extra_language": "🌍 Второй язык",
    }

    option_buttons: list[InlineKeyboardButton] = []
    for code, data in SALE_OPTIONS.items():
        mark = "✅ " if code in selected_options_set else ""
        option_buttons.append(
            InlineKeyboardButton(
                text=f"{mark}{option_labels.get(code, data['title'])}",
                callback_data=f"sale:opt:{code}",
            )
        )

    for idx in range(0, len(option_buttons), 2):
        rows.append(option_buttons[idx:idx + 2])

    total_minor = calculate_sale_total_minor(selected_tariff, selected_options)
    continue_text = (
        f"✨ Продолжить с этим набором · {format_payment_amount(total_minor)}"
        if selected_options
        else f"✨ Без доп. услуг · {format_payment_amount(total_minor)}"
    )
    rows.append([InlineKeyboardButton(text=continue_text, callback_data="sale:invoice")])
    rows.append([InlineKeyboardButton(text="⬅️ К конфигурации", callback_data="sale:open")])
    rows.append([InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def sale_support_kb(back_callback: str = "sale:open") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💬 Связаться по запуску", url=f"tg://user?id={OWNER_ID_INT}")],
            [InlineKeyboardButton(text="🧩 Собрать свой тариф", callback_data="sale:open")],
            [InlineKeyboardButton(text="🚀 Как проходит запуск", callback_data="sale:next_steps")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=back_callback)],
            [InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu")],
        ]
    )


def mini_app_launch_kb() -> InlineKeyboardMarkup:
    if not MINI_APP_URL:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="nav:main_menu")],
            ]
        )
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📱 Открыть Mini App", web_app=WebAppInfo(url=MINI_APP_URL))],
            [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="nav:main_menu")],
        ]
    )


def admin_management_panel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📋 Список админов", callback_data="dev:show_admins")],
            [InlineKeyboardButton(text="➕ Добавить админа", callback_data="dev:add_admin")],
            [InlineKeyboardButton(text="➖ Удалить админа", callback_data="dev:remove_menu")],
            [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="nav:main_menu")],
        ]
    )


def admin_remove_select_kb(limit: int = 30) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    admin_ids = get_admin_ids()
    for uid in admin_ids[:limit]:
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"🗑 Удалить администратора {uid}",
                    callback_data=f"dev:remove_admin:{uid}",
                )
            ]
        )
    rows.append([InlineKeyboardButton(text="⬅️ К управлению админами", callback_data="dev:show_admins")])
    rows.append([InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_masters_kb(demo_owner_id: int | None = None) -> InlineKeyboardMarkup:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    mode = get_workspace_booking_mode(demo_owner_id=owner_id)
    masters = get_all_masters(demo_owner_id=owner_id)
    rows: list[list[InlineKeyboardButton]] = []
    rows.append(
        [
            InlineKeyboardButton(
                text="🧭 Формат: команда" if mode == WORKSPACE_MODE_TEAM else "🧍 Формат: один мастер",
                callback_data="adm_mode_toggle",
            )
        ]
    )
    for master in masters:
        status = "✅" if int(master["is_active"]) == 1 else "❌"
        primary_mark = "⭐ " if masters and int(master["id"]) == int(masters[0]["id"]) else ""
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{status} {primary_mark}{master['name']}",
                    callback_data=f"adm_master_open:{master['id']}",
                )
            ]
        )
    if not masters:
        rows.append([InlineKeyboardButton(text="Мастеров пока нет", callback_data="cal_ignore")])
    rows.append([InlineKeyboardButton(text="➕ Добавить мастера", callback_data="adm_master_add")])
    rows.append([InlineKeyboardButton(text="⬅️ К разделу «Контент»", callback_data="admin:section:content")])
    rows.append([InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_master_detail_kb(master_id: int, demo_owner_id: int | None = None) -> InlineKeyboardMarkup:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    master = get_master_by_id(master_id, demo_owner_id=owner_id)
    if master is None:
        return InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅️ К мастерам", callback_data="admin:masters")]]
        )
    active = int(master["is_active"]) == 1
    position = get_master_sort_position(master_id, demo_owner_id=owner_id)
    total = position[1] if position is not None else 1
    can_move_up = position is not None and position[0] > 1
    can_move_down = position is not None and position[0] < total
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="⭐ Сделать основным", callback_data=f"adm_master_primary:{master_id}"),
                InlineKeyboardButton(text="🪟 Окошки", callback_data=f"adm_slot_master:{master_id}"),
            ],
            [
                InlineKeyboardButton(text="✏️ Имя", callback_data=f"adm_master_rename:{master_id}"),
                InlineKeyboardButton(text="✨ Специализация", callback_data=f"adm_master_spec:{master_id}"),
            ],
            [
                InlineKeyboardButton(text="📝 Описание", callback_data=f"adm_master_desc:{master_id}"),
                InlineKeyboardButton(
                    text="🖼 Фото",
                    callback_data=f"adm_master_photo:{master_id}",
                ),
            ],
            *(
                [[InlineKeyboardButton(text="🗑 Убрать фото", callback_data=f"adm_master_photo_clear:{master_id}")]]
                if (master["photo"] or "").strip()
                else []
            ),
            [
                InlineKeyboardButton(
                    text="🙈 Скрыть" if active else "👁 Показать",
                    callback_data=f"adm_master_toggle:{master_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬆️ Выше",
                    callback_data=f"adm_master_move:up:{master_id}" if can_move_up else "cal_ignore",
                ),
                InlineKeyboardButton(
                    text="⬇️ Ниже",
                    callback_data=f"adm_master_move:down:{master_id}" if can_move_down else "cal_ignore",
                ),
            ],
            [InlineKeyboardButton(text="🗑 Удалить мастера", callback_data=f"adm_master_delete:{master_id}")],
            [
                InlineKeyboardButton(text="⬅️ К мастерам", callback_data="admin:masters"),
                InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu"),
            ],
        ]
    )


def admin_slot_master_select_kb(demo_owner_id: int | None = None) -> InlineKeyboardMarkup:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    rows: list[list[InlineKeyboardButton]] = []
    for master in get_active_masters(demo_owner_id=owner_id):
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"👤 {master['name']}",
                    callback_data=f"adm_slot_master:{master['id']}",
                )
            ]
        )
    if not rows:
        rows.append([InlineKeyboardButton(text="Мастеров пока нет", callback_data="cal_ignore")])
    rows.append([InlineKeyboardButton(text="⬅️ К админке", callback_data="admin:panel")])
    rows.append([InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_slots_kb(
    date_str: str,
    master_id: int,
    demo_owner_id: int | None = None,
) -> InlineKeyboardMarkup:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    allowed = set(get_allowed_slots(date_str, demo_owner_id=owner_id, master_id=master_id))
    overridden = set(get_slot_overrides(date_str, demo_owner_id=owner_id, master_id=master_id).keys())
    rows: list[list[InlineKeyboardButton]] = []
    for slot in sort_time_slots(list(set(ALL_TIME_SLOTS) | allowed | overridden)):
        status = "✅" if slot in allowed else "❌"
        token = slot.replace(":", "")
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{status} {slot}",
                    callback_data=f"adm_slot_toggle:{date_str}:{token}",
                )
            ]
        )

    date_closed = is_date_closed(date_str, demo_owner_id=owner_id, master_id=master_id)
    rows.append(
        [
            InlineKeyboardButton(
                text="✅ Открыть дату" if date_closed else "🚫 Закрыть дату",
                callback_data=f"adm_date_toggle:{date_str}",
            )
        ]
    )
    rows.append([InlineKeyboardButton(text="➕ Добавить время", callback_data=f"adm_slot_add:{date_str}")])
    rows.append([InlineKeyboardButton(text="♻️ Сбросить настройки даты", callback_data=f"adm_slot_reset:{date_str}")])
    rows.append([InlineKeyboardButton(text="⬅️ К календарю", callback_data="adm_slot_calendar_back")])
    if is_master_choice_enabled(demo_owner_id=owner_id):
        rows.append([InlineKeyboardButton(text="👤 Выбрать другого мастера", callback_data="admin:slots")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_service_master_select_kb(demo_owner_id: int | None = None) -> InlineKeyboardMarkup:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    rows: list[list[InlineKeyboardButton]] = []
    for master in get_active_masters(demo_owner_id=owner_id):
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"👤 {master['name']}",
                    callback_data=f"adm_svc_master:{master['id']}",
                )
            ]
        )
    if not rows:
        rows.append([InlineKeyboardButton(text="Мастеров пока нет", callback_data="cal_ignore")])
    rows.append([InlineKeyboardButton(text="⬅️ К разделу «Контент»", callback_data="admin:section:content")])
    rows.append([InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_services_kb(master_id: int, demo_owner_id: int | None = None) -> InlineKeyboardMarkup:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    rows: list[list[InlineKeyboardButton]] = []
    for service in get_all_services(demo_owner_id=owner_id, master_id=master_id):
        status = "✅" if int(service["is_active"]) == 1 else "❌"
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{status} {service['name']} · {service['price']} USD",
                    callback_data=f"adm_svc_open:{service['id']}",
                )
            ]
        )
    if not rows:
        rows.append([InlineKeyboardButton(text="Услуг пока нет", callback_data="cal_ignore")])
    rows.append([InlineKeyboardButton(text="➕ Добавить услугу", callback_data="adm_svc_add")])
    if is_master_choice_enabled(demo_owner_id=owner_id):
        rows.append([InlineKeyboardButton(text="👤 Выбрать другого мастера", callback_data="admin:services")])
    rows.append([InlineKeyboardButton(text="⬅️ К разделу «Контент»", callback_data="admin:section:content")])
    rows.append([InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_service_detail_kb(service_id: int, demo_owner_id: int | None = None) -> InlineKeyboardMarkup:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    service = get_service_by_id(service_id, demo_owner_id=owner_id)
    if service is None:
        return InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅️ К услугам", callback_data="admin:services")]]
        )
    active = int(service["is_active"]) == 1
    position = get_service_sort_position(service_id, demo_owner_id=owner_id)
    total = position[1] if position is not None else 1
    can_move_up = position is not None and position[0] > 1
    can_move_down = position is not None and position[0] < total
    back_callback = f"adm_svc_master:{int(service['master_id'])}"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✏️ Название", callback_data=f"adm_svc_rename:{service_id}"),
                InlineKeyboardButton(text="💳 Стоимость", callback_data=f"adm_svc_price:{service_id}"),
            ],
            [
                InlineKeyboardButton(
                    text="🙈 Скрыть" if active else "👁 Показать",
                    callback_data=f"adm_svc_toggle:{service_id}",
                )
            ],
            *(
                [[
                    InlineKeyboardButton(
                        text="↕️ Порядок",
                        callback_data=f"adm_svc_move_hint:{service_id}",
                    )
                ]]
                if can_move_up or can_move_down
                else []
            ),
            [InlineKeyboardButton(text="🗑 Удалить услугу", callback_data=f"adm_svc_delete:{service_id}")],
            [
                InlineKeyboardButton(text="⬅️ К услугам", callback_data=back_callback),
                InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu"),
            ],
        ]
    )


def format_admin_services_text(master_id: int, demo_owner_id: int | None = None) -> str:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    master = get_master_by_id(master_id, demo_owner_id=owner_id)
    master_name = str(master["name"]) if master is not None else DEFAULT_MASTER_NAME
    return (
        "🛠 Услуги\n\n"
        f"👤 Специалист: {master_name}\n"
        "Выберите услугу или добавьте новую."
    )


def admin_portfolio_master_select_kb(demo_owner_id: int | None = None) -> InlineKeyboardMarkup:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    rows: list[list[InlineKeyboardButton]] = []
    for master in get_active_masters(demo_owner_id=owner_id):
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"👤 {master['name']}",
                    callback_data=f"adm_port_master:{master['id']}",
                )
            ]
        )
    if not rows:
        rows.append([InlineKeyboardButton(text="Мастеров пока нет", callback_data="cal_ignore")])
    rows.append([InlineKeyboardButton(text="⬅️ К разделу «Контент»", callback_data="admin:section:content")])
    rows.append([InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_portfolio_categories_kb(master_id: int, demo_owner_id: int | None = None) -> InlineKeyboardMarkup:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    rows: list[list[InlineKeyboardButton]] = []
    for category in get_portfolio_categories(active_only=False, demo_owner_id=owner_id, master_id=master_id):
        code = str(category["code"])
        title = str(category["title"])
        count = len(get_portfolio_items(category=code, active_only=False, demo_owner_id=owner_id, master_id=master_id))
        status = "✅" if int(category["is_active"]) == 1 else "❌"
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{status} {title} · {count}",
                    callback_data=f"adm_port_cat:{code}",
                )
            ]
        )
    if not rows:
        rows.append([InlineKeyboardButton(text="Категорий пока нет", callback_data="cal_ignore")])
    rows.append([InlineKeyboardButton(text="➕ Добавить категорию", callback_data="adm_port_cat_add")])
    if is_master_choice_enabled(demo_owner_id=owner_id):
        rows.append([InlineKeyboardButton(text="👤 Выбрать другого мастера", callback_data="admin:portfolio")])
    rows.append([InlineKeyboardButton(text="⬅️ К разделу «Контент»", callback_data="admin:section:content")])
    rows.append([InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_portfolio_category_kb(category: str, master_id: int, demo_owner_id: int | None = None) -> InlineKeyboardMarkup:
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    category_row = get_portfolio_category_by_code(category, demo_owner_id=owner_id, master_id=master_id)
    is_active = int(category_row["is_active"]) == 1 if category_row is not None else True
    position = get_portfolio_category_sort_position(category, demo_owner_id=owner_id, master_id=master_id)
    total = position[1] if position is not None else 1
    can_move_up = position is not None and position[0] > 1
    can_move_down = position is not None and position[0] < total
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✏️ Название", callback_data=f"adm_port_rename:{category}"),
                InlineKeyboardButton(text="➕ Фото", callback_data=f"adm_port_add:{category}"),
            ],
            [InlineKeyboardButton(text="🗑 Удалить фото", callback_data=f"adm_port_del:{category}")],
            [
                InlineKeyboardButton(
                    text="🙈 Скрыть" if is_active else "👁 Показать",
                    callback_data=f"adm_port_toggle:{category}",
                )
            ],
            *(
                [[
                    InlineKeyboardButton(
                        text="↕️ Порядок",
                        callback_data=f"adm_port_move_hint:{category}",
                    )
                ]]
                if can_move_up or can_move_down
                else []
            ),
            [InlineKeyboardButton(text="🗑 Удалить категорию", callback_data=f"adm_port_del_cat:{category}")],
            [
                InlineKeyboardButton(text="⬅️ К категориям", callback_data=f"adm_port_master:{master_id}"),
                InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu"),
            ],
        ]
    )


def admin_back_kb(
    back_callback: str = "admin:panel",
    back_text: str = "⬅️ Назад в админку",
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=back_text, callback_data=back_callback)],
            [InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu")],
        ]
    )


def admin_broadcast_confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Запустить рассылку", callback_data="admin:broadcast_send")],
            [InlineKeyboardButton(text="❌ Отменить", callback_data="admin:broadcast_cancel")],
            [InlineKeyboardButton(text="⬅️ К коммуникациям", callback_data="admin:section:comms")],
        ]
    )


def admin_portfolio_add_kb(category: str, master_id: int) -> InlineKeyboardMarkup:
    return admin_back_kb(
        back_callback=f"adm_port_cat:{category}",
        back_text="⬅️ К категории",
    )


def admin_portfolio_delete_kb(category: str, master_id: int, demo_owner_id: int | None = None, limit: int = 25) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    items = get_portfolio_items(category=category, active_only=False, demo_owner_id=demo_owner_id, master_id=master_id)
    for row in items[:limit]:
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"🗑 Удалить фото ID {row['id']}",
                    callback_data=f"adm_port_del_id:{category}:{row['id']}",
                )
            ]
        )
    if not rows:
        rows.append([InlineKeyboardButton(text="Фото пока нет", callback_data="cal_ignore")])
    rows.append([InlineKeyboardButton(text="🔄 Обновить список", callback_data=f"adm_port_del:{category}")])
    rows.append([InlineKeyboardButton(text="⬅️ К категории", callback_data=f"adm_port_cat:{category}")])
    rows.append([InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_cancel_select_kb(limit: int = 25, demo_owner_id: int | None = None) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    appointments = get_future_appointments(demo_owner_id=demo_owner_id)
    for row in appointments[:limit]:
        rows.append(
            [
                InlineKeyboardButton(
                    text=(
                        f"❌ ID {row['id']} | "
                        f"{format_date_human(row['appointment_date'])} {row['appointment_time']} | "
                        f"{row['client_name']} | {get_master_name_from_row(row)}"
                    ),
                    callback_data=f"adm_cancel_id:{row['id']}",
                )
            ]
        )
    if not rows:
        rows.append([InlineKeyboardButton(text="Будущих записей нет", callback_data="cal_ignore")])
    rows.append([InlineKeyboardButton(text="🔄 Обновить список", callback_data="admin:cancel")])
    rows.append([InlineKeyboardButton(text="⬅️ К разделу «Записи»", callback_data="admin:section:appointments")])
    rows.append([InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_cancel_confirm_kb(appointment_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, удалить запись", callback_data=f"adm_cancel_confirm:{appointment_id}")],
            [InlineKeyboardButton(text="⬅️ К списку", callback_data="admin:cancel")],
            [InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu")],
        ]
    )


def booking_confirm_kb(show_master_edit: bool = False) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text="✅ Подтвердить запись", callback_data="confirm:yes")]]
    if show_master_edit:
        rows.append(
            [
                InlineKeyboardButton(text="👤 Специалист", callback_data="booking:edit_master"),
                InlineKeyboardButton(text="🧾 Услуга", callback_data="booking:edit_service"),
            ]
        )
    else:
        rows.append([InlineKeyboardButton(text="🧾 Услуга", callback_data="booking:edit_service")])
    rows.extend(
        [
            [
                InlineKeyboardButton(text="📅 Дата", callback_data="booking:edit_date"),
                InlineKeyboardButton(text="🕒 Время", callback_data="booking:edit_time"),
            ],
            [
                InlineKeyboardButton(text="📱 Номер", callback_data="booking:edit_contact"),
                InlineKeyboardButton(text="👤 Имя", callback_data="booking:edit_name"),
            ],
            [
                InlineKeyboardButton(text="💬 Комментарий", callback_data="booking:edit_comment"),
                InlineKeyboardButton(text="📎 Референс", callback_data="booking:edit_source"),
            ],
            [InlineKeyboardButton(text="⬅️ К шагам записи", callback_data="booking:back_source")],
            [InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def booking_name_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="👤 Взять имя из Telegram", callback_data="booking:name_profile")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="booking:back_phone")],
            [InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu")],
        ]
    )


def booking_phone_step_kb(back_callback: str, back_text: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📱 Взять номер из Telegram", callback_data="booking:phone_contact")],
            [InlineKeyboardButton(text=back_text, callback_data=back_callback)],
            [InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu")],
        ]
    )


def booking_comment_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✍️ Добавить комментарий", callback_data="booking:add_comment")],
            [InlineKeyboardButton(text="⏭ Пропустить", callback_data="booking:skip_comment")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="booking:back_name")],
            [InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu")],
        ]
    )


def booking_source_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📎 Прикрепить фото/файл", callback_data="booking:add_source")],
            [InlineKeyboardButton(text="⏭ Пропустить", callback_data="booking:skip_source")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="booking:back_comment")],
            [InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu")],
        ]
    )


def time_slots_kb(
    available_slots: list[str],
    user_id: int | None = None,
    back_callback: str | None = None,
    back_text: str | None = None,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for slot in available_slots:
        rows.append([InlineKeyboardButton(text=slot, callback_data=f"time:{slot}")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="time:change_date")])
    resolved_back_callback, resolved_back_text = booking_back_target(user_id)
    rows.append(
        [
            InlineKeyboardButton(
                text=back_text or resolved_back_text,
                callback_data=back_callback or resolved_back_callback,
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def calendar_kb(
    year: int,
    month: int,
    user_id: int | None = None,
    master_id: int | None = None,
    back_callback: str | None = None,
    back_text: str | None = None,
) -> InlineKeyboardMarkup:
    cal = calendar.Calendar(firstweekday=0)
    rows: list[list[InlineKeyboardButton]] = []
    demo_owner_id = resolve_demo_owner_id_for_user(user_id) if user_id is not None else None

    rows.append([InlineKeyboardButton(text=f"{MONTH_NAMES_RU[month]} {year}", callback_data="cal_ignore")])
    rows.append([InlineKeyboardButton(text=d, callback_data="cal_ignore") for d in ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]])

    today = datetime.now(LOCAL_TZ).date()
    for week in cal.monthdayscalendar(year, month):
        week_row: list[InlineKeyboardButton] = []
        for day in week:
            if day == 0:
                week_row.append(InlineKeyboardButton(text=" ", callback_data="cal_ignore"))
                continue
            date_obj = datetime(year=year, month=month, day=day, tzinfo=LOCAL_TZ).date()
            date_str = date_obj.strftime("%Y-%m-%d")
            if date_obj < today:
                week_row.append(InlineKeyboardButton(text="·", callback_data="cal_ignore"))
            else:
                if is_day_fully_booked(date_str, demo_owner_id=demo_owner_id, master_id=master_id):
                    week_row.append(InlineKeyboardButton(text="❌", callback_data="cal_ignore"))
                else:
                    week_row.append(
                        InlineKeyboardButton(text=str(day), callback_data=f"cal_day:{date_str}")
                    )
        rows.append(week_row)

    prev_month, prev_year = (month - 1, year)
    if prev_month < 1:
        prev_month, prev_year = 12, year - 1
    next_month, next_year = (month + 1, year)
    if next_month > 12:
        next_month, next_year = 1, year + 1

    rows.append([
        InlineKeyboardButton(text="⬅️", callback_data=f"cal_prev:{prev_year}:{prev_month}"),
        InlineKeyboardButton(text="➡️", callback_data=f"cal_next:{next_year}:{next_month}"),
    ])
    resolved_back_callback, resolved_back_text = booking_back_target(user_id)
    rows.append(
        [
            InlineKeyboardButton(
                text=back_text or resolved_back_text,
                callback_data=back_callback or resolved_back_callback,
            )
        ]
    )

    return InlineKeyboardMarkup(inline_keyboard=rows)


def booking_success_kb(user_id: int) -> InlineKeyboardMarkup:
    if get_user_role(user_id) == "demo_buyer":
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="✨ Оформить ещё одну запись", callback_data="booking:restart")],
                [InlineKeyboardButton(text="⬅️ К клиентскому пути", callback_data="demo:path:menu")],
                [InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu")],
            ]
        )
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✨ Оформить ещё одну запись", callback_data="booking:restart")],
            [InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu")],
        ]
    )


def admin_slots_calendar_kb(
    year: int,
    month: int,
    demo_owner_id: int | None = None,
    master_id: int | None = None,
) -> InlineKeyboardMarkup:
    cal = calendar.Calendar(firstweekday=0)
    rows: list[list[InlineKeyboardButton]] = []
    today = datetime.now(LOCAL_TZ).date()
    owner_id = resolve_db_demo_owner_id(demo_owner_id)
    target_master_id = int(master_id or 0)
    master_row = get_master_by_id(target_master_id, demo_owner_id=owner_id) if target_master_id else None
    master_label = f" — {master_row['name']}" if master_row is not None else ""

    rows.append([InlineKeyboardButton(text=f"Выберите дату: {MONTH_NAMES_RU[month]} {year}{master_label}", callback_data="cal_ignore")])
    rows.append([InlineKeyboardButton(text=d, callback_data="cal_ignore") for d in ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]])

    for week in cal.monthdayscalendar(year, month):
        week_row: list[InlineKeyboardButton] = []
        for day in week:
            if day == 0:
                week_row.append(InlineKeyboardButton(text=" ", callback_data="cal_ignore"))
                continue
            date_obj = datetime(year=year, month=month, day=day, tzinfo=LOCAL_TZ).date()
            date_str = date_obj.strftime("%Y-%m-%d")
            if date_obj < today:
                week_row.append(InlineKeyboardButton(text="·", callback_data="cal_ignore"))
                continue
            mark = "❌" if is_date_closed(date_str, demo_owner_id=owner_id, master_id=target_master_id) else str(day)
            week_row.append(InlineKeyboardButton(text=mark, callback_data=f"adm_cal_day:{date_str}"))
        rows.append(week_row)

    prev_month, prev_year = (month - 1, year)
    if prev_month < 1:
        prev_month, prev_year = 12, year - 1
    next_month, next_year = (month + 1, year)
    if next_month > 12:
        next_month, next_year = 1, year + 1

    rows.append(
        [
            InlineKeyboardButton(text="⬅️", callback_data=f"adm_cal_prev:{prev_year}:{prev_month}"),
            InlineKeyboardButton(text="➡️", callback_data=f"adm_cal_next:{next_year}:{next_month}"),
        ]
    )
    if is_master_choice_enabled(demo_owner_id=owner_id):
        rows.append([InlineKeyboardButton(text="👤 Выбрать другого мастера", callback_data="admin:slots")])
    rows.append([InlineKeyboardButton(text="⬅️ К админке", callback_data="admin:panel")])
    rows.append([InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# =====================================================
# 6) APScheduler: напоминания за 2 часа и за 1 час
# =====================================================
REMINDER_OFFSETS_HOURS = (2, 1)


def format_hours_ru(hours: int) -> str:
    if hours % 10 == 1 and hours % 100 != 11:
        return f"{hours} час"
    if 2 <= hours % 10 <= 4 and not 12 <= hours % 100 <= 14:
        return f"{hours} часа"
    return f"{hours} часов"


def schedule_reminders_for_appointment(appointment_id: int, date_str: str, time_str: str) -> None:
    session_at = appointment_datetime(date_str, time_str)
    now = datetime.now(LOCAL_TZ)

    for hours_before in REMINDER_OFFSETS_HOURS:
        reminder_at = session_at - timedelta(hours=hours_before)
        if reminder_at <= now:
            continue
        scheduler.add_job(
            send_reminder_job,
            trigger="date",
            run_date=reminder_at,
            args=[appointment_id, hours_before],
            id=f"reminder_{hours_before}h_{appointment_id}",
            replace_existing=True,
            misfire_grace_time=300,
        )


def remove_appointment_reminder_jobs(appointment_id: int) -> None:
    job_ids = [f"reminder_{hours}h_{appointment_id}" for hours in REMINDER_OFFSETS_HOURS]
    # Миграционный cleanup старого формата ID напоминаний.
    job_ids.append(f"reminder_{appointment_id}")
    for job_id in job_ids:
        try:
            scheduler.remove_job(job_id)
        except JobLookupError:
            pass


async def send_reminder_job(appointment_id: int, hours_before: int) -> None:
    if bot_instance is None:
        return

    row = get_appointment_by_id(appointment_id, include_all=True)
    if row is None:
        return

    session_text = (
        f"{format_date_human(row['appointment_date'])} в {row['appointment_time']}\n"
        f"Мастер: {escape(get_master_name_from_row(row))}\n"
        f"Услуга: {escape(str(row['service']))}"
    )
    hours_text = format_hours_ru(hours_before)

    try:
        await bot_instance.send_message(
            chat_id=int(row["user_id"]),
            text=(
                f"⏰ Напоминание: до вашего сеанса осталось {hours_text}.\n\n"
                f"{session_text}\n"
                "Если планы изменились, пожалуйста, сообщите заранее."
            ),
        )
    except Exception:
        pass

    for admin_id in get_workspace_admin_notify_ids(int(row["demo_owner_id"])):
        if int(admin_id) == int(row["user_id"]):
            continue
        comment = (row["client_comment"] or "").strip()
        comment_line = f"\nКомментарий: {escape(comment)}" if comment else ""
        source_line = "\nИсходник: прикреплён" if row["source_file_id"] else ""
        profile_link = profile_link_html(int(row["user_id"]))
        try:
            await bot_instance.send_message(
                chat_id=admin_id,
                text=(
                    f"⏰ Напоминание мастеру: через {hours_text} запись.\n\n"
                    f"ID: {row['id']}\n"
                    f"👤 Специалист: {escape(get_master_name_from_row(row))}\n"
                    f"Клиент: {escape(str(row['client_name']))} ({profile_link})\n"
                    f"Телефон: {escape(str(row['phone']))}\n"
                    f"{session_text}"
                    f"{comment_line}{source_line}"
                ),
                reply_markup=main_menu_kb(admin_id),
            )
        except Exception:
            pass


def restore_reminders_from_db() -> None:
    for row in get_future_appointments_all():
        schedule_reminders_for_appointment(int(row["id"]), row["appointment_date"], row["appointment_time"])


async def notify_admin_about_new_appointment(appointment_id: int) -> None:
    if bot_instance is None:
        return

    row = get_appointment_by_id(appointment_id, include_all=True)
    if row is None:
        return
    profile_kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="👤 Открыть профиль клиента", url=f"tg://user?id={row['user_id']}")]
        ]
    )
    comment = (row["client_comment"] or "").strip()
    comment_line = f"\n💬 Комментарий: {escape(comment)}" if comment else ""
    source_line = "\n📎 Исходник: прикреплён" if row["source_file_id"] else ""
    profile_link = profile_link_html(int(row["user_id"]))

    for admin_id in get_workspace_admin_notify_ids(int(row["demo_owner_id"])):
        if int(admin_id) == int(row["user_id"]):
            continue
        try:
            await bot_instance.send_message(
                chat_id=admin_id,
                text=(
                    "🔔 Новая запись в расписании\n\n"
                    f"ID: {row['id']}\n"
                    f"📅 Дата: {format_date_human(row['appointment_date'])}\n"
                    f"🕒 Время: {row['appointment_time']}\n"
                    f"👤 Специалист: {escape(get_master_name_from_row(row))}\n"
                    f"👤 Клиент: {escape(str(row['client_name']))} ({profile_link})\n"
                    f"📱 Телефон: {escape(str(row['phone']))}\n"
                    f"🧾 Услуга: {escape(str(row['service']))}"
                    f"{comment_line}{source_line}"
                ),
                reply_markup=profile_kb,
            )
            if row["source_file_id"]:
                caption = (
                    f"📎 Исходник клиента для записи ID {row['id']}\n"
                    f"Клиент: {escape(str(row['client_name']))}"
                )
                if row["source_file_type"] == "document":
                    await bot_instance.send_document(
                        chat_id=admin_id,
                        document=row["source_file_id"],
                        caption=caption,
                    )
                else:
                    await bot_instance.send_photo(
                        chat_id=admin_id,
                        photo=row["source_file_id"],
                        caption=caption,
                    )
            await bot_instance.send_message(
                chat_id=admin_id,
                text="⚙️ Быстрые действия доступны в админ-панели.",
                reply_markup=main_menu_kb(admin_id),
            )
        except Exception:
            pass

# =====================================================
# 7) Обработчики: базовые команды и клиентское меню
# =====================================================
@router.message(CommandStart())
async def start_handler(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    await state.clear()
    clear_screen_history(message.from_user.id)
    upsert_user(
        user_id=message.from_user.id,
        full_name=message.from_user.full_name,
        username=message.from_user.username,
    )

    start_payload = ""
    if message.text:
        parts = message.text.split(maxsplit=1)
        if len(parts) > 1:
            start_payload = parts[1].strip()

    user_id = message.from_user.id
    manager_from_link: int | None = None
    demo_owner_id = resolve_demo_owner_id_for_user(user_id)
    ensure_demo_workspace(demo_owner_id)
    upsert_workspace_user(
        demo_owner_id=demo_owner_id,
        user_id=user_id,
        full_name=message.from_user.full_name,
        username=message.from_user.username,
    )

    if start_payload.startswith("lead_"):
        invite_token = start_payload.replace("lead_", "", 1).strip()
        invite = get_lead_invite(invite_token)
        if invite is not None:
            manager_from_link = int(invite["manager_id"])

    # Для демо-бота любой новый клиент получает демо-доступ к админ-панели автоматически.
    # Если клиент пришел по персональной ссылке менеджера, привязываем лида к менеджеру.
    is_internal_user = is_admin(user_id) or can_manage_leads(user_id) or can_manage_admins(user_id)
    if not is_internal_user:
        if (
            manager_from_link is not None
            and manager_from_link != user_id
            and (is_manager(manager_from_link) or is_owner(manager_from_link))
        ):
            _is_new, manager_changed = assign_demo_lead(user_id, manager_from_link)
            if manager_changed:
                await notify_owner_about_new_lead(
                    manager_id=manager_from_link,
                    lead_user_id=user_id,
                    lead_full_name=message.from_user.full_name,
                    lead_username=message.from_user.username,
                )
        elif get_demo_lead_row(user_id) is None:
            add_demo_lead(user_id, assigned_by=None)

    role = get_user_role(message.from_user.id)
    await open_home_screen_for_message(message.from_user.id, role)


@router.message(Command("cancel"))
async def command_cancel_handler(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    await state.clear()
    clear_screen_history(message.from_user.id)
    await open_home_screen_for_message(message.from_user.id, get_user_role(message.from_user.id))



@router.message(Command("menu"))
async def command_menu_handler(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    await state.clear()
    clear_screen_history(message.from_user.id)
    await open_home_screen_for_message(message.from_user.id, get_user_role(message.from_user.id))


@router.message(Command("admin"))
async def command_admin_handler(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    await state.clear()
    if not is_workspace_admin(message.from_user.id):
        await render_inline_screen(message.from_user.id, "⛔ Доступ к админке закрыт.")
        return
    if is_demo_lead(message.from_user.id) and not is_owner(message.from_user.id):
        await render_inline_screen(
            message.from_user.id,
            build_demo_overview_text(),
            reply_markup=demo_admin_panel_kb(),
        )
        return
    await render_inline_screen(
        message.from_user.id,
        "⚙️ Рабочая панель\n\nВыберите раздел.",
        reply_markup=admin_panel_kb(),
    )

def format_price_list_text(user_id: int) -> str:
    demo_owner_id = resolve_demo_owner_id_for_user(user_id)
    if is_master_choice_enabled(demo_owner_id=demo_owner_id):
        lines = ["📜 Прайс", "", "Актуальные услуги и стоимость по каждому специалисту."]
        has_any_services = False
        for master in get_active_masters(demo_owner_id=demo_owner_id):
            services = get_active_services(demo_owner_id=demo_owner_id, master_id=int(master["id"]))
            if not services:
                continue
            has_any_services = True
            lines.extend(["", f"👤 {master['name']}"])
            for service in services:
                lines.append(f"• {service['name']} — {service['price']} USD")
        if not has_any_services:
            return (
                "📜 Прайс\n\n"
                "Прайс пока пуст."
            )
        return "\n".join(lines)

    primary_master = get_primary_master(demo_owner_id=demo_owner_id, active_only=True)
    target_master_id = int(primary_master["id"]) if primary_master is not None else 0
    services = get_active_services(demo_owner_id=demo_owner_id, master_id=target_master_id)
    if not services:
        return (
            "📜 Прайс\n\n"
            "Прайс пока пуст."
        )
    lines = ["📜 Прайс", "", "Актуальные услуги и стоимость."]
    for service in services:
        lines.append(f"• {service['name']} — {service['price']} USD")
    return "\n".join(lines)


async def open_booking_services_screen(
    user_id: int,
    state: FSMContext,
    callback: CallbackQuery | None = None,
) -> None:
    demo_owner_id = resolve_demo_owner_id_for_user(user_id)
    has_many_masters = is_master_choice_enabled(demo_owner_id=demo_owner_id)
    data = await state.get_data()
    master_id = int(data.get("booking_master_id") or 0)
    await state.set_state(BookingFSM.choosing_service)
    text = (
        f"👤 {data.get('booking_master_name') or DEFAULT_MASTER_NAME}\n\n"
        "Выберите услугу."
    )
    reply_markup = services_kb(
        user_id,
        master_id=master_id,
        back_callback="booking:edit_master" if has_many_masters else None,
        back_text="⬅️ К мастерам" if has_many_masters else None,
    )
    if callback is not None:
        await render_inline_screen_from_callback(callback, text, reply_markup=reply_markup)
        return
    await render_inline_screen(user_id, text, reply_markup=reply_markup)


async def open_booking_start_screen(
    user_id: int,
    state: FSMContext,
    callback: CallbackQuery | None = None,
) -> bool:
    demo_owner_id = resolve_demo_owner_id_for_user(user_id)
    masters = get_active_masters(demo_owner_id=demo_owner_id)
    if not masters:
        await render_inline_screen(
            user_id,
            "🗓 Запись пока недоступна.\n\nСначала добавьте активного специалиста в админке.",
            reply_markup=main_menu_kb(user_id) if isinstance(main_menu_kb(user_id), InlineKeyboardMarkup) else None,
        )
        return False
    primary_master = get_primary_master(demo_owner_id=demo_owner_id, active_only=True)
    target_master_id = int(primary_master["id"]) if primary_master is not None else 0
    services = (
        any(
            get_active_services(demo_owner_id=demo_owner_id, master_id=int(master["id"]))
            for master in masters
        )
        if is_master_choice_enabled(demo_owner_id=demo_owner_id)
        else get_active_services(demo_owner_id=demo_owner_id, master_id=target_master_id)
    )
    if not services:
        await render_inline_screen(
            user_id,
            "🗓 Запись пока недоступна.\n\nСначала добавьте активные услуги.",
            reply_markup=main_menu_kb(user_id) if isinstance(main_menu_kb(user_id), InlineKeyboardMarkup) else None,
        )
        return False
    await state.update_data(
        booking_date=None,
        booking_time=None,
        booking_phone=None,
        booking_name=None,
        booking_comment="",
        booking_source_id=None,
        booking_source_type=None,
        booking_source_name=None,
        booking_return_to_confirm=False,
    )
    if is_master_choice_enabled(demo_owner_id=demo_owner_id):
        await state.set_state(BookingFSM.choosing_master)
        if callback is not None:
            await render_inline_screen_from_callback(
                callback,
                "👤 Выберите специалиста\n\nОткройте карточку и продолжите запись.",
                reply_markup=booking_masters_kb(user_id),
            )
        else:
            await render_inline_screen(
                user_id,
                "👤 Выберите специалиста\n\nОткройте карточку и продолжите запись.",
                reply_markup=booking_masters_kb(user_id),
            )
        return True

    master = primary_master or masters[0]
    await state.update_data(
        booking_master_id=int(master["id"]),
        booking_master_name=str(master["name"]),
    )
    await open_booking_services_screen(user_id, state, callback=callback)
    return True


async def open_price_screen(
    user_id: int,
    from_client_path: bool = False,
    callback: CallbackQuery | None = None,
) -> None:
    in_client_path = from_client_path or get_user_role(user_id) == "demo_buyer"
    reply_markup = demo_client_path_back_kb() if in_client_path else main_menu_kb(user_id)
    if callback is not None and isinstance(reply_markup, InlineKeyboardMarkup):
        await render_inline_screen_from_callback(
            callback,
            format_price_list_text(user_id),
            reply_markup=reply_markup,
        )
        return
    if in_client_path:
        await render_inline_screen(
            user_id,
            format_price_list_text(user_id),
            reply_markup=reply_markup if isinstance(reply_markup, InlineKeyboardMarkup) else None,
        )
        return
    if isinstance(reply_markup, InlineKeyboardMarkup):
        await render_inline_screen(user_id, format_price_list_text(user_id), reply_markup=reply_markup)
        return
    await send_static_screen(user_id, format_price_list_text(user_id), reply_markup=reply_markup)


async def open_portfolio_screen(
    user_id: int,
    from_client_path: bool = False,
    master_id: int | None = None,
    callback: CallbackQuery | None = None,
) -> None:
    in_client_path = from_client_path or get_user_role(user_id) == "demo_buyer"
    demo_owner_id = resolve_demo_owner_id_for_user(user_id)
    if is_master_choice_enabled(demo_owner_id=demo_owner_id) and not master_id:
        portfolio_text = "📸 Портфолио\n\nВыберите мастера."
        if callback is not None:
            await render_inline_screen_from_callback(
                callback,
                portfolio_text,
                reply_markup=portfolio_master_select_kb(user_id, from_client_path=in_client_path),
            )
        elif in_client_path:
            await render_inline_screen(
                user_id,
                portfolio_text,
                reply_markup=portfolio_master_select_kb(user_id, from_client_path=in_client_path),
            )
        else:
            await render_inline_screen(
                user_id,
                portfolio_text,
                reply_markup=portfolio_master_select_kb(user_id, from_client_path=in_client_path),
            )
        return

    target_master_id = int(master_id or 0)
    master = get_master_by_id(target_master_id, demo_owner_id=demo_owner_id) if target_master_id else None
    intro_text = (
        f"📸 {master['name']}\n\nВыберите категорию."
        if master is not None and is_master_choice_enabled(demo_owner_id=demo_owner_id)
        else "📸 Выберите категорию."
    )
    categories = get_portfolio_categories(active_only=True, demo_owner_id=demo_owner_id, master_id=target_master_id)
    if not categories:
        if is_master_choice_enabled(demo_owner_id=demo_owner_id) and target_master_id:
            reply_markup = InlineKeyboardMarkup(
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="⬅️ К мастерам",
                            callback_data="portfolio_master_back:demo" if in_client_path else "portfolio_master_back:menu",
                        )
                    ],
                    [InlineKeyboardButton(text="🏠 В меню", callback_data="nav:main_menu")],
                ]
            )
        else:
            reply_markup = demo_client_path_back_kb() if in_client_path else main_menu_kb(user_id)
        portfolio_empty_text = (
            "📸 Портфолио пока пустое."
        )
        if callback is not None and isinstance(reply_markup, InlineKeyboardMarkup):
            await render_inline_screen_from_callback(
                callback,
                portfolio_empty_text,
                reply_markup=reply_markup,
            )
        elif in_client_path and isinstance(reply_markup, InlineKeyboardMarkup):
            await render_inline_screen(
                user_id,
                portfolio_empty_text,
                reply_markup=reply_markup,
            )
        else:
            await send_static_screen(user_id, portfolio_empty_text, reply_markup=reply_markup)
        return
    if is_master_choice_enabled(demo_owner_id=demo_owner_id) and target_master_id:
        back_callback = "portfolio_master_back:demo" if in_client_path else "portfolio_master_back:menu"
        back_text = "⬅️ К мастерам"
    else:
        back_callback = "demo:path:menu" if in_client_path else "nav:main_menu"
        back_text = "⬅️ К клиентскому пути" if in_client_path else "⬅️ Назад в меню"
    reply_markup = portfolio_categories_kb(
        user_id,
        master_id=target_master_id,
        back_callback=back_callback,
        back_text=back_text,
    )
    if callback is not None:
        await render_inline_screen_from_callback(
            callback,
            intro_text,
            reply_markup=reply_markup,
        )
    elif in_client_path:
        await render_inline_screen(
            user_id,
            intro_text,
            reply_markup=reply_markup,
        )
    else:
        await render_inline_screen(user_id, intro_text, reply_markup=reply_markup)


def build_booking_confirmation_text(data: dict) -> str:
    master_name = data.get("booking_master_name", DEFAULT_MASTER_NAME)
    service = data.get("booking_service", "-")
    price = data.get("booking_price")
    date_str = data.get("booking_date", "-")
    time_str = data.get("booking_time", "-")
    phone = data.get("booking_phone", "-")
    client_name = data.get("booking_name", "-")
    comment = (data.get("booking_comment") or "").strip()
    price_line = f"{price} USD" if isinstance(price, int) else "-"
    date_human = format_date_human(date_str) if date_str and date_str != "-" else "-"
    lines = [
        "✨ Ваша запись почти готова",
        "",
        f"👤 {master_name}",
        f"🧾 {service} — {price_line}",
        f"📅 {date_human}",
        f"🕒 {time_str}",
        f"📱 {phone}",
        f"👤 {client_name}",
    ]
    if comment:
        lines.append(f"💬 Комментарий: {comment}")
    if data.get("booking_source_id"):
        lines.append("📎 Референс прикреплён")
    return "\n".join(lines)


def normalize_phone_input(value: str) -> str | None:
    raw = value.strip()
    if not raw:
        return None
    cleaned = re.sub(r"[^\d+]", "", raw)
    if cleaned.count("+") > 1 or ("+" in cleaned and not cleaned.startswith("+")):
        return None
    digits = re.sub(r"\D", "", cleaned)
    if len(digits) < 7:
        return None
    return f"+{digits}"


def build_booking_phone_step_text(data: dict, back_to: str) -> str:
    if back_to == "confirm":
        return (
            "📱 Контакт клиента\n\n"
            "Можно ввести номер вручную одним сообщением\n"
            "или нажать кнопку ниже, чтобы взять его из Telegram.\n\n"
            "Например: +375291234567"
        )

    date_str = str(data.get("booking_date") or "")
    time_str = str(data.get("booking_time") or "")
    date_human = format_date_human(date_str) if date_str else "Не выбрана"
    return (
        f"📅 {date_human}\n"
        f"🕒 {time_str or 'Не выбрано'}\n\n"
        "Отправьте номер клиента вручную\n"
        "или возьмите его из Telegram кнопкой ниже.\n\n"
        "Например: +375291234567"
    )


async def open_booking_confirmation_screen(
    user_id: int,
    state: FSMContext,
    callback: CallbackQuery | None = None,
) -> None:
    data = await state.get_data()
    await state.update_data(booking_return_to_confirm=False)
    await state.set_state(BookingFSM.confirming)
    reply_markup = booking_confirm_kb(
        show_master_edit=is_master_choice_enabled(
            demo_owner_id=resolve_demo_owner_id_for_user(user_id)
        )
    )
    if callback is not None:
        await update_static_screen_from_callback(
            callback,
            build_booking_confirmation_text(data),
            reply_markup=reply_markup,
        )
        return
    await update_current_static_screen(
        user_id,
        build_booking_confirmation_text(data),
        reply_markup=reply_markup,
    )


async def get_sale_selection(state: FSMContext) -> tuple[str, list[str]]:
    data = await state.get_data()
    tariff_code = data.get("sale_tariff")
    selected_options_raw = data.get("sale_options") or []
    return normalize_sale_selection(str(tariff_code) if tariff_code else None, selected_options_raw)


async def save_sale_selection(state: FSMContext, tariff_code: str, selected_options: list[str]) -> None:
    await state.update_data(sale_tariff=tariff_code, sale_options=selected_options)


async def open_sale_screen_for_message(message: Message, state: FSMContext) -> None:
    tariff_code, selected_options = await get_sale_selection(state)
    await save_sale_selection(state, tariff_code, selected_options)
    await render_inline_screen(
        message.from_user.id,
        build_sale_tariff_picker_text(tariff_code),
        reply_markup=sale_tariff_picker_kb(tariff_code),
    )


async def open_sale_screen_for_callback(callback: CallbackQuery, state: FSMContext) -> None:
    tariff_code, selected_options = await get_sale_selection(state)
    await save_sale_selection(state, tariff_code, selected_options)
    await render_inline_screen_from_callback(
        callback,
        build_sale_tariff_picker_text(tariff_code),
        reply_markup=sale_tariff_picker_kb(tariff_code),
    )


async def open_sale_addons_screen_for_message(message: Message, state: FSMContext) -> None:
    tariff_code, selected_options = await get_sale_selection(state)
    await save_sale_selection(state, tariff_code, selected_options)
    await render_inline_screen(
        message.from_user.id,
        build_sale_constructor_text(tariff_code, selected_options),
        reply_markup=sales_builder_kb(tariff_code, selected_options),
    )


async def open_sale_addons_screen_for_callback(callback: CallbackQuery, state: FSMContext) -> None:
    tariff_code, selected_options = await get_sale_selection(state)
    await save_sale_selection(state, tariff_code, selected_options)
    await render_inline_screen_from_callback(
        callback,
        build_sale_constructor_text(tariff_code, selected_options),
        reply_markup=sales_builder_kb(tariff_code, selected_options),
    )


def format_admin_management_text() -> str:
    admin_ids = get_admin_ids()
    lines = ["👥 Администраторы", "", "Текущий список:"]
    for idx, admin_id in enumerate(admin_ids, start=1):
        marks: list[str] = []
        if admin_id == ADMIN_ID_INT:
            marks.append("из .env")
        if is_developer(admin_id):
            marks.append("разработчик")
        if is_staff_manager(admin_id):
            marks.append("сотрудник")
        suffix = f" ({', '.join(marks)})" if marks else ""
        lines.append(f"{idx}. <code>{admin_id}</code>{suffix}")

    lines.extend(
        [
            "",
            "Эти ID получают внутренний доступ к рабочему контуру.",
            "???????? ?????? ????? ?????? ????????.",
        ]
    )
    return "\n".join(lines)


async def open_lead_link_screen_for_message(user_id: int) -> None:
    link = await build_lead_link_for_manager(user_id)
    if not link:
        await render_inline_screen(
            user_id,
            "🔗 Сейчас не удалось собрать lead-ссылку.\n\nПроверьте username бота в BotFather.",
            reply_markup=main_menu_kb(user_id),
        )
        return

    await render_inline_screen(
        user_id,
        "🔗 Ваша lead-ссылка\n\n"
        f"{link}\n\n"
        "Отправляйте её потенциальным клиентам.\n"
        "Новые лиды по этой ссылке автоматически закрепятся за вами.",
        reply_markup=main_menu_kb(user_id),
    )


async def open_lead_link_screen_for_callback(callback: CallbackQuery) -> None:
    link = await build_lead_link_for_manager(callback.from_user.id)
    if not link:
        await render_inline_screen_from_callback(
            callback,
            "🔗 Сейчас не удалось собрать lead-ссылку.\n\nПроверьте username бота в BotFather.",
            reply_markup=main_menu_kb(callback.from_user.id),
        )
        return

    await render_inline_screen_from_callback(
        callback,
        "🔗 Ваша lead-ссылка\n\n"
        f"{link}\n\n"
        "Отправляйте её потенциальным клиентам.\n"
        "Новые лиды по этой ссылке автоматически закрепятся за вами.",
        reply_markup=main_menu_kb(callback.from_user.id),
    )


async def open_contact_screen_for_message(message: Message) -> None:
    await render_inline_screen(
        message.from_user.id,
        "💬 Обсудить запуск\n\n"
        "Если хотите такой бот под свой формат работы, можно сразу обсудить запуск.\n\n"
        "Обычно обсуждаем:\n"
        "• один мастер или команда\n"
        "• какие услуги и портфолио перенести\n"
        "• нужен ли запуск под ключ\n"
        "• какой тариф подойдёт лучше\n"
        "• как спокойно организовать размещение бота\n\n"
        "После этого можно спокойно определить лучший формат и переходить к запуску без лишней суеты.",
        reply_markup=sale_support_kb(back_callback="nav:main_menu"),
    )


async def open_contact_screen_for_callback(callback: CallbackQuery) -> None:
    await render_inline_screen_from_callback(
        callback,
        "💬 Обсудить запуск\n\n"
        "Если хотите такой бот под свой формат работы, можно сразу обсудить запуск.\n\n"
        "Обычно обсуждаем:\n"
        "• один мастер или команда\n"
        "• какие услуги и портфолио перенести\n"
        "• нужен ли запуск под ключ\n"
        "• какой тариф подойдёт лучше\n"
        "• как спокойно организовать размещение бота\n\n"
        "После этого можно спокойно определить лучший формат и переходить к запуску без лишней суеты.",
        reply_markup=sale_support_kb(back_callback="sale:open"),
    )


async def open_booking_phone_step(
    user_id: int,
    state: FSMContext,
    callback: CallbackQuery | None = None,
    back_to: str = "time",
) -> None:
    data = await state.get_data()
    await state.update_data(booking_phone_back_to=back_to)
    await state.set_state(BookingFSM.waiting_phone)
    text = build_booking_phone_step_text(data, back_to)
    await send_static_screen(
        user_id,
        text,
        reply_markup=phone_request_kb(),
    )


async def open_mini_app_screen_for_message(user_id: int) -> None:
    if not is_owner(user_id):
        await render_inline_screen(
            user_id,
            "🚧 Mini App уже скоро.\n\n"
            "Готовим отдельный интерфейс с более плавным опытом. Откроем доступ после релиза.",
            reply_markup=demo_buyer_home_kb() if get_user_role(user_id) == "demo_buyer" else None,
        )
        return

    if not MINI_APP_URL:
        await render_inline_screen(
            user_id,
            "🧪 Раздел Mini App подготовлен.\n\n"
            "Для тестового запуска укажите переменную MINI_APP_URL в .env.",
            reply_markup=main_menu_kb(user_id),
        )
        return

    await render_inline_screen(
        user_id,
        "🧪 Mini App (тестовый доступ)\n\n"
        "Нажмите кнопку ниже, чтобы открыть приложение.",
        reply_markup=mini_app_launch_kb(),
    )


async def open_mini_app_screen_for_callback(callback: CallbackQuery) -> None:
    user_id = callback.from_user.id
    if not is_owner(user_id):
        await render_inline_screen_from_callback(
            callback,
            "🚧 Mini App уже скоро.\n\n"
            "Готовим отдельный интерфейс с более плавным опытом. Откроем доступ после релиза.",
            reply_markup=demo_buyer_home_kb() if get_user_role(user_id) == "demo_buyer" else None,
        )
        return

    if not MINI_APP_URL:
        await render_inline_screen_from_callback(
            callback,
            "🧪 Раздел Mini App подготовлен.\n\n"
            "Для тестового запуска укажите переменную MINI_APP_URL в .env.",
            reply_markup=mini_app_launch_kb(),
        )
        return

    await render_inline_screen_from_callback(
        callback,
        "🧪 Mini App (тестовый доступ)\n\n"
        "Нажмите кнопку ниже, чтобы открыть приложение.",
        reply_markup=mini_app_launch_kb(),
    )


async def open_home_screen_for_message(user_id: int, role: str) -> None:
    await render_inline_screen(
        user_id,
        role_home_text(user_id),
        reply_markup=main_menu_kb(user_id),
        track_history=False,
    )


async def open_home_screen_for_callback(callback: CallbackQuery, role: str) -> None:
    await render_inline_screen_from_callback(
        callback,
        role_home_text(callback.from_user.id),
        reply_markup=main_menu_kb(callback.from_user.id),
        track_history=False,
    )


@router.callback_query(F.data == "nav:main_menu")
async def nav_main_menu_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await ack_callback(callback)
    await state.clear()
    clear_screen_history(callback.from_user.id)
    await open_home_screen_for_callback(callback, get_user_role(callback.from_user.id))


@router.callback_query(F.data == "nav:back")
async def nav_back_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await ack_callback(callback)
    await state.clear()
    chat_id = callback.from_user.id
    history = SCREEN_HISTORY.get(chat_id, [])
    if not history:
        await open_home_screen_for_callback(callback, get_user_role(chat_id))
        return

    prev_text, prev_markup = history.pop()
    CURRENT_SCREEN[chat_id] = (prev_text, prev_markup)
    await update_static_screen_from_callback(
        callback,
        str(prev_text),
        reply_markup=prev_markup,
        track_history=False,
    )


@router.callback_query(F.data == "cal_ignore")
async def calendar_ignore_callback(callback: CallbackQuery) -> None:
    await callback.answer()


@router.message(lambda message: (message.text or "").strip().lower() in {"отмена", "cancel"})
async def text_cancel_handler(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    await state.clear()
    clear_screen_history(message.from_user.id)
    await open_home_screen_for_message(message.from_user.id, get_user_role(message.from_user.id))


@router.message(lambda message: (message.text or "") in {"🧪 Клиентский путь", "🧭 Путь клиента"})
async def demo_client_path_handler(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    await state.clear()
    if is_manager_only(message.from_user.id):
        await render_inline_screen(
            message.from_user.id,
            "🎯 Здесь у менеджера доступ только к лидам и своей lead-ссылке.",
            reply_markup=main_menu_kb(message.from_user.id),
        )
        return
    await render_inline_screen(
        message.from_user.id,
        "🧭 Путь клиента\n\nНачните с записи или откройте прайс и портфолио.",
        reply_markup=demo_client_path_kb(),
    )


@router.message(lambda message: (message.text or "") == "⚙️ Демо админки")
async def demo_admin_menu_handler(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    await state.clear()
    if is_manager_only(message.from_user.id):
        await render_inline_screen(
            message.from_user.id,
            "🎯 Здесь у менеджера доступ только к лидам и своей lead-ссылке.",
            reply_markup=main_menu_kb(message.from_user.id),
        )
        return
    await render_inline_screen(
        message.from_user.id,
        build_demo_overview_text(),
        reply_markup=demo_admin_panel_kb(),
    )


@router.message(lambda message: (message.text or "") == "💰 Тарифы и покупка")
async def sale_entry_handler(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    await state.clear()
    if is_manager_only(message.from_user.id):
        await render_inline_screen(
            message.from_user.id,
            "? ??????? ? ????-???? ???????? ?????? owner. ??????????? lead-??????.",
            reply_markup=main_menu_kb(message.from_user.id),
        )
        return
    if is_owner(message.from_user.id):
        await save_sale_selection(state, "lite", [])
        await render_inline_screen(
            message.from_user.id,
            build_sale_tariff_picker_text("lite"),
            reply_markup=sale_tariff_picker_kb("lite"),
        )
        await save_sale_selection(state, "lite", [])
        return
    await render_inline_screen(
        message.from_user.id,
        build_sale_tariffs_text(),
        reply_markup=demo_sale_scenario_kb(),
    )


@router.message(lambda message: (message.text or "") in {"📩 Связаться", "💬 Обсудить запуск"})
async def contact_owner_handler(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    await state.clear()
    await open_contact_screen_for_message(message)


@router.message(lambda message: (message.text or "").startswith("\U0001F4DC"))
async def price_handler(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    await state.clear()
    if is_manager_only(message.from_user.id):
        await render_inline_screen(
            message.from_user.id,
            "🎯 Здесь у менеджера доступ только к лидам и своей lead-ссылке.",
            reply_markup=main_menu_kb(message.from_user.id),
        )
        return
    await open_price_screen(
        message.from_user.id,
        from_client_path=get_user_role(message.from_user.id) == "demo_buyer",
    )


@router.message(lambda message: (message.text or "").startswith("\U0001F4F8"))
async def portfolio_handler(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    await state.clear()
    if is_manager_only(message.from_user.id):
        await render_inline_screen(
            message.from_user.id,
            "🎯 Здесь у менеджера доступ только к лидам и своей lead-ссылке.",
            reply_markup=main_menu_kb(message.from_user.id),
        )
        return
    await open_portfolio_screen(
        message.from_user.id,
        from_client_path=get_user_role(message.from_user.id) == "demo_buyer",
    )


@router.message(lambda message: (message.text or "").startswith("\U0001F4F1"))
async def mini_app_handler(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    await state.clear()
    await open_mini_app_screen_for_message(message.from_user.id)


@router.callback_query(F.data == "miniapp:open")
async def mini_app_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await open_mini_app_screen_for_callback(callback)
    await callback.answer()


@router.callback_query(F.data == "home:booking")
async def home_booking_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if is_manager_only(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    await ack_callback(callback)
    await state.clear()
    await open_booking_start_screen(callback.from_user.id, state, callback=callback)


@router.callback_query(F.data == "home:price")
async def home_price_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if is_manager_only(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    await ack_callback(callback)
    await state.clear()
    await open_price_screen(callback.from_user.id, callback=callback)


@router.callback_query(F.data == "home:portfolio")
async def home_portfolio_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if is_manager_only(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    await ack_callback(callback)
    await state.clear()
    await open_portfolio_screen(callback.from_user.id, callback=callback)


@router.callback_query(F.data == "home:sale")
async def home_sale_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    if is_manager_only(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    if is_owner(callback.from_user.id):
        await save_sale_selection(state, "lite", [])
        await open_sale_screen_for_callback(callback, state)
        await callback.answer()
        return
    await ack_callback(callback)
    await update_static_screen_from_callback(
        callback,
        build_sale_tariffs_text(),
        reply_markup=demo_sale_scenario_kb(),
    )


@router.callback_query(F.data == "leadmgr:link")
async def lead_link_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not can_manage_leads(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    await ack_callback(callback)
    await state.clear()
    await open_lead_link_screen_for_callback(callback)


@router.callback_query(F.data == "home:admins")
@router.callback_query(F.data == "dev:show_admins")
async def admin_management_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not can_manage_admins(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    await ack_callback(callback)
    await state.clear()
    await render_inline_screen_from_callback(
        callback,
        format_admin_management_text(),
        reply_markup=admin_management_panel_kb(),
    )


@router.callback_query(F.data == "dev:add_admin")
async def developer_add_admin_start(callback: CallbackQuery, state: FSMContext) -> None:
    if not can_manage_admins(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    await ack_callback(callback)
    await state.set_state(DeveloperFSM.waiting_new_admin_id)
    await render_inline_screen_from_callback(
        callback,
        "👥 Новый администратор\n\nВведите Telegram ID пользователя.",
        reply_markup=admin_back_kb(back_callback="dev:show_admins", back_text="⬅️ К администраторам"),
    )


@router.message(DeveloperFSM.waiting_new_admin_id)
async def developer_add_admin_finish(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    if not can_manage_admins(message.from_user.id):
        await state.clear()
        return
    raw = (message.text or "").strip()
    if not raw.isdigit():
        await render_inline_screen(
            message.from_user.id,
            "Введите корректный Telegram ID.",
            reply_markup=admin_back_kb(back_callback="dev:show_admins", back_text="⬅️ К администраторам"),
        )
        return
    ok = add_admin_user(int(raw), added_by=message.from_user.id)
    await state.clear()
    await render_inline_screen(
        message.from_user.id,
        "✅ Администратор добавлен." if ok else "Не удалось добавить администратора.\n\nПроверьте ID и попробуйте ещё раз.",
        reply_markup=admin_management_panel_kb(),
    )


@router.callback_query(F.data == "dev:remove_menu")
async def developer_remove_admin_menu_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not can_manage_admins(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    await ack_callback(callback)
    await state.clear()
    await render_inline_screen_from_callback(
        callback,
        "➖ Удаление администратора\n\nВыберите ID из списка ниже.",
        reply_markup=admin_remove_select_kb(),
    )


@router.callback_query(F.data.startswith("dev:remove_admin:"))
async def developer_remove_admin_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not can_manage_admins(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    await ack_callback(callback)
    await state.clear()
    try:
        target_id = int(callback.data.split(":", maxsplit=2)[2])
    except Exception:
        await callback.answer("Некорректный ID", show_alert=True)
        return
    if target_id == callback.from_user.id and is_owner(callback.from_user.id):
        await render_inline_screen_from_callback(
            callback,
            "Нельзя удалить владельца из списка администраторов.",
            reply_markup=admin_remove_select_kb(),
        )
        return
    removed = remove_admin_user(target_id)
    await render_inline_screen_from_callback(
        callback,
        f"✅ Администратор <code>{target_id}</code> удалён." if removed else "Администратор не найден.",
        reply_markup=admin_management_panel_kb(),
    )


@router.message(lambda message: (message.text or "").startswith("✨"))
async def demo_overview_message_handler(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    await state.clear()
    user_id = message.from_user.id
    if not is_demo_lead(user_id) and not is_owner(user_id):
        await render_inline_screen(
            user_id,
            "Этот раздел доступен только в демо-режиме.",
            reply_markup=main_menu_kb(user_id),
        )
        return
    await render_inline_screen(
        user_id,
        build_demo_overview_text(),
        reply_markup=demo_admin_panel_kb(),
    )


@router.callback_query(F.data == "lead:open")
async def demo_overview_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_demo_lead(callback.from_user.id) and not is_owner(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    await state.clear()
    await render_inline_screen_from_callback(
        callback,
        build_demo_overview_text(),
        reply_markup=demo_admin_panel_kb(),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("demo:path:"))
async def demo_client_path_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if is_manager_only(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return

    action = callback.data.split(":", maxsplit=2)[2]
    await ack_callback(callback)
    await state.clear()

    if action == "menu":
        await render_inline_screen_from_callback(
            callback,
            "🧭 Путь клиента\n\nНачните с записи, а потом при желании откройте прайс и портфолио.",
            reply_markup=demo_client_path_kb(),
        )
        return
    if action == "booking":
        await open_booking_start_screen(callback.from_user.id, state, callback=callback)
        return
    if action == "price":
        await open_price_screen(callback.from_user.id, from_client_path=True, callback=callback)
        return
    if action == "portfolio":
        await open_portfolio_screen(callback.from_user.id, from_client_path=True, callback=callback)
        return

    await render_inline_screen_from_callback(
        callback,
        "🧭 Путь клиента\n\nНачните с записи, а потом при желании откройте прайс и портфолио.",
        reply_markup=demo_client_path_kb(),
    )


@router.callback_query(F.data == "lead:live_admin")
async def demo_open_live_admin_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    await ack_callback(callback)
    await state.clear()
    await render_inline_screen_from_callback(
        callback,
        "⚙️ Рабочая панель\n\nВыберите раздел.",
        reply_markup=admin_panel_kb(),
    )


@router.callback_query(F.data == "lead:sale_inside")
async def demo_sale_inside_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_demo_lead(callback.from_user.id) and not is_owner(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    await ack_callback(callback)
    await state.clear()
    await render_inline_screen_from_callback(
        callback,
        build_sale_inside_text(),
        reply_markup=demo_sale_scenario_kb(),
    )


@router.callback_query(F.data == "lead:sale_tariffs")
async def demo_sale_tariffs_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_demo_lead(callback.from_user.id) and not is_owner(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    await ack_callback(callback)
    await state.clear()
    await render_inline_screen_from_callback(
        callback,
        build_sale_tariffs_text(),
        reply_markup=demo_sale_scenario_kb(),
    )


@router.callback_query(F.data.in_({"lead:today", "lead:all", "lead:services", "lead:portfolio"}))
async def demo_feature_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_demo_lead(callback.from_user.id) and not is_owner(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    feature_code = callback.data.split(":", maxsplit=1)[1]
    if feature_code == "portfolio":
        await admin_portfolio_start(callback, state)
        return
    await ack_callback(callback)
    await state.clear()
    await open_demo_feature_preview(callback.from_user.id, feature_code)


@router.callback_query(F.data.startswith("master_card:"))
async def master_card_callback(callback: CallbackQuery) -> None:
    if is_manager_only(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    parts = callback.data.split(":", maxsplit=2)
    if len(parts) != 3 or not parts[2].isdigit():
        await callback.answer("Некорректный мастер", show_alert=True)
        return
    context = parts[1]
    master_id = int(parts[2])
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    master = get_master_by_id(master_id, demo_owner_id=demo_owner_id)
    if master is None or int(master["is_active"]) != 1:
        await callback.answer("Мастер недоступен", show_alert=True)
        return
    await ack_callback(callback)
    await update_static_screen_from_callback(
        callback,
        format_master_public_card_text(master_id, demo_owner_id=demo_owner_id),
        reply_markup=master_card_kb(callback.from_user.id, master_id, context),
    )
    await send_master_preview(callback.from_user.id, master)


@router.callback_query(F.data.startswith("master_card_back:"))
async def master_card_back_callback(callback: CallbackQuery) -> None:
    await ack_callback(callback)
    context = callback.data.split(":", maxsplit=1)[1]
    if context == "booking":
        await update_static_screen_from_callback(
            callback,
            "👤 Выберите специалиста.",
            reply_markup=booking_masters_kb(callback.from_user.id),
        )
    else:
        await open_portfolio_screen(
            callback.from_user.id,
            from_client_path=(context == "portfolio_demo"),
            callback=callback,
        )


@router.callback_query(F.data.startswith("portfolio_master:"))
async def portfolio_master_callback(callback: CallbackQuery) -> None:
    if is_manager_only(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    _, master_id_raw = callback.data.split(":", maxsplit=1)
    if not master_id_raw.isdigit():
        await callback.answer("Некорректный мастер", show_alert=True)
        return
    master_id = int(master_id_raw)
    await ack_callback(callback)
    await open_portfolio_screen(
        callback.from_user.id,
        from_client_path=get_user_role(callback.from_user.id) == "demo_buyer",
        master_id=master_id,
        callback=callback,
    )


@router.callback_query(F.data.startswith("portfolio_master_back:"))
async def portfolio_master_back_callback(callback: CallbackQuery) -> None:
    await ack_callback(callback)
    mode = callback.data.split(":", maxsplit=1)[1]
    await open_portfolio_screen(
        callback.from_user.id,
        from_client_path=(mode == "demo"),
        callback=callback,
    )


@router.callback_query(F.data.startswith("portfolio:"))
async def portfolio_category_callback(callback: CallbackQuery) -> None:
    if is_manager_only(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return

    parts = callback.data.split(":", maxsplit=2)
    if len(parts) != 3 or not parts[1].isdigit():
        await callback.answer("Некорректная категория", show_alert=True)
        return
    master_id = int(parts[1])
    category = parts[2]
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    category_title = get_portfolio_category_title(category, demo_owner_id=demo_owner_id, master_id=master_id)
    items = get_portfolio_items(category=category, active_only=True, demo_owner_id=demo_owner_id, master_id=master_id)
    await ack_callback(callback)

    if not items:
        role = get_user_role(callback.from_user.id)
        back_callback = "portfolio_master_back:demo" if role == "demo_buyer" else "portfolio_master_back:menu"
        back_text = "⬅️ К мастерам"
        await update_static_screen_from_callback(
            callback,
            f"📸 {category_title}\n\nВ этой категории пока нет работ.",
            reply_markup=portfolio_categories_kb(
                callback.from_user.id,
                master_id=master_id,
                back_callback=back_callback,
                back_text=back_text,
            ),
        )
        return

    await show_or_update_portfolio_preview(callback.from_user.id, str(items[0]["url"]))

    role = get_user_role(callback.from_user.id)
    back_callback = "portfolio_master_back:demo" if role == "demo_buyer" else "portfolio_master_back:menu"
    back_text = "⬅️ К мастерам"
    await update_static_screen_from_callback(
        callback,
        (
            f"📸 {category_title}\n\n"
            f"Фото: {len(items)}\n"
            "Можно переключать категории ниже."
        ),
        reply_markup=portfolio_categories_kb(
            callback.from_user.id,
            master_id=master_id,
            back_callback=back_callback,
            back_text=back_text,
        ),
        clear_aux=False,
    )


@router.message(lambda message: (message.text or "").startswith("\U0001F485"))
async def booking_start_handler(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    await state.clear()
    if is_manager_only(message.from_user.id):
        await send_static_screen(
            message.from_user.id,
            "🎯 Здесь у менеджера доступ только к лидам и своей lead-ссылке.",
            reply_markup=main_menu_kb(message.from_user.id),
        )
        return
    await open_booking_start_screen(message.from_user.id, state)


@router.callback_query(F.data.startswith("booking_master:"))
async def booking_master_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if is_manager_only(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return

    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    try:
        master_id = int(callback.data.split(":", maxsplit=1)[1])
    except Exception:
        await callback.answer("Некорректный мастер", show_alert=True)
        return

    master = get_master_by_id(master_id, demo_owner_id=demo_owner_id)
    if master is None or int(master["is_active"]) != 1:
        await callback.answer("Мастер недоступен", show_alert=True)
        return
    await ack_callback(callback)

    await state.update_data(
        booking_master_id=int(master["id"]),
        booking_master_name=str(master["name"]),
        booking_service=None,
        booking_price=None,
        booking_date=None,
        booking_time=None,
    )
    await state.set_state(BookingFSM.choosing_service)
    await update_static_screen_from_callback(
        callback,
        f"👤 {master['name']}\n\nВыберите услугу.",
        reply_markup=services_kb(
            callback.from_user.id,
            master_id=int(master["id"]),
            back_callback="booking:edit_master",
            back_text="⬅️ К мастерам",
        ),
    )


@router.callback_query(F.data.startswith("service:"))
async def booking_service_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if is_manager_only(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return

    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    try:
        service_id = int(callback.data.split(":", maxsplit=1)[1])
    except Exception:
        await callback.answer("Некорректная услуга", show_alert=True)
        return

    data = await state.get_data()
    return_to_confirm = bool(data.get("booking_return_to_confirm"))
    master_id = int(data.get("booking_master_id") or 0)
    master_name = str(data.get("booking_master_name") or "")
    if not master_id or not master_name:
        masters = get_active_masters(demo_owner_id=demo_owner_id)
        if is_master_choice_enabled(demo_owner_id=demo_owner_id):
            await state.set_state(BookingFSM.choosing_master)
            await update_static_screen_from_callback(
                callback,
                "👤 Сначала выберите специалиста.",
                reply_markup=booking_masters_kb(callback.from_user.id),
            )
            await callback.answer()
            return
        if masters:
            primary_master = get_primary_master(demo_owner_id=demo_owner_id, active_only=True) or masters[0]
            master_id = int(primary_master["id"])
            master_name = str(primary_master["name"])
            await state.update_data(
                booking_master_id=master_id,
                booking_master_name=master_name,
            )
    service = get_service_by_id(service_id, demo_owner_id=demo_owner_id, master_id=master_id)
    if service is None or int(service["is_active"]) != 1:
        await callback.answer("Услуга недоступна для выбранного мастера", show_alert=True)
        return
    await ack_callback(callback)

    now = datetime.now(LOCAL_TZ)
    await state.update_data(
        booking_service=str(service["name"]),
        booking_price=int(service["price"]),
        calendar_year=now.year,
        calendar_month=now.month,
    )
    if return_to_confirm:
        await open_booking_confirmation_screen(callback.from_user.id, state, callback=callback)
        return
    await state.set_state(BookingFSM.choosing_date)
    await update_static_screen_from_callback(
        callback,
        "📅 Выберите дату.",
        reply_markup=calendar_kb(
            now.year,
            now.month,
            callback.from_user.id,
            master_id=master_id,
            back_callback="booking:edit_service",
            back_text="⬅️ К услугам",
        ),
    )


@router.callback_query(F.data.startswith("cal_prev:"))
async def calendar_prev_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if is_manager_only(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    try:
        _, year_s, month_s = callback.data.split(":", maxsplit=2)
        year = int(year_s)
        month = int(month_s)
    except Exception:
        await callback.answer("Некорректные данные календаря", show_alert=True)
        return
    await ack_callback(callback)

    await state.set_state(BookingFSM.choosing_date)
    await state.update_data(calendar_year=year, calendar_month=month)
    state_data = await state.get_data()
    back_callback = "booking:return_confirm" if state_data.get("booking_return_to_confirm") else "booking:edit_service"
    back_text = "⬅️ К подтверждению" if state_data.get("booking_return_to_confirm") else "⬅️ К услугам"
    await update_static_screen_from_callback(
        callback,
        "📅 Выберите дату.",
        reply_markup=calendar_kb(
            year,
            month,
            callback.from_user.id,
            master_id=int(state_data.get("booking_master_id") or 0),
            back_callback=back_callback,
            back_text=back_text,
        ),
    )


@router.callback_query(F.data.startswith("cal_next:"))
async def calendar_next_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if is_manager_only(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    try:
        _, year_s, month_s = callback.data.split(":", maxsplit=2)
        year = int(year_s)
        month = int(month_s)
    except Exception:
        await callback.answer("Некорректные данные календаря", show_alert=True)
        return
    await ack_callback(callback)

    await state.set_state(BookingFSM.choosing_date)
    await state.update_data(calendar_year=year, calendar_month=month)
    state_data = await state.get_data()
    back_callback = "booking:return_confirm" if state_data.get("booking_return_to_confirm") else "booking:edit_service"
    back_text = "⬅️ К подтверждению" if state_data.get("booking_return_to_confirm") else "⬅️ К услугам"
    await update_static_screen_from_callback(
        callback,
        "📅 Выберите дату.",
        reply_markup=calendar_kb(
            year,
            month,
            callback.from_user.id,
            master_id=int(state_data.get("booking_master_id") or 0),
            back_callback=back_callback,
            back_text=back_text,
        ),
    )


@router.callback_query(F.data.startswith("cal_day:"))
async def calendar_day_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if is_manager_only(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return

    date_str = callback.data.split(":", maxsplit=1)[1]
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    state_data = await state.get_data()
    return_to_confirm = bool(state_data.get("booking_return_to_confirm"))
    master_id = int(state_data.get("booking_master_id") or 0)
    free_slots = get_available_slots_for_booking(
        date_str,
        demo_owner_id=demo_owner_id,
        master_id=master_id,
    )

    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        await callback.answer("Некорректная дата", show_alert=True)
        return
    await ack_callback(callback)

    await state.update_data(
        booking_date=date_str,
        calendar_year=date_obj.year,
        calendar_month=date_obj.month,
    )

    if not free_slots:
        await state.set_state(BookingFSM.choosing_date)
        await update_static_screen_from_callback(
            callback,
            "📅 На эту дату свободных окон нет.\n\nВыберите другой день.",
            reply_markup=calendar_kb(
                date_obj.year,
                date_obj.month,
                callback.from_user.id,
                master_id=master_id,
                back_callback="booking:return_confirm" if return_to_confirm else "booking:edit_service",
                back_text="⬅️ К подтверждению" if return_to_confirm else "⬅️ К услугам",
            ),
        )
        return

    await state.set_state(BookingFSM.choosing_time)
    await update_static_screen_from_callback(
        callback,
        f"📅 {format_date_human(date_str)}\n\n🕒 Выберите время.",
        reply_markup=time_slots_kb(
            free_slots,
            callback.from_user.id,
            back_callback="booking:return_confirm" if return_to_confirm else "booking:edit_service",
            back_text="⬅️ К подтверждению" if return_to_confirm else "⬅️ К услугам",
        ),
    )


@router.callback_query(F.data == "time:change_date")
async def booking_change_date_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if is_manager_only(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    await ack_callback(callback)

    data = await state.get_data()
    year = int(data.get("calendar_year") or datetime.now(LOCAL_TZ).year)
    month = int(data.get("calendar_month") or datetime.now(LOCAL_TZ).month)
    await state.set_state(BookingFSM.choosing_date)
    await update_static_screen_from_callback(
        callback,
        "📅 Выберите другую дату.",
        reply_markup=calendar_kb(
            year,
            month,
            callback.from_user.id,
            master_id=int(data.get("booking_master_id") or 0),
            back_callback="booking:edit_service",
            back_text="⬅️ К услугам",
        ),
    )


@router.callback_query(F.data.startswith("time:"))
async def booking_time_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if is_manager_only(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return

    slot = callback.data.split(":", maxsplit=1)[1]
    if slot == "change_date":
        await callback.answer()
        return

    data = await state.get_data()
    return_to_confirm = bool(data.get("booking_return_to_confirm"))
    date_str = str(data.get("booking_date") or "")
    master_id = int(data.get("booking_master_id") or 0)
    if not date_str:
        await state.set_state(BookingFSM.choosing_date)
        now = datetime.now(LOCAL_TZ)
        await update_static_screen_from_callback(
            callback,
            "📅 Сначала выберите дату.",
            reply_markup=calendar_kb(
                now.year,
                now.month,
                callback.from_user.id,
                master_id=master_id,
                back_callback="booking:edit_service",
                back_text="⬅️ К услугам",
            ),
        )
        return
    await ack_callback(callback)

    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    free_slots = get_available_slots_for_booking(
        date_str,
        demo_owner_id=demo_owner_id,
        master_id=master_id,
    )
    if slot not in free_slots:
        if not free_slots:
            await state.set_state(BookingFSM.choosing_date)
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            await update_static_screen_from_callback(
                callback,
                "📅 На эту дату свободных окон больше нет.\n\nВыберите другой день.",
                reply_markup=calendar_kb(
                    date_obj.year,
                    date_obj.month,
                    callback.from_user.id,
                    master_id=master_id,
                    back_callback="booking:return_confirm" if return_to_confirm else "booking:edit_service",
                    back_text="⬅️ К подтверждению" if return_to_confirm else "⬅️ К услугам",
                ),
            )
        else:
            await state.set_state(BookingFSM.choosing_time)
            await update_static_screen_from_callback(
                callback,
                "🕒 Это время уже занято.\n\nВыберите другое.",
                reply_markup=time_slots_kb(
                    free_slots,
                    callback.from_user.id,
                    back_callback="booking:return_confirm" if return_to_confirm else "booking:edit_service",
                    back_text="⬅️ К подтверждению" if return_to_confirm else "⬅️ К услугам",
                ),
            )
        return

    await state.update_data(booking_time=slot)
    if return_to_confirm:
        await open_booking_confirmation_screen(callback.from_user.id, state, callback=callback)
        return
    await open_booking_phone_step(callback.from_user.id, state, callback=callback, back_to="time")


@router.message(BookingFSM.waiting_phone, F.contact)
async def booking_phone_contact_handler(message: Message, state: FSMContext) -> None:
    contact = message.contact
    if contact is None:
        return

    await clear_aux_messages(message.from_user.id)
    await hide_reply_keyboard(message.from_user.id)
    phone = normalize_phone_input(str(contact.phone_number or "").strip())
    if not phone:
        data = await state.get_data()
        back_to = str(data.get("booking_phone_back_to") or "time")
        await update_current_static_screen(
            message.from_user.id,
            build_booking_phone_step_text(data, back_to) + "\n\nНомер не распознан. Попробуйте ещё раз.",
        )
        return

    await state.update_data(booking_phone=phone)
    data = await state.get_data()
    if data.get("booking_return_to_confirm") and data.get("booking_name"):
        await open_booking_confirmation_screen(message.from_user.id, state)
    else:
        await state.set_state(BookingFSM.waiting_client_name)
        await update_current_static_screen(
            message.from_user.id,
            "?? ??? ??????? ????????.\n\n??????? ??????? 2 ???????.",
            reply_markup=booking_name_kb(),
        )
    await try_delete_user_message(message)


@router.message(BookingFSM.waiting_phone)
async def booking_phone_text_fallback(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    await clear_aux_messages(message.from_user.id)
    if is_reply_keyboard_active(message.from_user.id):
        await hide_reply_keyboard(message.from_user.id)
    if (message.text or "").strip() == "⬅️ Назад":
        data = await state.get_data()
        back_to = str(data.get("booking_phone_back_to") or "time")
        if back_to == "confirm":
            await open_booking_confirmation_screen(message.from_user.id, state)
            return
        date_str = str(data.get("booking_date") or "")
        if not date_str:
            await open_booking_services_screen(message.from_user.id, state)
            return
        demo_owner_id = resolve_demo_owner_id_for_user(message.from_user.id)
        master_id = int(data.get("booking_master_id") or 0)
        free_slots = get_available_slots_for_booking(
            date_str,
            demo_owner_id=demo_owner_id,
            master_id=master_id,
        )
        await state.set_state(BookingFSM.choosing_time)
        await update_current_static_screen(
            message.from_user.id,
            f"📅 {format_date_human(date_str)}\n\nВыберите время.",
            reply_markup=time_slots_kb(
                free_slots,
                message.from_user.id,
                back_callback="booking:edit_service",
                back_text="⬅️ К услугам",
            ),
        )
        return

    normalized_phone = normalize_phone_input(message.text or "")
    data = await state.get_data()
    back_to = str(data.get("booking_phone_back_to") or "time")
    if not normalized_phone:
        await update_current_static_screen(
            message.from_user.id,
            build_booking_phone_step_text(data, back_to) + "\n\nПроверьте формат номера и отправьте его ещё раз.",
        )
        return

    await state.update_data(booking_phone=normalized_phone)
    data = await state.get_data()
    if data.get("booking_return_to_confirm") and data.get("booking_name"):
        await open_booking_confirmation_screen(message.from_user.id, state)
        return

    await state.set_state(BookingFSM.waiting_client_name)
    await update_current_static_screen(
        message.from_user.id,
        "👤 Как подписать запись?\n\nМожно взять имя из Telegram или ввести своё.",
        reply_markup=booking_name_kb(),
    )


@router.callback_query(F.data == "booking:back_phone")
async def booking_back_to_phone_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await ack_callback(callback)
    back_to = str((await state.get_data()).get("booking_phone_back_to") or "time")
    await open_booking_phone_step(callback.from_user.id, state, callback=callback, back_to=back_to)


@router.callback_query(F.data == "booking:phone_contact")
async def booking_phone_contact_request_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await ack_callback(callback)
    data = await state.get_data()
    back_to = str(data.get("booking_phone_back_to") or "time")
    await open_booking_phone_step(callback.from_user.id, state, callback=callback, back_to=back_to)


@router.callback_query(F.data == "booking:name_profile")
async def booking_use_profile_name_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await ack_callback(callback)
    full_name = callback.from_user.full_name or str(callback.from_user.id)
    await state.update_data(booking_name=full_name)
    data = await state.get_data()
    if data.get("booking_return_to_confirm"):
        await open_booking_confirmation_screen(callback.from_user.id, state, callback=callback)
        return
    await state.set_state(BookingFSM.waiting_comment)
    await update_static_screen_from_callback(
        callback,
        "💬 Комментарий для мастера можно добавить или пропустить.",
        reply_markup=booking_comment_kb(),
    )


@router.message(BookingFSM.waiting_client_name)
async def booking_name_message_handler(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    name = (message.text or "").strip()
    if len(name) < 2:
        await update_current_static_screen(
            message.from_user.id,
            "👤 Имя слишком короткое.\n\nВведите минимум 2 символа.",
            reply_markup=booking_name_kb(),
        )
        return

    await state.update_data(booking_name=name)
    data = await state.get_data()
    if data.get("booking_return_to_confirm"):
        await open_booking_confirmation_screen(message.from_user.id, state)
        return
    await state.set_state(BookingFSM.waiting_comment)
    await update_current_static_screen(
        message.from_user.id,
        "💬 Комментарий для мастера можно добавить или пропустить.",
        reply_markup=booking_comment_kb(),
    )


@router.callback_query(F.data == "booking:back_name")
async def booking_back_to_name_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await ack_callback(callback)
    data = await state.get_data()
    if data.get("booking_return_to_confirm"):
        await open_booking_confirmation_screen(callback.from_user.id, state, callback=callback)
        return
    await state.set_state(BookingFSM.waiting_client_name)
    await update_static_screen_from_callback(
        callback,
        "👤 Введите имя клиента\n\nИли используйте имя из профиля.",
        reply_markup=booking_name_kb(),
    )


@router.callback_query(F.data == "booking:skip_comment")
async def booking_skip_comment_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await ack_callback(callback)
    await state.update_data(booking_comment="")
    data = await state.get_data()
    if data.get("booking_return_to_confirm"):
        await open_booking_confirmation_screen(callback.from_user.id, state, callback=callback)
        return
    await state.set_state(BookingFSM.waiting_source)
    await update_static_screen_from_callback(
        callback,
        "📎 Референс можно прикрепить сейчас или пропустить.",
        reply_markup=booking_source_kb(),
    )


@router.callback_query(F.data == "booking:add_comment")
async def booking_add_comment_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await ack_callback(callback)
    await state.set_state(BookingFSM.waiting_comment)
    await update_static_screen_from_callback(
        callback,
        "💬 Отправьте комментарий одним сообщением.",
        reply_markup=booking_comment_kb(),
    )


@router.message(BookingFSM.waiting_comment)
async def booking_comment_message_handler(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    comment = (message.text or "").strip()
    await state.update_data(booking_comment=comment)
    data = await state.get_data()
    if data.get("booking_return_to_confirm"):
        await open_booking_confirmation_screen(message.from_user.id, state)
        return
    await state.set_state(BookingFSM.waiting_source)
    await update_current_static_screen(
        message.from_user.id,
        "📎 Референс можно прикрепить сейчас или пропустить.",
        reply_markup=booking_source_kb(),
    )


@router.callback_query(F.data == "booking:back_comment")
async def booking_back_to_comment_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await ack_callback(callback)
    data = await state.get_data()
    if data.get("booking_return_to_confirm"):
        await open_booking_confirmation_screen(callback.from_user.id, state, callback=callback)
        return
    await state.set_state(BookingFSM.waiting_comment)
    await update_static_screen_from_callback(
        callback,
        "💬 Комментарий для мастера можно добавить или пропустить.",
        reply_markup=booking_comment_kb(),
    )


@router.callback_query(F.data == "booking:skip_source")
async def booking_skip_source_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await ack_callback(callback)
    await state.update_data(
        booking_source_id=None,
        booking_source_type=None,
        booking_source_name=None,
    )
    await open_booking_confirmation_screen(callback.from_user.id, state, callback=callback)


@router.callback_query(F.data == "booking:add_source")
async def booking_add_source_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await ack_callback(callback)
    await state.set_state(BookingFSM.waiting_source)
    await update_static_screen_from_callback(
        callback,
        "📎 Отправьте фото или файл одним сообщением.",
        reply_markup=booking_source_kb(),
    )


@router.message(BookingFSM.waiting_source)
async def booking_source_message_handler(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)

    source_file_id = None
    source_file_type = None
    source_file_name = None

    if message.photo:
        source_file_id = message.photo[-1].file_id
        source_file_type = "photo"
    elif message.document:
        source_file_id = message.document.file_id
        source_file_type = "document"
        source_file_name = message.document.file_name

    if not source_file_id:
        await update_current_static_screen(
            message.from_user.id,
            "📎 Отправьте фото или файл.\n\nИли нажмите «Пропустить».",
            reply_markup=booking_source_kb(),
        )
        return

    await state.update_data(
        booking_source_id=source_file_id,
        booking_source_type=source_file_type,
        booking_source_name=source_file_name,
    )
    await open_booking_confirmation_screen(message.from_user.id, state)


@router.callback_query(F.data == "booking:back_source")
async def booking_back_to_source_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await ack_callback(callback)
    data = await state.get_data()
    if data.get("booking_return_to_confirm"):
        await open_booking_confirmation_screen(callback.from_user.id, state, callback=callback)
        return
    await state.set_state(BookingFSM.waiting_source)
    await update_static_screen_from_callback(
        callback,
        "📎 Референс можно прикрепить сейчас или пропустить.",
        reply_markup=booking_source_kb(),
    )


@router.callback_query(F.data == "booking:edit_service")
async def booking_edit_service_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await ack_callback(callback)
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    has_many_masters = is_master_choice_enabled(demo_owner_id=demo_owner_id)
    data = await state.get_data()
    await state.update_data(booking_return_to_confirm=True)
    back_callback = "booking:return_confirm"
    back_text = "⬅️ К подтверждению"
    await state.set_state(BookingFSM.choosing_service)
    await update_static_screen_from_callback(
        callback,
        f"👤 {data.get('booking_master_name') or DEFAULT_MASTER_NAME}\n\nВыберите услугу.",
        reply_markup=services_kb(
            callback.from_user.id,
            master_id=int(data.get("booking_master_id") or 0),
            back_callback=back_callback if not has_many_masters else back_callback,
            back_text=back_text if not has_many_masters else back_text,
        ),
    )


@router.callback_query(F.data == "booking:edit_master")
async def booking_edit_master_callback(callback: CallbackQuery, state: FSMContext) -> None:
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    masters = get_active_masters(demo_owner_id=demo_owner_id)
    if not is_master_choice_enabled(demo_owner_id=demo_owner_id) or len(masters) <= 1:
        await callback.answer("Выбор мастера доступен только в режиме команды.", show_alert=True)
        return
    await ack_callback(callback)
    await state.update_data(booking_return_to_confirm=False)
    await state.set_state(BookingFSM.choosing_master)
    await state.update_data(
        booking_service=None,
        booking_price=None,
        booking_date=None,
        booking_time=None,
    )
    await update_static_screen_from_callback(
        callback,
        "👤 Выберите специалиста.",
        reply_markup=booking_masters_kb(callback.from_user.id),
    )


@router.callback_query(F.data == "booking:edit_date")
async def booking_edit_date_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await ack_callback(callback)
    await state.update_data(booking_return_to_confirm=True)
    data = await state.get_data()
    back_callback = "booking:return_confirm"
    back_text = "⬅️ К подтверждению"
    date_str = str(data.get("booking_date") or "")
    if date_str:
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            year, month = date_obj.year, date_obj.month
        except ValueError:
            now = datetime.now(LOCAL_TZ)
            year, month = now.year, now.month
    else:
        now = datetime.now(LOCAL_TZ)
        year, month = now.year, now.month
    await state.set_state(BookingFSM.choosing_date)
    await state.update_data(calendar_year=year, calendar_month=month)
    await update_static_screen_from_callback(
        callback,
        "📅 Выберите дату.",
        reply_markup=calendar_kb(
            year,
            month,
            callback.from_user.id,
            master_id=int(data.get("booking_master_id") or 0),
            back_callback=back_callback,
            back_text=back_text,
        ),
    )


@router.callback_query(F.data == "booking:edit_time")
async def booking_edit_time_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await ack_callback(callback)
    await state.update_data(booking_return_to_confirm=True)
    data = await state.get_data()
    date_str = str(data.get("booking_date") or "")
    if not date_str:
        await booking_edit_date_callback(callback, state)
        return
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    master_id = int(data.get("booking_master_id") or 0)
    free_slots = get_available_slots_for_booking(
        date_str,
        demo_owner_id=demo_owner_id,
        master_id=master_id,
    )
    if not free_slots:
        await booking_edit_date_callback(callback, state)
        return
    await state.set_state(BookingFSM.choosing_time)
    await update_static_screen_from_callback(
        callback,
        f"📅 {format_date_human(date_str)}\n\n🕒 Выберите время.",
        reply_markup=time_slots_kb(
            free_slots,
            callback.from_user.id,
            back_callback="booking:return_confirm",
            back_text="⬅️ К подтверждению",
        ),
    )


@router.callback_query(F.data == "booking:edit_contact")
async def booking_edit_contact_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await ack_callback(callback)
    await state.update_data(booking_return_to_confirm=True)
    await open_booking_phone_step(callback.from_user.id, state, callback=callback, back_to="confirm")


@router.callback_query(F.data == "booking:edit_name")
async def booking_edit_name_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await ack_callback(callback)
    await state.update_data(booking_return_to_confirm=True)
    await state.set_state(BookingFSM.waiting_client_name)
    await update_static_screen_from_callback(
        callback,
        "👤 Введите имя клиента\n\nИли используйте имя из профиля.",
        reply_markup=booking_name_kb(),
    )


@router.callback_query(F.data == "booking:edit_comment")
async def booking_edit_comment_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await ack_callback(callback)
    await state.update_data(booking_return_to_confirm=True)
    await state.set_state(BookingFSM.waiting_comment)
    await update_static_screen_from_callback(
        callback,
        "💬 Добавьте комментарий одним сообщением или пропустите этот шаг.",
        reply_markup=booking_comment_kb(),
    )


@router.callback_query(F.data == "booking:edit_source")
async def booking_edit_source_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await ack_callback(callback)
    await state.update_data(booking_return_to_confirm=True)
    await state.set_state(BookingFSM.waiting_source)
    await update_static_screen_from_callback(
        callback,
        "📎 Прикрепите референс одним сообщением или пропустите этот шаг.",
        reply_markup=booking_source_kb(),
    )


@router.callback_query(F.data == "booking:return_time")
async def booking_return_time_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await ack_callback(callback)
    await clear_aux_messages(callback.from_user.id)
    await hide_reply_keyboard(callback.from_user.id)
    data = await state.get_data()
    date_str = str(data.get("booking_date") or "")
    if not date_str:
        await open_booking_services_screen(callback.from_user.id, state, callback=callback)
        return
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    master_id = int(data.get("booking_master_id") or 0)
    free_slots = get_available_slots_for_booking(
        date_str,
        demo_owner_id=demo_owner_id,
        master_id=master_id,
    )
    await state.set_state(BookingFSM.choosing_time)
    await update_static_screen_from_callback(
        callback,
        f"📅 {format_date_human(date_str)}\n\n🕒 Выберите время.",
        reply_markup=time_slots_kb(
            free_slots,
            callback.from_user.id,
            back_callback="booking:edit_service",
            back_text="⬅️ К услугам",
        ),
    )


@router.callback_query(F.data == "booking:return_confirm")
async def booking_return_confirm_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await ack_callback(callback)
    await clear_aux_messages(callback.from_user.id)
    await hide_reply_keyboard(callback.from_user.id)
    await open_booking_confirmation_screen(callback.from_user.id, state, callback=callback)


@router.callback_query(F.data == "confirm:yes")
async def booking_confirm_callback(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    required_fields = [
        data.get("booking_master_id"),
        data.get("booking_master_name"),
        data.get("booking_service"),
        data.get("booking_date"),
        data.get("booking_time"),
        data.get("booking_phone"),
        data.get("booking_name"),
    ]
    if any(not item for item in required_fields):
        await state.clear()
        await update_static_screen_from_callback(
            callback,
            "Не удалось завершить запись.\n\nПожалуйста, начните заново.",
            reply_markup=main_menu_kb(callback.from_user.id),
        )
        await callback.answer()
        return
    await ack_callback(callback)

    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    appointment_date = str(data["booking_date"])
    appointment_time = str(data["booking_time"])
    master_id = int(data["booking_master_id"])
    master_name = str(data["booking_master_name"])
    if not is_slot_free(
        appointment_date,
        appointment_time,
        demo_owner_id=demo_owner_id,
        master_id=master_id,
    ):
        free_slots = get_available_slots_for_booking(
            appointment_date,
            demo_owner_id=demo_owner_id,
            master_id=master_id,
        )
        if free_slots:
            await state.set_state(BookingFSM.choosing_time)
            await update_static_screen_from_callback(
                callback,
                "🕒 Это время уже занято.\n\nВыберите другое.",
                reply_markup=time_slots_kb(
                    free_slots,
                    callback.from_user.id,
                    back_callback="booking:edit_service",
                    back_text="⬅️ К услугам",
                ),
            )
        else:
            date_obj = datetime.strptime(appointment_date, "%Y-%m-%d")
            await state.set_state(BookingFSM.choosing_date)
            await update_static_screen_from_callback(
                callback,
                "📅 На эту дату свободных окон больше нет.\n\nВыберите другой день.",
                reply_markup=calendar_kb(
                    date_obj.year,
                    date_obj.month,
                    callback.from_user.id,
                    master_id=master_id,
                    back_callback="booking:edit_service",
                    back_text="⬅️ К услугам",
                ),
            )
        return

    appointment_id = create_appointment(
        demo_owner_id=demo_owner_id,
        master_id=master_id,
        master_name=master_name,
        user_id=callback.from_user.id,
        client_name=str(data["booking_name"]),
        phone=str(data["booking_phone"]),
        service=str(data["booking_service"]),
        client_comment=str(data.get("booking_comment") or "").strip() or None,
        source_file_id=data.get("booking_source_id"),
        source_file_type=data.get("booking_source_type"),
        source_file_name=data.get("booking_source_name"),
        appointment_date=appointment_date,
        appointment_time=appointment_time,
    )

    if appointment_id is None:
        await update_static_screen_from_callback(
            callback,
            "Не удалось создать запись.\n\nПопробуйте чуть позже.",
            reply_markup=main_menu_kb(callback.from_user.id),
        )
        await state.clear()
        return

    schedule_reminders_for_appointment(appointment_id, appointment_date, appointment_time)
    await notify_admin_about_new_appointment(appointment_id)

    await state.clear()
    price = data.get("booking_price")
    price_line = f"{price} USD" if isinstance(price, int) else "-"
    await update_static_screen_from_callback(
        callback,
        "✨ Запись оформлена\n\n"
        f"👤 {master_name}\n"
        f"🧾 {data['booking_service']}\n"
        f"📅 {format_date_human(appointment_date)}\n"
        f"🕒 {appointment_time}\n"
        f"💳 {price_line}\n\n"
        "Клиент уже видит аккуратную, понятную запись. Если нужно, можно сразу оформить ещё одну.",
        reply_markup=booking_success_kb(callback.from_user.id),
    )


@router.callback_query(F.data == "booking:restart")
async def booking_restart_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await ack_callback(callback)
    await state.clear()
    await open_booking_start_screen(callback.from_user.id, state, callback=callback)


@router.message(lambda message: (message.text or "").startswith("\U0001F4B3"))
async def buy_bot_message_handler(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    await state.clear()
    if is_manager_only(message.from_user.id):
        await send_static_screen(
            message.from_user.id,
            "? ??????? ? ????-???? ???????? ?????? owner. ??????????? lead-??????.",
            reply_markup=main_menu_kb(message.from_user.id),
        )
        return
    await save_sale_selection(state, "lite", [])
    await open_sale_screen_for_message(message, state)


@router.callback_query(F.data == "sale:open")
async def sale_open_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if is_manager_only(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    tariff_code, selected_options = await get_sale_selection(state)
    await state.clear()
    await save_sale_selection(state, tariff_code, selected_options)
    await open_sale_screen_for_callback(callback, state)
    await callback.answer()


@router.callback_query(F.data == "sale:addons")
async def sale_addons_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if is_manager_only(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    await open_sale_addons_screen_for_callback(callback, state)
    await callback.answer()


@router.callback_query(F.data == "sale:next_steps")
async def sale_next_steps_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    if is_manager_only(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    await update_static_screen_from_callback(
        callback,
        build_sale_next_steps_text(),
        reply_markup=sale_support_kb(back_callback="sale:open"),
    )
    await callback.answer()


@router.callback_query(F.data == "sale:contact")
async def sale_contact_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    if is_manager_only(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    await open_contact_screen_for_callback(callback)
    await callback.answer()


@router.callback_query(F.data.startswith("sale:set:"))
async def sale_set_tariff_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if is_manager_only(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return

    parts = callback.data.split(":", maxsplit=2)
    if len(parts) != 3:
        await callback.answer("Некорректные данные", show_alert=True)
        return
    tariff_code = parts[2]
    if tariff_code not in SALE_TARIFFS:
        await callback.answer("Тариф не найден", show_alert=True)
        return
    _, selected_options = await get_sale_selection(state)
    await save_sale_selection(state, tariff_code, selected_options)
    await open_sale_addons_screen_for_callback(callback, state)
    await callback.answer("✨ Основа выбрана")


@router.callback_query(F.data.startswith("sale:opt:"))
async def sale_toggle_option_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if is_manager_only(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    parts = callback.data.split(":", maxsplit=2)
    if len(parts) != 3:
        await callback.answer("Некорректные данные", show_alert=True)
        return
    option_code = parts[2]
    if option_code not in SALE_OPTIONS:
        await callback.answer("Опция не найдена", show_alert=True)
        return

    tariff_code, selected_options = await get_sale_selection(state)
    selected_set = set(selected_options)
    if option_code in selected_set:
        selected_set.remove(option_code)
    else:
        selected_set.add(option_code)
    normalized_tariff, normalized_options = normalize_sale_selection(tariff_code, selected_set)
    await save_sale_selection(state, normalized_tariff, normalized_options)
    await open_sale_addons_screen_for_callback(callback, state)
    await callback.answer("Опция обновлена")


@router.callback_query(F.data == "sale:invoice")
async def sale_invoice_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if is_manager_only(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return

    if not PAYMENT_PROVIDER_TOKEN:
        await update_static_screen_from_callback(
            callback,
            "😔 Оплата сейчас временно недоступна.\n\nНо мы можем спокойно обсудить запуск и подобрать подходящий формат вручную.",
            reply_markup=sale_support_kb(back_callback="sale:open"),
        )
        await callback.answer()
        return

    tariff_code, selected_options = await get_sale_selection(state)
    tariff = SALE_TARIFFS[tariff_code]
    total_minor = calculate_sale_total_minor(tariff_code, selected_options)

    option_titles = [str(SALE_OPTIONS[code]["title"]) for code in selected_options]
    options_line = ", ".join(option_titles) if option_titles else "без дополнительных опций"
    invoice_title = f"Запуск бота: {tariff['title']}"
    invoice_description = (
        f"Формат: {tariff['title']}\n"
        f"Дополнительно: {options_line}\n"
        f"Итого: {format_payment_amount(total_minor)}"
    )
    order_code = tariff_code
    if selected_options:
        order_code = f"{tariff_code}+{'+'.join(selected_options)}"
    order_title = f"{tariff['title']} ({options_line})"

    order = create_sales_order(
        user_id=callback.from_user.id,
        tariff_code=order_code,
        tariff_title=order_title,
        amount_minor=total_minor,
        currency=PAYMENT_CURRENCY,
    )

    prices = [LabeledPrice(label=invoice_title, amount=total_minor)]
    try:
        await callback.bot.send_invoice(
            chat_id=callback.from_user.id,
            title=invoice_title,
            description=invoice_description,
            payload=f"sale_order:{order['id']}",
            provider_token=PAYMENT_PROVIDER_TOKEN,
            currency=PAYMENT_CURRENCY,
            prices=prices,
        )
        await callback.answer()
    except Exception:
        await update_static_screen_from_callback(
            callback,
            "😔 С оплатой что-то пошло не так.\n\nМожно вернуться назад или просто обсудить запуск вручную.",
            reply_markup=sale_support_kb(back_callback="sale:open"),
        )
        await callback.answer()


@router.pre_checkout_query()
async def pre_checkout_handler(pre_checkout_query: PreCheckoutQuery) -> None:
    if bot_instance is None:
        return

    payload = pre_checkout_query.invoice_payload
    order = get_sales_order_by_payload(payload)
    if order is None:
        await bot_instance.answer_pre_checkout_query(
            pre_checkout_query.id,
            ok=False,
            error_message="Заказ не найден.",
        )
        return

    await bot_instance.answer_pre_checkout_query(pre_checkout_query.id, ok=True)


@router.message(F.successful_payment)
async def successful_payment_handler(message: Message) -> None:
    payment = message.successful_payment
    if payment is None:
        return

    order = mark_sales_order_paid(payment.invoice_payload)
    if order is None:
        await send_static_screen(
            message.from_user.id,
            "✨ Оплата получена\n\nСпасибо. Я скоро свяжусь с вами, чтобы спокойно перейти к запуску.",
            reply_markup=main_menu_kb(message.from_user.id),
        )
        return

    lead_row = get_demo_lead(message.from_user.id)
    assigned_by = int(lead_row["assigned_by"]) if lead_row is not None and lead_row["assigned_by"] is not None else None

    notify_ids = set(sales_notify_ids())
    if assigned_by is not None:
        notify_ids.add(assigned_by)

    lead_identity = format_user_identity(
        user_id=message.from_user.id,
        full_name=message.from_user.full_name,
        username=message.from_user.username,
    )
    amount_major = int(order["amount_minor"]) / 100
    manager_line = (
        f"ID менеджера: <code>{assigned_by}</code>" if assigned_by is not None else "ID менеджера: не указан"
    )

    for notify_id in sorted(notify_ids):
        try:
            await bot_instance.send_message(
                chat_id=notify_id,
                text=(
                    "💳 Оплата получена\n\n"
                    f"Клиент: {lead_identity}\n"
                    f"Тариф: {escape(str(order['tariff_title']))}\n"
                    f"Сумма: {amount_major:.2f} {escape(str(order['currency']))}\n"
                    f"{manager_line}"
                ),
            )
        except Exception:
            pass

    await send_static_screen(
        message.from_user.id,
        "✨ Оплата получена\n\nСпасибо. Я свяжусь с вами, уточню формат работы и спокойно помогу перейти к запуску.",
        reply_markup=main_menu_kb(message.from_user.id),
    )


async def open_admin_management_screen(user_id: int) -> None:
    await render_inline_screen(
        user_id,
        format_admin_management_text(),
        reply_markup=admin_management_panel_kb(),
    )


@router.message(Command("developer"))
@router.message(Command("admins"))
async def command_admin_management_handler(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    await state.clear()
    if not can_manage_admins(message.from_user.id):
        await render_inline_screen(message.from_user.id, "⛔ Доступ к управлению администраторами закрыт.")
        return
    await open_admin_management_screen(message.from_user.id)


@router.message(F.text.in_(["👥 Управление админами", "🧑‍💻 Разработчик"]))
async def admin_management_panel_handler(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    await state.clear()
    if not can_manage_admins(message.from_user.id):
        await render_inline_screen(message.from_user.id, "⛔ Доступ к управлению администраторами закрыт.")
        return
    await open_admin_management_screen(message.from_user.id)


def format_leads_overview_text(viewer_id: int) -> str:
    assigned_by = None if is_owner(viewer_id) else viewer_id
    leads = get_demo_leads(limit=50, assigned_by=assigned_by)
    if not leads:
        return "🎯 Лиды\n\nНовых лидов пока нет."
    title = "🎯 Лиды · последние 50" if is_owner(viewer_id) else "🎯 Ваши лиды · последние 50"
    lines = [title]
    for idx, row in enumerate(leads, start=1):
        status = "✅ оплачен" if str(row["status"]) == "paid" else "🆕 лид"
        name = escape(str(row["full_name"] or row["user_id"]))
        username = f" (@{escape(str(row['username']))})" if row["username"] else ""
        lines.append(f"{idx}. {name}{username} · ID {row['user_id']} · {status}")
    return "\n".join(lines)


async def open_lead_manager_screen(user_id: int) -> None:
    await render_inline_screen(
        user_id,
        format_leads_overview_text(user_id),
        reply_markup=lead_manager_panel_kb(user_id),
    )


@router.message(Command("leads"))
@router.message(lambda message: can_manage_leads(message.from_user.id) and (message.text or "").startswith("\U0001F3AF"))
async def lead_manager_open_handler(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    await state.clear()
    if not can_manage_leads(message.from_user.id):
        await render_inline_screen(message.from_user.id, "⛔ Доступ к лидам закрыт.")
        return
    await open_lead_manager_screen(message.from_user.id)


@router.message(lambda message: can_manage_leads(message.from_user.id) and (message.text or "").startswith("🔗"))
async def lead_link_handler(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    await state.clear()
    if not can_manage_leads(message.from_user.id):
        await render_inline_screen(message.from_user.id, "⛔ Доступ к lead-ссылкам закрыт.")
        return
    await open_lead_link_screen_for_message(message.from_user.id)


@router.callback_query(F.data == "leadmgr:open")
async def lead_manager_open_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not can_manage_leads(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    await state.clear()
    await render_inline_screen_from_callback(
        callback,
        format_leads_overview_text(callback.from_user.id),
        reply_markup=lead_manager_panel_kb(callback.from_user.id),
    )
    await callback.answer()


@router.callback_query(F.data == "leadmgr:list")
async def lead_manager_list_callback(callback: CallbackQuery) -> None:
    if not can_manage_leads(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    await render_inline_screen_from_callback(
        callback,
        format_leads_overview_text(callback.from_user.id),
        reply_markup=lead_manager_list_kb(callback.from_user.id),
    )
    await callback.answer()


@router.callback_query(F.data == "leadmgr:add_menu")
async def lead_manager_add_menu_callback(callback: CallbackQuery) -> None:
    if not is_owner(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    await render_inline_screen_from_callback(
        callback,
        "➕ Новый лид\n\nВыберите пользователя, которого нужно назначить лидом.",
        reply_markup=lead_manager_add_select_kb(),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("leadmgr:add:"))
async def lead_manager_add_callback(callback: CallbackQuery) -> None:
    if not is_owner(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return

    try:
        target_id = int(callback.data.split(":", maxsplit=2)[2])
    except Exception:
        await callback.answer("Некорректный ID", show_alert=True)
        return

    assign_demo_lead(target_id, assigned_by=callback.from_user.id)
    await render_inline_screen_from_callback(
        callback,
        f"✅ Пользователь <code>{target_id}</code> назначен лидом.",
        reply_markup=lead_manager_add_select_kb(),
    )
    await callback.answer("Готово")


@router.callback_query(F.data.startswith("leadmgr:remove:"))
async def lead_manager_remove_callback(callback: CallbackQuery) -> None:
    if not is_owner(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return

    try:
        target_id = int(callback.data.split(":", maxsplit=2)[2])
    except Exception:
        await callback.answer("Некорректный ID", show_alert=True)
        return

    removed = remove_demo_lead(target_id)
    if removed:
        text = f"✅ Лид <code>{target_id}</code> удалён."
        notice = "Удалено"
    else:
        text = "Лид не найден."
        notice = "Не найден"

    await render_inline_screen_from_callback(
        callback,
        text,
        reply_markup=lead_manager_list_kb(callback.from_user.id),
    )
    await callback.answer(notice)


@router.message(lambda message: is_workspace_admin(message.from_user.id) and (message.text or "").startswith("\u2699"))
async def admin_panel_handler(message: Message) -> None:
    await try_delete_user_message(message)
    if not is_workspace_admin(message.from_user.id):
        await render_inline_screen(message.from_user.id, "⛔ Доступ к админке закрыт.")
        return
    if is_demo_lead(message.from_user.id) and not is_owner(message.from_user.id):
        await render_inline_screen(
            message.from_user.id,
            build_demo_overview_text(),
            reply_markup=demo_admin_panel_kb(),
        )
        return
    await render_inline_screen(
        message.from_user.id,
        "⚙️ Рабочая панель\n\nВыберите раздел.",
        reply_markup=admin_panel_kb(),
    )


@router.callback_query(F.data == "admin:section:appointments")
async def admin_section_appointments_callback(callback: CallbackQuery) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    await update_static_screen_from_callback(
        callback,
        "📋 Записи\n\nВыберите, что хотите открыть.",
        reply_markup=admin_appointments_kb(),
    )
    await callback.answer()


@router.callback_query(F.data == "admin:section:content")
async def admin_section_content_callback(callback: CallbackQuery) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    await update_static_screen_from_callback(
        callback,
        "🗂 Контент\n\nЗдесь настраиваются мастера, услуги и портфолио.",
        reply_markup=admin_content_kb(),
    )
    await callback.answer()


@router.callback_query(F.data == "admin:section:comms")
async def admin_section_comms_callback(callback: CallbackQuery) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    await update_static_screen_from_callback(
        callback,
        "📢 Коммуникации\n\nРассылка по пользователям рабочего контура.",
        reply_markup=admin_comms_kb(),
    )
    await callback.answer()


@router.callback_query(F.data == "admin:today")
async def admin_today_callback(callback: CallbackQuery) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return

    today_str = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d")
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    rows = get_appointments_for_date(today_str, demo_owner_id=demo_owner_id)
    today_human = format_date_human(today_str)
    text = (
        f"📅 На {today_human} записей пока нет."
        if not rows
        else f"📅 Записи на сегодня ({today_human})\n\n{format_appointments(rows)}"
    )
    await update_static_screen_from_callback(
        callback,
        text,
        reply_markup=admin_back_kb(
            back_callback="admin:section:appointments",
            back_text="⬅️ К разделу «Записи»",
        ),
    )
    await callback.answer()


@router.callback_query(F.data == "admin:all")
async def admin_all_callback(callback: CallbackQuery) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return

    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    rows = get_future_appointments(demo_owner_id=demo_owner_id)
    text = (
        "🗓 Будущих записей пока нет."
        if not rows
        else f"🗓 Все будущие записи\n\n{format_appointments(rows)}"
    )
    await update_static_screen_from_callback(
        callback,
        text,
        reply_markup=admin_back_kb(
            back_callback="admin:section:appointments",
            back_text="⬅️ К разделу «Записи»",
        ),
    )
    await callback.answer()


@router.callback_query(F.data == "admin:panel")
async def admin_panel_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    await state.clear()
    await update_static_screen_from_callback(
        callback,
        "⚙️ Рабочая панель\n\nВыберите раздел.",
        reply_markup=admin_panel_kb(),
    )
    await callback.answer()

@router.callback_query(F.data == "admin:slots")
async def admin_slots_start(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    masters = get_active_masters(demo_owner_id=demo_owner_id)
    booking_mode = get_workspace_booking_mode(demo_owner_id=demo_owner_id)
    await state.clear()
    if not masters:
        await update_static_screen_from_callback(
            callback,
            "👤 Сначала добавьте хотя бы одного активного специалиста.",
            reply_markup=admin_back_kb(
                back_callback="admin:section:content",
                back_text="⬅️ К разделу «Контент»",
            ),
        )
        await callback.answer()
        return

    now = datetime.now(LOCAL_TZ)
    await state.set_state(AdminFSM.waiting_slot_date)
    if is_master_choice_enabled(demo_owner_id=demo_owner_id):
        await update_static_screen_from_callback(
            callback,
            "🪟 Окошки\n\nВыберите мастера, для которого хотите настроить расписание:",
            reply_markup=admin_slot_master_select_kb(demo_owner_id=demo_owner_id),
        )
        await callback.answer()
        return

    master = get_primary_master(demo_owner_id=demo_owner_id, active_only=True) or masters[0]
    await state.update_data(
        admin_slot_master_id=int(master["id"]),
        admin_slots_year=now.year,
        admin_slots_month=now.month,
    )
    await update_static_screen_from_callback(
        callback,
        "🪟 Окошки\n\n"
        f"Режим: {'команда' if booking_mode == WORKSPACE_MODE_TEAM else 'один мастер'}\n"
        f"Мастер: {master['name']}\n"
        "Выберите дату в календаре. Дни с ❌ закрыты.",
        reply_markup=admin_slots_calendar_kb(
            now.year,
            now.month,
            demo_owner_id=demo_owner_id,
            master_id=int(master["id"]),
        ),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("adm_slot_master:"))
async def admin_slot_master_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    try:
        master_id = int(callback.data.split(":", maxsplit=1)[1])
    except Exception:
        await callback.answer("Некорректный мастер", show_alert=True)
        return
    master = get_master_by_id(master_id, demo_owner_id=demo_owner_id)
    if master is None or int(master["is_active"]) != 1:
        await callback.answer("Мастер недоступен", show_alert=True)
        return

    now = datetime.now(LOCAL_TZ)
    await state.set_state(AdminFSM.waiting_slot_date)
    await state.update_data(
        admin_slot_master_id=master_id,
        admin_slots_year=now.year,
        admin_slots_month=now.month,
    )
    await update_static_screen_from_callback(
        callback,
        f"🪟 Окошки\n\nМастер: {master['name']}\nВыберите дату в календаре. Дни с ❌ закрыты.",
        reply_markup=admin_slots_calendar_kb(
            now.year,
            now.month,
            demo_owner_id=demo_owner_id,
            master_id=master_id,
        ),
    )
    await callback.answer()


@router.callback_query(F.data == "adm_slot_calendar_back")
async def admin_slot_calendar_back_to_month(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    data = await state.get_data()
    master_id = int(data.get("admin_slot_master_id") or 0)
    year = int(data.get("admin_slots_year") or datetime.now(LOCAL_TZ).year)
    month = int(data.get("admin_slots_month") or datetime.now(LOCAL_TZ).month)
    await state.set_state(AdminFSM.waiting_slot_date)
    await update_static_screen_from_callback(
        callback,
        "🪟 Окошки\n\nВыберите дату в календаре. Дни с ❌ закрыты.",
        reply_markup=admin_slots_calendar_kb(
            year,
            month,
            demo_owner_id=demo_owner_id,
            master_id=master_id,
        ),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("adm_cal_prev:"))
async def admin_slots_calendar_prev_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    state_data = await state.get_data()
    master_id = int(state_data.get("admin_slot_master_id") or 0)
    _, year_s, month_s = callback.data.split(":")
    year = int(year_s)
    month = int(month_s)
    await state.set_state(AdminFSM.waiting_slot_date)
    await state.update_data(admin_slots_year=year, admin_slots_month=month, admin_slot_master_id=master_id)
    await update_static_screen_from_callback(
        callback,
        "🪟 Окошки\n\nВыберите дату в календаре. Дни с ❌ закрыты.",
        reply_markup=admin_slots_calendar_kb(
            year,
            month,
            demo_owner_id=demo_owner_id,
            master_id=master_id,
        ),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("adm_cal_next:"))
async def admin_slots_calendar_next_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    state_data = await state.get_data()
    master_id = int(state_data.get("admin_slot_master_id") or 0)
    _, year_s, month_s = callback.data.split(":")
    year = int(year_s)
    month = int(month_s)
    await state.set_state(AdminFSM.waiting_slot_date)
    await state.update_data(admin_slots_year=year, admin_slots_month=month, admin_slot_master_id=master_id)
    await update_static_screen_from_callback(
        callback,
        "🪟 Окошки\n\nВыберите дату в календаре. Дни с ❌ закрыты.",
        reply_markup=admin_slots_calendar_kb(
            year,
            month,
            demo_owner_id=demo_owner_id,
            master_id=master_id,
        ),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("adm_cal_day:"))
async def admin_slots_calendar_day_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    date_str = callback.data.split(":", maxsplit=1)[1]
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        await callback.answer("Некорректная дата", show_alert=True)
        return
    state_data = await state.get_data()
    master_id = int(state_data.get("admin_slot_master_id") or 0)
    await state.set_state(AdminFSM.waiting_slot_date)
    await state.update_data(
        slot_date=date_str,
        admin_slots_year=date_obj.year,
        admin_slots_month=date_obj.month,
        admin_slot_master_id=master_id,
    )
    await update_static_screen_from_callback(
        callback,
        format_slots_admin_text(date_str, demo_owner_id=demo_owner_id, master_id=master_id),
        reply_markup=admin_slots_kb(date_str, master_id=master_id, demo_owner_id=demo_owner_id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("adm_slot_toggle:"))
async def admin_slot_toggle_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    state_data = await state.get_data()
    master_id = int(state_data.get("admin_slot_master_id") or 0)
    _, date_str, token = callback.data.split(":")
    if len(token) != 4 or not token.isdigit():
        await callback.answer("Некорректное время", show_alert=True)
        return
    time_str = f"{token[:2]}:{token[2:]}"
    currently_allowed = time_str in set(get_allowed_slots(date_str, demo_owner_id=demo_owner_id, master_id=master_id))
    set_slot_override(
        date_str,
        time_str,
        0 if currently_allowed else 1,
        demo_owner_id=demo_owner_id,
        master_id=master_id,
    )
    await update_static_screen_from_callback(
        callback,
        format_slots_admin_text(date_str, demo_owner_id=demo_owner_id, master_id=master_id),
        reply_markup=admin_slots_kb(date_str, master_id=master_id, demo_owner_id=demo_owner_id),
    )
    await callback.answer("Обновлено")


@router.callback_query(F.data.startswith("adm_date_toggle:"))
async def admin_date_toggle_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    state_data = await state.get_data()
    master_id = int(state_data.get("admin_slot_master_id") or 0)
    _, date_str = callback.data.split(":")
    now_closed = toggle_date_closed(date_str, demo_owner_id=demo_owner_id, master_id=master_id)
    await update_static_screen_from_callback(
        callback,
        format_slots_admin_text(date_str, demo_owner_id=demo_owner_id, master_id=master_id),
        reply_markup=admin_slots_kb(date_str, master_id=master_id, demo_owner_id=demo_owner_id),
    )
    await callback.answer("Дата закрыта" if now_closed else "Дата открыта")


@router.callback_query(F.data.startswith("adm_slot_reset:"))
async def admin_slot_reset_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    state_data = await state.get_data()
    master_id = int(state_data.get("admin_slot_master_id") or 0)
    _, date_str = callback.data.split(":")
    clear_slot_settings_for_date(date_str, demo_owner_id=demo_owner_id, master_id=master_id)
    await update_static_screen_from_callback(
        callback,
        format_slots_admin_text(date_str, demo_owner_id=demo_owner_id, master_id=master_id),
        reply_markup=admin_slots_kb(date_str, master_id=master_id, demo_owner_id=demo_owner_id),
    )
    await callback.answer("Настройки даты сброшены")


@router.callback_query(F.data.startswith("adm_slot_add:"))
async def admin_slot_add_time_start(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    _, date_str = callback.data.split(":")
    await state.set_state(AdminFSM.waiting_slot_custom_time)
    await state.update_data(slot_date=date_str)
    await update_static_screen_from_callback(
        callback,
        f"Введите дополнительное время для {date_str} в формате HH:MM.\nПример: 10:30",
        reply_markup=admin_back_kb(
            back_callback=f"adm_slot_back:{date_str}",
            back_text="⬅️ К дате",
        ),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("adm_slot_back:"))
async def admin_slot_back_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    state_data = await state.get_data()
    master_id = int(state_data.get("admin_slot_master_id") or 0)
    _, date_str = callback.data.split(":")
    await state.set_state(AdminFSM.waiting_slot_date)
    await update_static_screen_from_callback(
        callback,
        format_slots_admin_text(date_str, demo_owner_id=demo_owner_id, master_id=master_id),
        reply_markup=admin_slots_kb(date_str, master_id=master_id, demo_owner_id=demo_owner_id),
    )
    await callback.answer()


@router.message(AdminFSM.waiting_slot_custom_time)
async def admin_slot_add_time_finish(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    if not is_workspace_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    date_str = data.get("slot_date")
    demo_owner_id = resolve_demo_owner_id_for_user(message.from_user.id)
    master_id = int(data.get("admin_slot_master_id") or 0)
    if not date_str:
        now = datetime.now(LOCAL_TZ)
        await state.set_state(AdminFSM.waiting_slot_date)
        await state.update_data(admin_slots_year=now.year, admin_slots_month=now.month)
        await send_static_screen(
            message.from_user.id,
            "Сначала выберите дату в календаре.",
            reply_markup=admin_slots_calendar_kb(
                now.year,
                now.month,
                demo_owner_id=demo_owner_id,
                master_id=master_id,
            ),
        )
        return
    normalized = normalize_time_str((message.text or "").strip())
    if normalized is None:
        await send_static_screen(
            message.from_user.id,
            "???????? ?????? ???????. ??????????? HH:MM.",
            reply_markup=admin_back_kb(
                back_callback=f"adm_slot_back:{date_str}",
                back_text="⬅️ К дате",
            ),
        )
        return

    set_slot_override(
        date_str,
        normalized,
        1,
        demo_owner_id=demo_owner_id,
        master_id=master_id,
    )
    await state.set_state(AdminFSM.waiting_slot_date)
    await send_static_screen(
        message.from_user.id,
        format_slots_admin_text(date_str, demo_owner_id=demo_owner_id, master_id=master_id),
        reply_markup=admin_slots_kb(date_str, master_id=master_id, demo_owner_id=demo_owner_id),
    )


@router.callback_query(F.data == "admin:masters")
async def admin_masters_start(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    await state.clear()
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    mode = get_workspace_booking_mode(demo_owner_id=demo_owner_id)
    await update_static_screen_from_callback(
        callback,
        "👥 Мастера\n\n"
        f"Формат: {'команда с выбором мастера' if mode == WORKSPACE_MODE_TEAM else 'один мастер без выбора'}.\n"
        "В solo запись идёт к основному мастеру. В team клиент сначала выбирает специалиста.",
        reply_markup=admin_masters_kb(demo_owner_id=demo_owner_id),
    )
    await callback.answer()


@router.callback_query(F.data == "adm_master_add")
async def admin_master_add_start(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    await state.set_state(AdminFSM.waiting_new_master_name)
    await update_static_screen_from_callback(
        callback,
        "?? ??? ??????? ????????.\n\n??????? ??????? 2 ???????.",
        reply_markup=admin_back_kb(back_callback="admin:masters", back_text="⬅️ К мастерам"),
    )
    await callback.answer()


@router.callback_query(F.data == "adm_mode_toggle")
async def admin_mode_toggle_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    await state.clear()
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    current_mode = get_workspace_booking_mode(demo_owner_id=demo_owner_id)
    new_mode = (
        WORKSPACE_MODE_SOLO
        if current_mode == WORKSPACE_MODE_TEAM
        else WORKSPACE_MODE_TEAM
    )
    set_workspace_booking_mode(new_mode, demo_owner_id=demo_owner_id)
    await update_static_screen_from_callback(
        callback,
        "👥 Мастера\n\n"
        f"Режим обновлён: {'команда с выбором мастера' if new_mode == WORKSPACE_MODE_TEAM else 'один мастер без выбора'}.\n"
        "Данные мастеров сохранены. При необходимости режим можно менять в любой момент.",
        reply_markup=admin_masters_kb(demo_owner_id=demo_owner_id),
    )
    await callback.answer("Режим обновлён")


@router.message(AdminFSM.waiting_new_master_name)
async def admin_master_add_finish(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    if not is_workspace_admin(message.from_user.id):
        await state.clear()
        return
    demo_owner_id = resolve_demo_owner_id_for_user(message.from_user.id)
    name = (message.text or "").strip()
    if len(name) < 2:
        await send_static_screen(
            message.from_user.id,
            "👤 Имя слишком короткое.\n\nВведите минимум 2 символа.",
            reply_markup=admin_back_kb(back_callback="admin:masters", back_text="⬅️ К мастерам"),
        )
        return
    ok = create_master(name, demo_owner_id=demo_owner_id)
    await state.clear()
    if ok:
        created_master = next(
            (row for row in reversed(get_all_masters(demo_owner_id=demo_owner_id)) if str(row["name"]) == name),
            None,
        )
        if created_master is not None:
            await open_admin_master_detail_screen(
                message.from_user.id,
                int(created_master["id"]),
                demo_owner_id=demo_owner_id,
            )
            return
    await send_static_screen(
        message.from_user.id,
        "✅ Мастер добавлен." if ok else "Не удалось добавить мастера.\n\nВозможно, такое имя уже есть.",
        reply_markup=admin_masters_kb(demo_owner_id=demo_owner_id),
    )


@router.callback_query(F.data.startswith("adm_master_open:"))
async def admin_master_open(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    await state.clear()
    _, master_id_raw = callback.data.split(":")
    if not master_id_raw.isdigit():
        await callback.answer("Некорректный ID", show_alert=True)
        return
    master_id = int(master_id_raw)
    await refresh_admin_master_detail_callback(
        callback,
        master_id,
        demo_owner_id=resolve_demo_owner_id_for_user(callback.from_user.id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("adm_master_primary:"))
async def admin_master_primary_callback(callback: CallbackQuery) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    _, master_id_raw = callback.data.split(":")
    if not master_id_raw.isdigit():
        await callback.answer("Некорректный ID", show_alert=True)
        return
    master_id = int(master_id_raw)
    ok = set_primary_master(
        master_id,
        demo_owner_id=resolve_demo_owner_id_for_user(callback.from_user.id),
    )
    await refresh_admin_master_detail_callback(
        callback,
        master_id,
        demo_owner_id=resolve_demo_owner_id_for_user(callback.from_user.id),
    )
    await callback.answer("Основной мастер обновлён" if ok else "Не удалось обновить", show_alert=not ok)


@router.callback_query(F.data.startswith("adm_master_move:"))
async def admin_master_move_callback(callback: CallbackQuery) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    parts = callback.data.split(":")
    if len(parts) != 3:
        await callback.answer("Некорректная сортировка", show_alert=True)
        return
    _, direction, master_id_raw = parts
    if direction not in {"up", "down"} or not master_id_raw.isdigit():
        await callback.answer("Некорректная сортировка", show_alert=True)
        return
    master_id = int(master_id_raw)
    ok, text = move_master(
        master_id,
        direction,
        demo_owner_id=resolve_demo_owner_id_for_user(callback.from_user.id),
    )
    await refresh_admin_master_detail_callback(
        callback,
        master_id,
        demo_owner_id=resolve_demo_owner_id_for_user(callback.from_user.id),
    )
    await callback.answer(text if ok else text, show_alert=not ok)


@router.callback_query(F.data.startswith("adm_master_rename:"))
async def admin_master_rename_start(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    _, master_id_raw = callback.data.split(":")
    if not master_id_raw.isdigit():
        await callback.answer("Некорректный ID", show_alert=True)
        return
    await state.set_state(AdminFSM.waiting_edit_master_name)
    await state.update_data(edit_master_id=int(master_id_raw))
    await update_static_screen_from_callback(
        callback,
        "Введите новое имя.",
        reply_markup=admin_back_kb(
            back_callback=f"adm_master_open:{master_id_raw}",
            back_text="⬅️ К мастеру",
        ),
    )
    await callback.answer()


@router.message(AdminFSM.waiting_edit_master_name)
async def admin_master_rename_finish(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    if not is_workspace_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    master_id = data.get("edit_master_id")
    if not master_id:
        await state.clear()
        await send_static_screen(
            message.from_user.id,
            "Мастер не выбран.",
            reply_markup=admin_back_kb(back_callback="admin:masters", back_text="⬅️ К мастерам"),
        )
        return
    name = (message.text or "").strip()
    if len(name) < 2:
        await send_static_screen(
            message.from_user.id,
            "??? ?????? ????????? ??????? 2 ???????.",
            reply_markup=admin_back_kb(
                back_callback=f"adm_master_open:{int(master_id)}",
                back_text="⬅️ К мастеру",
            ),
        )
        return
    ok = update_master_name(
        int(master_id),
        name,
        demo_owner_id=resolve_demo_owner_id_for_user(message.from_user.id),
    )
    await state.clear()
    if ok:
        await open_admin_master_detail_screen(
            message.from_user.id,
            int(master_id),
            demo_owner_id=resolve_demo_owner_id_for_user(message.from_user.id),
        )
    else:
        await send_static_screen(
            message.from_user.id,
            "Не удалось обновить имя мастера.",
            reply_markup=admin_master_detail_kb(
                int(master_id),
                demo_owner_id=resolve_demo_owner_id_for_user(message.from_user.id),
            ),
        )


@router.callback_query(F.data.startswith("adm_master_spec:"))
async def admin_master_spec_start(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    _, master_id_raw = callback.data.split(":")
    if not master_id_raw.isdigit():
        await callback.answer("Некорректный ID", show_alert=True)
        return
    master_id = int(master_id_raw)
    master = get_master_by_id(master_id, demo_owner_id=resolve_demo_owner_id_for_user(callback.from_user.id))
    if master is None:
        await callback.answer("Мастер не найден", show_alert=True)
        return
    await state.set_state(AdminFSM.waiting_edit_master_specialization)
    await state.update_data(edit_master_id=master_id)
    await update_static_screen_from_callback(
        callback,
        "Введите специализацию.\n\n"
        f"Сейчас: {get_master_specialization_from_row(master)}",
        reply_markup=admin_back_kb(
            back_callback=f"adm_master_open:{master_id}",
            back_text="⬅️ К мастеру",
        ),
    )
    await callback.answer()


@router.message(AdminFSM.waiting_edit_master_specialization)
async def admin_master_spec_finish(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    if not is_workspace_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    master_id = int(data.get("edit_master_id") or 0)
    if not master_id:
        await state.clear()
        await send_static_screen(
            message.from_user.id,
            "Мастер не выбран.",
            reply_markup=admin_back_kb(back_callback="admin:masters", back_text="⬅️ К мастерам"),
        )
        return
    specialization = (message.text or "").strip()
    if len(specialization) < 2:
        await send_static_screen(
            message.from_user.id,
            "Укажите специализацию минимум из 2 символов.",
            reply_markup=admin_back_kb(back_callback=f"adm_master_open:{master_id}", back_text="⬅️ К мастеру"),
        )
        return
    update_master_specialization(master_id, specialization, demo_owner_id=resolve_demo_owner_id_for_user(message.from_user.id))
    await state.clear()
    await open_admin_master_detail_screen(
        message.from_user.id,
        master_id,
        demo_owner_id=resolve_demo_owner_id_for_user(message.from_user.id),
    )


@router.callback_query(F.data.startswith("adm_master_desc:"))
async def admin_master_description_start(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    _, master_id_raw = callback.data.split(":")
    if not master_id_raw.isdigit():
        await callback.answer("Некорректный ID", show_alert=True)
        return
    master_id = int(master_id_raw)
    master = get_master_by_id(master_id, demo_owner_id=resolve_demo_owner_id_for_user(callback.from_user.id))
    if master is None:
        await callback.answer("Мастер не найден", show_alert=True)
        return
    await state.set_state(AdminFSM.waiting_edit_master_description)
    await state.update_data(edit_master_id=master_id)
    await update_static_screen_from_callback(
        callback,
        "Введите короткое описание.\n\n"
        f"Сейчас: {get_master_description_from_row(master)}",
        reply_markup=admin_back_kb(
            back_callback=f"adm_master_open:{master_id}",
            back_text="⬅️ К мастеру",
        ),
    )
    await callback.answer()


@router.message(AdminFSM.waiting_edit_master_description)
async def admin_master_description_finish(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    if not is_workspace_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    master_id = int(data.get("edit_master_id") or 0)
    if not master_id:
        await state.clear()
        await send_static_screen(
            message.from_user.id,
            "Мастер не выбран.",
            reply_markup=admin_back_kb(back_callback="admin:masters", back_text="⬅️ К мастерам"),
        )
        return
    description = (message.text or "").strip()
    if len(description) < 10:
        await send_static_screen(
            message.from_user.id,
            "Описание должно быть чуть подробнее — минимум 10 символов.",
            reply_markup=admin_back_kb(back_callback=f"adm_master_open:{master_id}", back_text="⬅️ К мастеру"),
        )
        return
    update_master_description(master_id, description, demo_owner_id=resolve_demo_owner_id_for_user(message.from_user.id))
    await state.clear()
    await open_admin_master_detail_screen(
        message.from_user.id,
        master_id,
        demo_owner_id=resolve_demo_owner_id_for_user(message.from_user.id),
    )


@router.callback_query(F.data.startswith("adm_master_photo:"))
async def admin_master_photo_start(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    _, master_id_raw = callback.data.split(":")
    if not master_id_raw.isdigit():
        await callback.answer("Некорректный ID", show_alert=True)
        return
    master_id = int(master_id_raw)
    if get_master_by_id(master_id, demo_owner_id=resolve_demo_owner_id_for_user(callback.from_user.id)) is None:
        await callback.answer("Мастер не найден", show_alert=True)
        return
    await state.set_state(AdminFSM.waiting_edit_master_photo)
    await state.update_data(edit_master_id=master_id)
    await update_static_screen_from_callback(
        callback,
        "Отправьте фото мастера одним сообщением.\n"
        "Можно отправить и прямую ссылку вида https://...\n\n"
        "Это фото будет показываться в карточке мастера клиенту.",
        reply_markup=admin_back_kb(
            back_callback=f"adm_master_open:{master_id}",
            back_text="⬅️ К мастеру",
        ),
    )
    await callback.answer()


@router.message(AdminFSM.waiting_edit_master_photo)
async def admin_master_photo_finish(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    if not is_workspace_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    master_id = int(data.get("edit_master_id") or 0)
    if not master_id:
        await state.clear()
        await send_static_screen(
            message.from_user.id,
            "Мастер не выбран.",
            reply_markup=admin_back_kb(back_callback="admin:masters", back_text="⬅️ К мастерам"),
        )
        return
    photo_ref = None
    if message.photo:
        photo_ref = message.photo[-1].file_id
    else:
        url = (message.text or "").strip()
        if url.startswith("http://") or url.startswith("https://"):
            photo_ref = url
    if not photo_ref:
        await send_static_screen(
            message.from_user.id,
            "Отправьте фото или корректную ссылку вида https://...",
            reply_markup=admin_back_kb(back_callback=f"adm_master_open:{master_id}", back_text="⬅️ К мастеру"),
        )
        return
    update_master_photo(master_id, photo_ref, demo_owner_id=resolve_demo_owner_id_for_user(message.from_user.id))
    await state.clear()
    await open_admin_master_detail_screen(
        message.from_user.id,
        master_id,
        demo_owner_id=resolve_demo_owner_id_for_user(message.from_user.id),
    )


@router.callback_query(F.data.startswith("adm_master_photo_clear:"))
async def admin_master_photo_clear_callback(callback: CallbackQuery) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    _, master_id_raw = callback.data.split(":")
    if not master_id_raw.isdigit():
        await callback.answer("Некорректный ID", show_alert=True)
        return
    master_id = int(master_id_raw)
    update_master_photo(master_id, None, demo_owner_id=resolve_demo_owner_id_for_user(callback.from_user.id))
    await refresh_admin_master_detail_callback(
        callback,
        master_id,
        demo_owner_id=resolve_demo_owner_id_for_user(callback.from_user.id),
    )
    await callback.answer("Фото удалено")


@router.callback_query(F.data.startswith("adm_master_toggle:"))
async def admin_master_toggle_callback(callback: CallbackQuery) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    _, master_id_raw = callback.data.split(":")
    if not master_id_raw.isdigit():
        await callback.answer("Некорректный ID", show_alert=True)
        return
    master_id = int(master_id_raw)
    ok, text = toggle_master_active(
        master_id,
        demo_owner_id=resolve_demo_owner_id_for_user(callback.from_user.id),
    )
    await refresh_admin_master_detail_callback(
        callback,
        master_id,
        demo_owner_id=resolve_demo_owner_id_for_user(callback.from_user.id),
    )
    await callback.answer("Обновлено" if ok else text, show_alert=not ok)


@router.callback_query(F.data.startswith("adm_master_delete:"))
async def admin_master_delete_callback(callback: CallbackQuery) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    _, master_id_raw = callback.data.split(":")
    if not master_id_raw.isdigit():
        await callback.answer("Некорректный ID", show_alert=True)
        return
    ok, text = delete_master(
        int(master_id_raw),
        demo_owner_id=resolve_demo_owner_id_for_user(callback.from_user.id),
    )
    if not ok:
        await update_static_screen_from_callback(
            callback,
            text,
            reply_markup=admin_master_detail_kb(
                int(master_id_raw),
                demo_owner_id=resolve_demo_owner_id_for_user(callback.from_user.id),
            ),
        )
        await callback.answer("Не удалось", show_alert=True)
        return
    await update_static_screen_from_callback(
        callback,
        text,
        reply_markup=admin_masters_kb(demo_owner_id=resolve_demo_owner_id_for_user(callback.from_user.id)),
    )
    await callback.answer("Удалено")


@router.callback_query(F.data == "admin:services")
async def admin_services_start(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    await state.clear()
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    masters = get_active_masters(demo_owner_id=demo_owner_id)
    if not masters:
        await update_static_screen_from_callback(
            callback,
            "👤 Сначала добавьте хотя бы одного активного специалиста.",
            reply_markup=admin_back_kb(
                back_callback="admin:section:content",
                back_text="⬅️ К разделу «Контент»",
            ),
        )
        await callback.answer()
        return
    if is_master_choice_enabled(demo_owner_id=demo_owner_id):
        await update_static_screen_from_callback(
            callback,
            "🧾 Услуги\n\nВыберите мастера.",
            reply_markup=admin_service_master_select_kb(demo_owner_id=demo_owner_id),
        )
        await callback.answer()
        return

    master = get_primary_master(demo_owner_id=demo_owner_id, active_only=True) or masters[0]
    master_id = int(master["id"])
    await state.update_data(admin_service_master_id=master_id)
    await update_static_screen_from_callback(
        callback,
        format_admin_services_text(master_id, demo_owner_id=demo_owner_id),
        reply_markup=admin_services_kb(master_id, demo_owner_id=demo_owner_id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("adm_svc_master:"))
async def admin_service_master_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    _, master_id_raw = callback.data.split(":")
    if not master_id_raw.isdigit():
        await callback.answer("Некорректный ID", show_alert=True)
        return
    master_id = int(master_id_raw)
    master = get_master_by_id(master_id, demo_owner_id=demo_owner_id)
    if master is None or int(master["is_active"]) != 1:
        await callback.answer("Мастер недоступен", show_alert=True)
        return
    await state.clear()
    await state.update_data(admin_service_master_id=master_id)
    await update_static_screen_from_callback(
        callback,
        format_admin_services_text(master_id, demo_owner_id=demo_owner_id),
        reply_markup=admin_services_kb(master_id, demo_owner_id=demo_owner_id),
    )
    await callback.answer()


@router.callback_query(F.data == "adm_svc_add")
async def admin_service_add_start(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    data = await state.get_data()
    master_id = int(data.get("admin_service_master_id") or 0)
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    if not master_id:
        master = get_primary_master(demo_owner_id=demo_owner_id, active_only=True)
        if master is None:
            await callback.answer("Сначала выберите мастера", show_alert=True)
            return
        master_id = int(master["id"])
        await state.update_data(admin_service_master_id=master_id)
    await state.set_state(AdminFSM.waiting_new_service_name)
    await update_static_screen_from_callback(
        callback,
        "🧾 Новая услуга\n\nВведите название услуги.",
        reply_markup=admin_back_kb(back_callback=f"adm_svc_master:{master_id}", back_text="⬅️ К услугам"),
    )
    await callback.answer()


@router.message(AdminFSM.waiting_new_service_name)
async def admin_service_add_name(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    if not is_workspace_admin(message.from_user.id):
        await state.clear()
        return
    name = (message.text or "").strip()
    data = await state.get_data()
    master_id = int(data.get("admin_service_master_id") or 0)
    if len(name) < 2:
        await send_static_screen(
            message.from_user.id,
            "🧾 Название слишком короткое.\n\nВведите минимум 2 символа.",
            reply_markup=admin_back_kb(back_callback=f"adm_svc_master:{master_id}" if master_id else "admin:services", back_text="⬅️ К услугам"),
        )
        return
    await state.update_data(new_service_name=name)
    await state.set_state(AdminFSM.waiting_new_service_price)
    await send_static_screen(
        message.from_user.id,
        "💳 Стоимость услуги\n\nВведите цену целым числом в USD.",
        reply_markup=admin_back_kb(back_callback=f"adm_svc_master:{master_id}" if master_id else "admin:services", back_text="⬅️ К услугам"),
    )


@router.message(AdminFSM.waiting_new_service_price)
async def admin_service_add_price(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    if not is_workspace_admin(message.from_user.id):
        await state.clear()
        return
    raw = (message.text or "").strip()
    data = await state.get_data()
    master_id = int(data.get("admin_service_master_id") or 0)
    if not raw.isdigit() or int(raw) <= 0:
        await send_static_screen(
            message.from_user.id,
            "💳 Стоимость должна быть положительным целым числом.",
            reply_markup=admin_back_kb(back_callback=f"adm_svc_master:{master_id}" if master_id else "admin:services", back_text="⬅️ К услугам"),
        )
        return
    name = data.get("new_service_name")
    if not name:
        await state.set_state(AdminFSM.waiting_new_service_name)
        await send_static_screen(
            message.from_user.id,
            "🧾 Название услуги\n\nВведите название услуги заново.",
            reply_markup=admin_back_kb(back_callback=f"adm_svc_master:{master_id}" if master_id else "admin:services", back_text="⬅️ К услугам"),
        )
        return
    ok = create_service(
        str(name),
        int(raw),
        demo_owner_id=resolve_demo_owner_id_for_user(message.from_user.id),
        master_id=master_id,
    )
    await state.clear()
    await send_static_screen(
        message.from_user.id,
        "✅ Услуга добавлена." if ok else "Не удалось добавить услугу.\n\nВозможно, такое название уже есть.",
        reply_markup=admin_services_kb(master_id, demo_owner_id=resolve_demo_owner_id_for_user(message.from_user.id)),
    )


@router.callback_query(F.data.startswith("adm_svc_open:"))
async def admin_service_open(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    _, service_id_raw = callback.data.split(":")
    if not service_id_raw.isdigit():
        await callback.answer("Некорректный ID", show_alert=True)
        return
    service_id = int(service_id_raw)
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    service = get_service_by_id(service_id, demo_owner_id=demo_owner_id)
    if service is None:
        await callback.answer("Услуга не найдена", show_alert=True)
        return
    await state.clear()
    await state.update_data(admin_service_master_id=int(service["master_id"]))
    await update_static_screen_from_callback(
        callback,
        format_service_detail_text(service_id, demo_owner_id=demo_owner_id),
        reply_markup=admin_service_detail_kb(service_id, demo_owner_id=demo_owner_id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("adm_svc_move:"))
async def admin_service_move_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    parts = callback.data.split(":")
    if len(parts) != 3:
        await callback.answer("Некорректная сортировка", show_alert=True)
        return
    _, direction, service_id_raw = parts
    if direction not in {"up", "down"} or not service_id_raw.isdigit():
        await callback.answer("Некорректная сортировка", show_alert=True)
        return
    service_id = int(service_id_raw)
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    service = get_service_by_id(service_id, demo_owner_id=demo_owner_id)
    if service is None:
        await callback.answer("Услуга не найдена", show_alert=True)
        return
    await state.update_data(admin_service_master_id=int(service["master_id"]))
    ok, text = move_service(service_id, direction, demo_owner_id=demo_owner_id)
    await update_static_screen_from_callback(
        callback,
        format_service_detail_text(service_id, demo_owner_id=demo_owner_id),
        reply_markup=admin_service_detail_kb(service_id, demo_owner_id=demo_owner_id),
    )
    await callback.answer(text, show_alert=not ok)


@router.callback_query(F.data.startswith("adm_svc_move_hint:"))
async def admin_service_move_hint_callback(callback: CallbackQuery) -> None:
    await callback.answer(
        "Порядок услуги можно менять, если это действительно нужно. Сейчас основное — название, цена и видимость.",
        show_alert=True,
    )


@router.callback_query(F.data.startswith("adm_svc_rename:"))
async def admin_service_rename_start(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    _, service_id_raw = callback.data.split(":")
    if not service_id_raw.isdigit():
        await callback.answer("Некорректный ID", show_alert=True)
        return
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    service = get_service_by_id(int(service_id_raw), demo_owner_id=demo_owner_id)
    master_id = int(service["master_id"]) if service is not None else 0
    await state.set_state(AdminFSM.waiting_edit_service_name)
    await state.update_data(edit_service_id=int(service_id_raw), admin_service_master_id=master_id)
    await update_static_screen_from_callback(
        callback,
        "✏️ Название услуги\n\nВведите новое название.",
        reply_markup=admin_back_kb(
            back_callback=f"adm_svc_open:{service_id_raw}",
            back_text="⬅️ К услуге",
        ),
    )
    await callback.answer()


@router.message(AdminFSM.waiting_edit_service_name)
async def admin_service_rename_finish(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    if not is_workspace_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    service_id = data.get("edit_service_id")
    if not service_id:
        await state.clear()
        await send_static_screen(
            message.from_user.id,
            "🧾 Услуга не выбрана.",
            reply_markup=admin_back_kb(back_callback="admin:services", back_text="⬅️ К услугам"),
        )
        return
    name = (message.text or "").strip()
    ok = update_service_name(
        int(service_id),
        name,
        demo_owner_id=resolve_demo_owner_id_for_user(message.from_user.id),
    )
    await state.clear()
    await send_static_screen(
        message.from_user.id,
        "✅ Название обновлено." if ok else "Не удалось обновить название.",
        reply_markup=admin_service_detail_kb(
            int(service_id),
            demo_owner_id=resolve_demo_owner_id_for_user(message.from_user.id),
        ),
    )


@router.callback_query(F.data.startswith("adm_svc_price:"))
async def admin_service_price_start(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    _, service_id_raw = callback.data.split(":")
    if not service_id_raw.isdigit():
        await callback.answer("Некорректный ID", show_alert=True)
        return
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    service = get_service_by_id(int(service_id_raw), demo_owner_id=demo_owner_id)
    master_id = int(service["master_id"]) if service is not None else 0
    await state.set_state(AdminFSM.waiting_edit_service_price)
    await state.update_data(edit_service_id=int(service_id_raw), admin_service_master_id=master_id)
    await update_static_screen_from_callback(
        callback,
        "💳 Стоимость услуги\n\nВведите новую цену целым числом в USD.",
        reply_markup=admin_back_kb(
            back_callback=f"adm_svc_open:{service_id_raw}",
            back_text="⬅️ К услуге",
        ),
    )
    await callback.answer()


@router.message(AdminFSM.waiting_edit_service_price)
async def admin_service_price_finish(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    if not is_workspace_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    service_id = data.get("edit_service_id")
    raw = (message.text or "").strip()
    if not service_id or not raw.isdigit() or int(raw) <= 0:
        back_cb = f"adm_svc_open:{service_id}" if service_id else "admin:services"
        await send_static_screen(
            message.from_user.id,
            "💳 Введите корректную стоимость.",
            reply_markup=admin_back_kb(
                back_callback=back_cb,
                back_text="⬅️ Назад",
            ),
        )
        return
    ok = update_service_price(
        int(service_id),
        int(raw),
        demo_owner_id=resolve_demo_owner_id_for_user(message.from_user.id),
    )
    await state.clear()
    await send_static_screen(
        message.from_user.id,
        "✅ Стоимость обновлена." if ok else "Не удалось обновить стоимость.",
        reply_markup=admin_service_detail_kb(
            int(service_id),
            demo_owner_id=resolve_demo_owner_id_for_user(message.from_user.id),
        ),
    )


@router.callback_query(F.data.startswith("adm_svc_toggle:"))
async def admin_service_toggle_callback(callback: CallbackQuery) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    _, service_id_raw = callback.data.split(":")
    if not service_id_raw.isdigit():
        await callback.answer("Некорректный ID", show_alert=True)
        return
    service_id = int(service_id_raw)
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    toggle_service_active(service_id, demo_owner_id=demo_owner_id)
    await update_static_screen_from_callback(
        callback,
        format_service_detail_text(service_id, demo_owner_id=demo_owner_id),
        reply_markup=admin_service_detail_kb(service_id, demo_owner_id=demo_owner_id),
    )
    await callback.answer("Статус обновлён")


@router.callback_query(F.data.startswith("adm_svc_delete:"))
async def admin_service_delete_callback(callback: CallbackQuery) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    _, service_id_raw = callback.data.split(":")
    if not service_id_raw.isdigit():
        await callback.answer("Некорректный ID", show_alert=True)
        return
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    service = get_service_by_id(int(service_id_raw), demo_owner_id=demo_owner_id)
    master_id = int(service["master_id"]) if service is not None else 0
    deleted = delete_service(int(service_id_raw), demo_owner_id=demo_owner_id)
    await update_static_screen_from_callback(
        callback,
        "✅ Услуга удалена." if deleted else "🧾 Услуга не найдена.",
        reply_markup=admin_services_kb(master_id, demo_owner_id=demo_owner_id),
    )
    await callback.answer()


@router.callback_query(F.data == "admin:portfolio")
async def admin_portfolio_start(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    await state.clear()
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    masters = get_active_masters(demo_owner_id=demo_owner_id)
    if not masters:
        await update_static_screen_from_callback(
            callback,
            "👤 Сначала добавьте хотя бы одного активного специалиста.",
            reply_markup=admin_back_kb(
                back_callback="admin:section:content",
                back_text="⬅️ К разделу «Контент»",
            ),
        )
        await callback.answer()
        return
    if is_master_choice_enabled(demo_owner_id=demo_owner_id):
        await update_static_screen_from_callback(
            callback,
            "🖼 Портфолио\n\nВыберите мастера.",
            reply_markup=admin_portfolio_master_select_kb(demo_owner_id=demo_owner_id),
        )
        await callback.answer()
        return

    master = get_primary_master(demo_owner_id=demo_owner_id, active_only=True) or masters[0]
    master_id = int(master["id"])
    await state.update_data(admin_portfolio_master_id=master_id)
    await update_static_screen_from_callback(
        callback,
        f"🖼 Портфолио\n\n👤 Специалист: {master['name']}\nВыберите категорию.",
        reply_markup=admin_portfolio_categories_kb(master_id, demo_owner_id=demo_owner_id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("adm_port_master:"))
async def admin_portfolio_master_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    _, master_id_raw = callback.data.split(":")
    if not master_id_raw.isdigit():
        await callback.answer("Некорректный ID", show_alert=True)
        return
    master_id = int(master_id_raw)
    master = get_master_by_id(master_id, demo_owner_id=demo_owner_id)
    if master is None or int(master["is_active"]) != 1:
        await callback.answer("Мастер недоступен", show_alert=True)
        return
    await state.clear()
    await state.update_data(admin_portfolio_master_id=master_id)
    await update_static_screen_from_callback(
        callback,
        f"🖼 Портфолио\n\n👤 Специалист: {master['name']}\nВыберите категорию.",
        reply_markup=admin_portfolio_categories_kb(master_id, demo_owner_id=demo_owner_id),
    )
    await callback.answer()


@router.callback_query(F.data == "adm_port_cat_add")
async def admin_portfolio_category_add_start(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    data = await state.get_data()
    master_id = int(data.get("admin_portfolio_master_id") or 0)
    await state.set_state(AdminFSM.waiting_new_portfolio_category_title)
    await update_static_screen_from_callback(
        callback,
        "Введите название новой категории портфолио:",
        reply_markup=admin_back_kb(
            back_callback=f"adm_port_master:{master_id}" if master_id else "admin:portfolio",
            back_text="⬅️ К категориям",
        ),
    )
    await callback.answer()


@router.message(AdminFSM.waiting_new_portfolio_category_title)
async def admin_portfolio_category_add_finish(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    if not is_workspace_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    master_id = int(data.get("admin_portfolio_master_id") or 0)
    title = (message.text or "").strip()
    if len(title) < 2:
        await send_static_screen(
            message.from_user.id,
            "🖼 Название слишком короткое.\n\nВведите минимум 2 символа.",
            reply_markup=admin_back_kb(
                back_callback=f"adm_port_master:{master_id}" if master_id else "admin:portfolio",
                back_text="⬅️ К категориям",
            ),
        )
        return
    code = create_portfolio_category(
        title,
        demo_owner_id=resolve_demo_owner_id_for_user(message.from_user.id),
        master_id=master_id,
    )
    await state.clear()
    if code is None:
        await send_static_screen(
            message.from_user.id,
            "Не удалось добавить категорию.\n\nПопробуйте другое название.",
            reply_markup=admin_portfolio_categories_kb(
                master_id,
                demo_owner_id=resolve_demo_owner_id_for_user(message.from_user.id),
            ),
        )
        return
    await send_static_screen(
        message.from_user.id,
        format_portfolio_category_admin_text(
            code,
            demo_owner_id=resolve_demo_owner_id_for_user(message.from_user.id),
            master_id=master_id,
        ),
        reply_markup=admin_portfolio_category_kb(
            code,
            master_id=master_id,
            demo_owner_id=resolve_demo_owner_id_for_user(message.from_user.id),
        ),
    )


@router.callback_query(F.data.startswith("adm_port_cat:"))
async def admin_portfolio_category_open(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    data = await state.get_data()
    master_id = int(data.get("admin_portfolio_master_id") or 0)
    _, category = callback.data.split(":")
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    if get_portfolio_category_by_code(category, demo_owner_id=demo_owner_id, master_id=master_id) is None:
        await callback.answer("Категория не найдена.", show_alert=True)
        return
    await state.update_data(admin_portfolio_master_id=master_id)
    await update_static_screen_from_callback(
        callback,
        format_portfolio_category_admin_text(category, demo_owner_id=demo_owner_id, master_id=master_id),
        reply_markup=admin_portfolio_category_kb(category, master_id=master_id, demo_owner_id=demo_owner_id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("adm_port_toggle:"))
async def admin_portfolio_category_toggle(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    data = await state.get_data()
    master_id = int(data.get("admin_portfolio_master_id") or 0)
    _, category = callback.data.split(":")
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    category_row = get_portfolio_category_by_code(category, demo_owner_id=demo_owner_id, master_id=master_id)
    if category_row is None:
        await callback.answer("Категория не найдена.", show_alert=True)
        return
    toggle_portfolio_category_active(category, demo_owner_id=demo_owner_id, master_id=master_id)
    await update_static_screen_from_callback(
        callback,
        format_portfolio_category_admin_text(category, demo_owner_id=demo_owner_id, master_id=master_id),
        reply_markup=admin_portfolio_category_kb(category, master_id=master_id, demo_owner_id=demo_owner_id),
    )
    await callback.answer("Статус категории обновлен")


@router.callback_query(F.data.startswith("adm_port_move:"))
async def admin_portfolio_category_move(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    parts = callback.data.split(":", maxsplit=2)
    if len(parts) != 3:
        await callback.answer("Некорректная сортировка", show_alert=True)
        return
    _, direction, category = parts
    if direction not in {"up", "down"}:
        await callback.answer("Некорректная сортировка", show_alert=True)
        return
    data = await state.get_data()
    master_id = int(data.get("admin_portfolio_master_id") or 0)
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    if get_portfolio_category_by_code(category, demo_owner_id=demo_owner_id, master_id=master_id) is None:
        await callback.answer("Категория не найдена.", show_alert=True)
        return
    ok, text = move_portfolio_category(
        category,
        direction,
        demo_owner_id=demo_owner_id,
        master_id=master_id,
    )
    await update_static_screen_from_callback(
        callback,
        format_portfolio_category_admin_text(category, demo_owner_id=demo_owner_id, master_id=master_id),
        reply_markup=admin_portfolio_category_kb(category, master_id=master_id, demo_owner_id=demo_owner_id),
    )
    await callback.answer(text, show_alert=not ok)


@router.callback_query(F.data.startswith("adm_port_move_hint:"))
async def admin_portfolio_move_hint_callback(callback: CallbackQuery) -> None:
    await callback.answer(
        "Порядок категории можно настроить позже. На старте важнее название, фото и видимость для клиента.",
        show_alert=True,
    )


@router.callback_query(F.data.startswith("adm_port_del_cat:"))
async def admin_portfolio_category_delete(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    data = await state.get_data()
    master_id = int(data.get("admin_portfolio_master_id") or 0)
    _, category = callback.data.split(":")
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    ok, text = delete_portfolio_category(category, demo_owner_id=demo_owner_id, master_id=master_id)
    if not ok:
        await callback.answer(text, show_alert=True)
        await update_static_screen_from_callback(
            callback,
            format_portfolio_category_admin_text(category, demo_owner_id=demo_owner_id, master_id=master_id),
            reply_markup=admin_portfolio_category_kb(category, master_id=master_id, demo_owner_id=demo_owner_id),
        )
        return
    await update_static_screen_from_callback(
        callback,
        text,
        reply_markup=admin_portfolio_categories_kb(master_id, demo_owner_id=demo_owner_id),
    )
    await callback.answer("Удалено")


@router.callback_query(F.data.startswith("adm_port_rename:"))
async def admin_portfolio_rename_start(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    data = await state.get_data()
    master_id = int(data.get("admin_portfolio_master_id") or 0)
    _, category = callback.data.split(":")
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    category_row = get_portfolio_category_by_code(category, demo_owner_id=demo_owner_id, master_id=master_id)
    if category_row is None:
        await callback.answer("Категория не найдена.", show_alert=True)
        return

    await state.set_state(AdminFSM.waiting_edit_portfolio_category_title)
    await state.update_data(edit_portfolio_category=category, admin_portfolio_master_id=master_id)
    await update_static_screen_from_callback(
        callback,
        (
            f"Текущее название: «{category_row['title']}»\n\n"
            "Введите новое название категории:"
        ),
        reply_markup=admin_back_kb(
            back_callback=f"adm_port_cat:{category}",
            back_text="⬅️ К категории",
        ),
    )
    await callback.answer()


@router.message(AdminFSM.waiting_edit_portfolio_category_title)
async def admin_portfolio_rename_finish(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    if not is_workspace_admin(message.from_user.id):
        await state.clear()
        return

    data = await state.get_data()
    category = data.get("edit_portfolio_category")
    master_id = int(data.get("admin_portfolio_master_id") or 0)
    if not category:
        await state.clear()
        await send_static_screen(
            message.from_user.id,
            "🖼 Категория не выбрана.",
            reply_markup=admin_back_kb(back_callback="admin:portfolio", back_text="⬅️ К категориям"),
        )
        return

    new_title = (message.text or "").strip()
    if len(new_title) < 2:
        await send_static_screen(
            message.from_user.id,
            "🖼 Название слишком короткое.\n\nВведите минимум 2 символа.",
            reply_markup=admin_back_kb(
                back_callback=f"adm_port_cat:{category}",
                back_text="⬅️ К категории",
            ),
        )
        return

    updated = update_portfolio_category_title(
        category,
        new_title,
        demo_owner_id=resolve_demo_owner_id_for_user(message.from_user.id),
        master_id=master_id,
    )
    await state.clear()
    if not updated:
        await send_static_screen(
            message.from_user.id,
            "Не удалось обновить название категории.",
            reply_markup=admin_back_kb(back_callback="admin:portfolio", back_text="⬅️ К категориям"),
        )
        return

    await send_static_screen(
        message.from_user.id,
        format_portfolio_category_admin_text(
            category,
            demo_owner_id=resolve_demo_owner_id_for_user(message.from_user.id),
            master_id=master_id,
        ),
        reply_markup=admin_portfolio_category_kb(
            category,
            master_id=master_id,
            demo_owner_id=resolve_demo_owner_id_for_user(message.from_user.id),
        ),
    )


@router.callback_query(F.data.startswith("adm_port_add:"))
async def admin_portfolio_add_start(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    data = await state.get_data()
    master_id = int(data.get("admin_portfolio_master_id") or 0)
    _, category = callback.data.split(":")
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    if get_portfolio_category_by_code(category, demo_owner_id=demo_owner_id, master_id=master_id) is None:
        await callback.answer("Категория не найдена.", show_alert=True)
        return
    await state.set_state(AdminFSM.waiting_portfolio_add_url)
    await state.update_data(portfolio_category=category, admin_portfolio_master_id=master_id)
    await update_static_screen_from_callback(
        callback,
        "Отправьте фото в чат или URL изображения для добавления.",
        reply_markup=admin_portfolio_add_kb(category, master_id),
    )
    await callback.answer()


@router.message(AdminFSM.waiting_portfolio_add_url)
async def admin_portfolio_add_finish(message: Message, state: FSMContext) -> None:
    await try_delete_user_message(message)
    if not is_workspace_admin(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    category = data.get("portfolio_category")
    master_id = int(data.get("admin_portfolio_master_id") or 0)
    demo_owner_id = resolve_demo_owner_id_for_user(message.from_user.id)
    if not category or get_portfolio_category_by_code(category, demo_owner_id=demo_owner_id, master_id=master_id) is None:
        await send_static_screen(
            message.from_user.id,
            "Категория не найдена.",
            reply_markup=admin_back_kb(),
        )
        await state.clear()
        return

    media_ref = None
    if message.photo:
        media_ref = message.photo[-1].file_id
    else:
        url = (message.text or "").strip()
        if url.startswith("http://") or url.startswith("https://"):
            media_ref = url

    if media_ref is None:
        await send_static_screen(
            message.from_user.id,
            "Отправьте фото или корректный URL вида https://...",
            reply_markup=admin_portfolio_add_kb(str(category), master_id),
        )
        return

    add_portfolio_item(
        str(category),
        media_ref,
        demo_owner_id=demo_owner_id,
        master_id=master_id,
    )
    await state.clear()
    await send_static_screen(
        message.from_user.id,
        format_portfolio_category_admin_text(str(category), demo_owner_id=demo_owner_id, master_id=master_id),
        reply_markup=admin_portfolio_category_kb(str(category), master_id=master_id, demo_owner_id=demo_owner_id),
    )


@router.callback_query(F.data.startswith("adm_port_del:"))
async def admin_portfolio_delete_start(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    data = await state.get_data()
    master_id = int(data.get("admin_portfolio_master_id") or 0)
    _, category = callback.data.split(":")
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    if get_portfolio_category_by_code(category, demo_owner_id=demo_owner_id, master_id=master_id) is None:
        await callback.answer("Категория не найдена.", show_alert=True)
        return
    await state.update_data(admin_portfolio_master_id=master_id)
    await update_static_screen_from_callback(
        callback,
        f"{format_portfolio_category_admin_text(category, demo_owner_id=demo_owner_id, master_id=master_id)}\n\nВыберите фото для удаления кнопкой ниже.",
        reply_markup=admin_portfolio_delete_kb(category, master_id, demo_owner_id=demo_owner_id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("adm_port_del_id:"))
async def admin_portfolio_delete_pick(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    data = await state.get_data()
    master_id = int(data.get("admin_portfolio_master_id") or 0)
    _, category, item_id_raw = callback.data.split(":")
    if not item_id_raw.isdigit():
        await callback.answer("Некорректный ID", show_alert=True)
        return
    item_id = int(item_id_raw)
    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    item = get_portfolio_item_by_id(item_id, demo_owner_id=demo_owner_id, master_id=master_id)
    if item is None or item["category"] != category:
        await callback.answer("Фото не найдено", show_alert=True)
        return

    delete_portfolio_item(item_id, demo_owner_id=demo_owner_id)
    await update_static_screen_from_callback(
        callback,
        f"Фото ID {item_id} удалено.\n\n{format_portfolio_category_admin_text(category, demo_owner_id=demo_owner_id, master_id=master_id)}",
        reply_markup=admin_portfolio_delete_kb(category, master_id, demo_owner_id=demo_owner_id),
    )
    await callback.answer("Удалено")


@router.callback_query(F.data == "admin:broadcast")
async def admin_broadcast_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return

    await state.set_state(AdminFSM.waiting_broadcast_message)
    await update_static_screen_from_callback(callback,
        "📢 Рассылка\n\n"
        "Шаг 1/2: отправьте сообщение, которое нужно разослать.\n"
        "Сначала покажу предпросмотр, потом попрошу подтверждение.",
        reply_markup=admin_back_kb(back_callback="admin:section:comms", back_text="⬅️ К коммуникациям"),
    )
    await callback.answer()


@router.message(AdminFSM.waiting_broadcast_message)
async def admin_broadcast_message_handler(message: Message, state: FSMContext) -> None:
    if not is_workspace_admin(message.from_user.id):
        await state.clear()
        return

    await state.set_state(AdminFSM.waiting_broadcast_confirm)
    await clear_aux_messages(message.from_user.id)
    source_chat_id = message.chat.id
    source_message_id = message.message_id
    try:
        preview = await message.copy_to(chat_id=message.from_user.id)
        track_aux_message(message.from_user.id, int(preview.message_id))
        source_chat_id = preview.chat.id
        source_message_id = preview.message_id
    except Exception:
        pass
    await state.update_data(
        broadcast_source_chat_id=source_chat_id,
        broadcast_source_message_id=source_message_id,
        broadcast_preview_message_id=source_message_id if int(source_chat_id) == int(message.from_user.id) else None,
    )
    await try_delete_user_message(message)
    await send_static_screen(
        message.from_user.id,
        "Шаг 2/2: проверьте предпросмотр выше и подтвердите отправку.",
        reply_markup=admin_broadcast_confirm_kb(),
        clear_aux=False,
    )


@router.callback_query(F.data == "admin:broadcast_send")
async def admin_broadcast_send_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return

    data = await state.get_data()
    source_chat_id = data.get("broadcast_source_chat_id")
    source_message_id = data.get("broadcast_source_message_id")
    preview_message_id = data.get("broadcast_preview_message_id")
    if not source_chat_id or not source_message_id:
        await state.clear()
        await update_static_screen_from_callback(
            callback,
            "Сообщение для рассылки не найдено. Начните заново.",
            reply_markup=admin_comms_kb(),
        )
        await callback.answer()
        return

    demo_owner_id = resolve_demo_owner_id_for_user(callback.from_user.id)
    user_ids = get_workspace_user_ids(demo_owner_id=demo_owner_id)
    sent = 0
    failed = 0
    if bot_instance is not None:
        for user_id in user_ids:
            try:
                await bot_instance.copy_message(
                    chat_id=user_id,
                    from_chat_id=int(source_chat_id),
                    message_id=int(source_message_id),
                )
                sent += 1
            except Exception:
                failed += 1

    await state.clear()
    await clear_aux_messages(callback.from_user.id)
    if preview_message_id:
        try:
            await bot_instance.delete_message(
                chat_id=callback.from_user.id,
                message_id=int(preview_message_id),
            )
        except Exception:
            pass
    await update_static_screen_from_callback(
        callback,
        f"Рассылка завершена.\nУспешно: {sent}\nНе доставлено: {failed}",
        reply_markup=admin_comms_kb(),
    )
    await callback.answer("Готово")


@router.callback_query(F.data == "admin:broadcast_cancel")
async def admin_broadcast_cancel_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    data = await state.get_data()
    preview_message_id = data.get("broadcast_preview_message_id")
    await state.clear()
    await clear_aux_messages(callback.from_user.id)
    if preview_message_id:
        try:
            await bot_instance.delete_message(
                chat_id=callback.from_user.id,
                message_id=int(preview_message_id),
            )
        except Exception:
            pass
    await update_static_screen_from_callback(
        callback,
        "Рассылка отменена.",
        reply_markup=admin_comms_kb(),
    )
    await callback.answer("Отменено")


@router.message(AdminFSM.waiting_broadcast_confirm)
async def admin_broadcast_confirm_waiting_handler(message: Message) -> None:
    await try_delete_user_message(message)
    await send_static_screen(
        message.from_user.id,
        "Подтвердите рассылку кнопкой «Запустить рассылку» или отмените действие.",
        reply_markup=admin_broadcast_confirm_kb(),
    )


@router.callback_query(F.data == "admin:cancel")
async def admin_cancel_appointment_start(callback: CallbackQuery, state: FSMContext) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    await state.clear()
    await update_static_screen_from_callback(
        callback,
        "Выберите запись для удаления. Ввод ID с клавиатуры не требуется.",
        reply_markup=admin_cancel_select_kb(
            demo_owner_id=resolve_demo_owner_id_for_user(callback.from_user.id)
        ),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("adm_cancel_id:"))
async def admin_cancel_appointment_pick(callback: CallbackQuery) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    _, appointment_id_raw = callback.data.split(":")
    if not appointment_id_raw.isdigit():
        await callback.answer("Некорректный ID", show_alert=True)
        return
    appointment_id = int(appointment_id_raw)
    await update_static_screen_from_callback(
        callback,
        "Подтвердите удаление записи:\n\n"
        f"ID: {appointment_id}\n"
        "После подтверждения запись и напоминания будут удалены.",
        reply_markup=admin_cancel_confirm_kb(appointment_id),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("adm_cancel_confirm:"))
async def admin_cancel_appointment_confirm(callback: CallbackQuery) -> None:
    if not is_workspace_admin(callback.from_user.id):
        await callback.answer("Раздел недоступен", show_alert=True)
        return
    _, appointment_id_raw = callback.data.split(":")
    if not appointment_id_raw.isdigit():
        await callback.answer("Некорректный ID", show_alert=True)
        return
    appointment_id = int(appointment_id_raw)
    deleted = delete_appointment_by_id(
        appointment_id,
        demo_owner_id=resolve_demo_owner_id_for_user(callback.from_user.id),
    )
    if not deleted:
        await callback.answer("Запись уже удалена или не найдена", show_alert=True)
        await update_static_screen_from_callback(
            callback,
            "Обновленный список записей:",
            reply_markup=admin_cancel_select_kb(
                demo_owner_id=resolve_demo_owner_id_for_user(callback.from_user.id)
            ),
        )
        return
    remove_appointment_reminder_jobs(appointment_id)
    await update_static_screen_from_callback(
        callback,
        f"Запись ID {appointment_id} удалена.\n\nВыберите следующую запись или вернитесь назад.",
        reply_markup=admin_cancel_select_kb(
            demo_owner_id=resolve_demo_owner_id_for_user(callback.from_user.id)
        ),
    )
    await callback.answer("Удалено")


# =====================================================
# 10) Запуск приложения
# =====================================================
async def main() -> None:
    global bot_instance

    init_db()
    ensure_demo_workspace(OWNER_ID_INT)
    bot_instance = patch_bot_text_output(
        Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
    )
    dp.update.middleware(DemoOwnerContextMiddleware())

    scheduler.start()
    restore_reminders_from_db()

    try:
        await dp.start_polling(bot_instance)
    finally:
        scheduler.shutdown(wait=False)
        await bot_instance.session.close()


if __name__ == "__main__":
    asyncio.run(main())








