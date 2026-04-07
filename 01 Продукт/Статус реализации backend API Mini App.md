---
title: Статус реализации backend API Mini App
type: implementation-status
status: active
tags:
  - obsidian
  - продукт
  - mini-app
  - backend
  - api
aliases:
  - Backend API Mini App Status
  - Статус backend Mini App
---

# Статус реализации backend API Mini App

## Что уже реализовано
- поднят `aiohttp` API-слой внутри текущего проекта;
- API запускается рядом с текущим polling-ботом в одном asyncio-процессе;
- добавлен конфиг:
  - `MINI_APP_API_HOST`
  - `MINI_APP_API_PORT`
  - `MINI_APP_DRAFT_TTL_MINUTES`
- добавлена таблица `mini_app_booking_drafts` для черновиков записи;
- добавлен единый error format для Mini App API;
- добавлен базовый CORS middleware;
- добавлены endpoints first backend sprint:
  - `GET /mini-app/health`
  - `GET /mini-app/home`
  - `GET /mini-app/masters`
  - `GET /mini-app/services`
  - `GET /mini-app/availability/dates`
  - `GET /mini-app/availability/slots`
  - `POST /mini-app/bookings/draft`
  - `POST /mini-app/bookings/confirm`
- UI-вход в `Mini App` больше не ограничен только `owner`;
- launch URL Mini App теперь собирается с `ownerId`, чтобы приложение открывалось в правильном workspace-контексте.

## Что уже переиспользует текущую логику бота
- мастеров и их режим `solo/team`;
- услуги;
- доступные слоты и занятость;
- создание записи через общую booking-логику;
- уведомление админов о новой записи;
- напоминания по записи.

## Что важно
Mini App backend не создаёт вторую параллельную систему.
Он использует те же сущности и ту же booking-логику, что уже работают в боте.

## Что изменили в доступе
- раньше бот показывал экран `Mini App уже скоро` всем, кроме `owner`;
- теперь bot-side вход в Mini App открыт для пользователей, которым показывается этот раздел;
- при открытии кнопка прокидывает `ownerId` в URL Mini App, чтобы фронт видел нужный рабочий контур, а не падал в неверный context.

## Что пока сознательно не сделано
- витринные endpoints `price` и `portfolio`;
- отдельный endpoint карточки мастера;
- Telegram init-data validation;
- upload flow для референсов;
- клиентский кабинет;
- отмена / перенос записи;
- отдельный production-ready auth layer.
- production frontend Mini App вместо placeholder-страницы.

## Текущее состояние launch URL
- `MINI_APP_URL` сейчас указывает на `https://nails-miniapp.m-samasiuk.workers.dev`;
- placeholder-страница заменена на локальный frontend-каркас Mini App;
- новый каркас лежит в:
  - [mini_app/index.html](c:\Users\Mlrt2\OneDrive\Рабочий стол\VIBE CODING\mini_app\index.html)
  - [mini_app/styles.css](c:\Users\Mlrt2\OneDrive\Рабочий стол\VIBE CODING\mini_app\styles.css)
  - [mini_app/app.js](c:\Users\Mlrt2\OneDrive\Рабочий стол\VIBE CODING\mini_app\app.js)
- если frontend открыт на отдельном домене `workers.dev`, для live API нужен отдельный public base URL;
- для этого в bot-side launch добавлена поддержка `MINI_APP_PUBLIC_API_BASE_URL`, которая прокидывает `apiBase` в query string Mini App.

## Что логично делать следующим шагом
1. начать реализацию frontend-каркаса Mini App;
2. подключить главный экран к `GET /mini-app/home`;
3. подключить happy path записи к `draft` и `confirm`;
4. добавить витринные endpoints второй волной.

## Техническая проверка
- `python -m py_compile main.py` — прошло;
- `import aiohttp` — доступен;
- `build_mini_app_api_app()` — собирается без ошибок.

## Итог
Backend Mini App уже перешёл из стадии документации в стадию реальной реализации.
У нас есть первый рабочий API-контур, на который теперь можно сажать frontend first sprint.
