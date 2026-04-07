const tg = window.Telegram?.WebApp ?? null;
const query = new URLSearchParams(window.location.search);
const now = new Date();

const state = {
  screen: "loading",
  sourceMode: "loading",
  ownerId: Number(query.get("ownerId")) || null,
  apiBase: resolveApiBase(),
  history: [],
  home: null,
  masters: [],
  services: [],
  dateItems: [],
  timeItems: [],
  selectedMasterId: null,
  selectedServiceId: null,
  selectedDate: null,
  selectedTime: null,
  phone: "",
  name: tg?.initDataUnsafe?.user?.first_name ?? "",
  comment: "",
  draftId: null,
  summary: null,
  error: "",
};

const mockData = createMockData();

boot();

async function boot() {
  configureTelegramShell();
  render();

  try {
    const healthOk = await pingApi();
    if (healthOk) {
      state.sourceMode = "live";
      await hydrateLiveData();
    } else {
      state.sourceMode = "mock";
      hydrateMockData();
    }
  } catch (error) {
    console.warn("[mini-app] Falling back to preview mode", error);
    state.sourceMode = "mock";
    hydrateMockData();
  }

  state.screen = "home";
  updateStatusPill();
  render();
}

function configureTelegramShell() {
  if (!tg) return;
  tg.ready();
  tg.expand();
  tg.setHeaderColor("#f7f0e8");
  tg.setBackgroundColor("#f7f0e8");
}

function resolveApiBase() {
  const apiBaseFromQuery = query.get("apiBase");
  if (apiBaseFromQuery) {
    localStorage.setItem("miniAppApiBase", apiBaseFromQuery);
    return apiBaseFromQuery.replace(/\/$/, "");
  }
  const savedApiBase = localStorage.getItem("miniAppApiBase");
  if (savedApiBase) {
    return savedApiBase.replace(/\/$/, "");
  }
  return window.location.origin.replace(/\/$/, "");
}

async function pingApi() {
  const url = new URL("/mini-app/health", state.apiBase);
  const response = await fetch(url.toString(), { method: "GET" });
  if (!response.ok) return false;
  const payload = await response.json();
  return payload?.status === "ok";
}

async function hydrateLiveData() {
  const home = await apiGet("/mini-app/home");
  const mastersPayload = await apiGet("/mini-app/masters");
  state.home = {
    mode: home.mode,
    headline: decodeText(home.headline),
    subline: decodeText(home.subline),
    primaryCta: {
      label: decodeText(home.primaryCta?.label || "Записаться"),
      action: home.primaryCta?.action || "start_booking",
    },
    secondaryCtas: (home.secondaryCtas || []).map((item) => ({
      label: decodeText(item.label),
      action: item.action,
    })),
    trustText: decodeText(home.trustText),
  };
  state.masters = (mastersPayload.items || []).map(normalizeMaster);
}

function hydrateMockData() {
  state.home = mockData.home;
  state.masters = mockData.masters;
}

async function apiGet(path, params = {}) {
  const url = new URL(path, state.apiBase);
  const payload = { ...params };
  if (state.ownerId) payload.ownerId = String(state.ownerId);
  for (const [key, value] of Object.entries(payload)) {
    if (value !== undefined && value !== null && value !== "") {
      url.searchParams.set(key, String(value));
    }
  }
  const response = await fetch(url.toString(), {
    method: "GET",
    headers: { Accept: "application/json" },
  });
  return parseApiResponse(response);
}

async function apiPost(path, body = {}) {
  const response = await fetch(new URL(path, state.apiBase).toString(), {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      ownerId: state.ownerId,
      ...body,
    }),
  });
  return parseApiResponse(response);
}

async function parseApiResponse(response) {
  const payload = await response.json().catch(() => ({}));
  if (response.ok) return payload;
  const errorMessage = decodeText(payload?.error?.message || "Не удалось загрузить экран. Попробуйте ещё раз.");
  throw new Error(errorMessage);
}

function normalizeMaster(item) {
  return {
    id: item.id,
    name: decodeText(item.name),
    specialization: decodeText(item.specialization || ""),
    photoUrl: item.photoUrl || "",
    shortBio: decodeText(item.shortBio || ""),
    isPrimary: Boolean(item.isPrimary),
  };
}

function normalizeService(item) {
  return {
    id: item.id,
    name: decodeText(item.name),
    price: item.price,
    currency: decodeText(item.currency || "USD"),
    description: decodeText(item.description || ""),
    masterIds: item.masterIds || [],
  };
}

function createMockData() {
  const month = formatYearMonth(now);
  const dateItems = Array.from({ length: 10 }, (_, index) => {
    const date = new Date(now.getFullYear(), now.getMonth(), now.getDate() + index);
    return {
      date: toIsoDate(date),
      isAvailable: index !== 2 && index !== 6,
      slotsCount: index !== 2 && index !== 6 ? (index % 3) + 2 : 0,
    };
  });

  return {
    home: {
      mode: "solo",
      headline: "Удобная запись к beauty-мастеру",
      subline: "Выберите услугу, время и запишитесь без лишней переписки.",
      primaryCta: { label: "Записаться", action: "start_booking" },
      secondaryCtas: [
        { label: "Прайс", action: "open_price" },
        { label: "Портфолио", action: "open_portfolio" },
      ],
      trustText: "Клиенту проще записаться. Мастеру проще держать запись в порядке.",
    },
    masters: [
      {
        id: 1,
        name: "Анна",
        specialization: "Маникюр и укрепление",
        shortBio: "Аккуратный clean look, комфортный сервис и внимание к деталям.",
        isPrimary: true,
      },
      {
        id: 2,
        name: "София",
        specialization: "Наращивание и сложный дизайн",
        shortBio: "Помогает собрать образ под событие, сезон и желаемую длину.",
        isPrimary: false,
      },
    ],
    services: {
      1: [
        { id: 101, name: "Маникюр с покрытием", price: 65, currency: "USD", description: "Классический ухоженный результат на каждый день.", masterIds: [1] },
        { id: 102, name: "Укрепление + покрытие", price: 80, currency: "USD", description: "Когда хочется дольше носить покрытие спокойно.", masterIds: [1] },
      ],
      2: [
        { id: 201, name: "Наращивание medium", price: 95, currency: "USD", description: "Форма, длина и аккуратный силуэт под твой запрос.", masterIds: [2] },
        { id: 202, name: "Дизайн set", price: 120, currency: "USD", description: "Если нужен более выразительный и собранный образ.", masterIds: [2] },
      ],
    },
    availability: {
      month,
      dateItems,
      slotsByDate: Object.fromEntries(
        dateItems.map((item, index) => [
          item.date,
          item.isAvailable ? ["10:00", "12:30", "15:00", "18:00"].slice(0, (index % 3) + 2) : [],
        ]),
      ),
    },
    portfolio: [
      { title: "Clean nails", text: "Спокойная база, глянец и ухоженная форма." },
      { title: "Soft chrome", text: "Лёгкий блеск и более премиальное впечатление." },
      { title: "Event set", text: "Дизайн под событие и собранный образ." },
      { title: "Natural nude", text: "Минимализм, который приятно носить каждый день." },
    ],
  };
}

function updateStatusPill() {
  const pill = document.getElementById("status-pill");
  if (!pill) return;
  pill.className = "status-pill";
  if (state.sourceMode === "live") {
    pill.classList.add("status-pill--live");
    pill.textContent = "Live API";
    return;
  }
  if (state.sourceMode === "mock") {
    pill.classList.add("status-pill--mock");
    pill.textContent = "Preview mode";
    return;
  }
  if (state.sourceMode === "error") {
    pill.classList.add("status-pill--error");
    pill.textContent = "Нужна проверка";
    return;
  }
  pill.textContent = "Загрузка...";
}

function pushHistory() {
  state.history.push({
    screen: state.screen,
    selectedMasterId: state.selectedMasterId,
    selectedServiceId: state.selectedServiceId,
    selectedDate: state.selectedDate,
    selectedTime: state.selectedTime,
    phone: state.phone,
    name: state.name,
    comment: state.comment,
    draftId: state.draftId,
    summary: state.summary,
  });
}

function goTo(screen) {
  pushHistory();
  state.screen = screen;
  state.error = "";
  render();
}

function goBack() {
  const previous = state.history.pop();
  if (!previous) {
    state.screen = "home";
    render();
    return;
  }
  Object.assign(state, previous);
  render();
}

async function startBookingFlow() {
  state.draftId = null;
  state.summary = null;
  state.selectedServiceId = null;
  state.selectedDate = null;
  state.selectedTime = null;
  if (shouldChooseMaster()) {
    goTo("masters");
    return;
  }
  state.selectedMasterId = getDefaultMasterId();
  await openServices();
}

function shouldChooseMaster() {
  return state.home?.mode === "team" && state.masters.length > 1;
}

function getDefaultMasterId() {
  return state.masters.find((item) => item.isPrimary)?.id || state.masters[0]?.id || null;
}
async function openServices(masterId = state.selectedMasterId) {
  state.selectedMasterId = masterId;
  state.selectedServiceId = null;
  state.selectedDate = null;
  state.selectedTime = null;

  if (state.sourceMode === "live") {
    const payload = await apiGet("/mini-app/services", { masterId });
    state.services = (payload.items || []).map(normalizeService);
  } else {
    state.services = mockData.services[masterId] || Object.values(mockData.services)[0];
  }

  goTo("services");
}

async function ensureAllServicesLoaded() {
  if (state.services.length) return;
  if (state.sourceMode === "live") {
    const payload = await apiGet("/mini-app/services");
    state.services = (payload.items || []).map(normalizeService);
    return;
  }
  state.services = Object.values(mockData.services).flat();
}

async function openDates(serviceId) {
  state.selectedServiceId = serviceId;
  state.selectedDate = null;
  state.selectedTime = null;
  const month = formatYearMonth(now);

  if (state.sourceMode === "live") {
    const payload = await apiGet("/mini-app/availability/dates", {
      masterId: state.selectedMasterId,
      serviceId,
      month,
    });
    state.dateItems = payload.items || [];
  } else {
    state.dateItems = mockData.availability.dateItems;
  }

  goTo("dates");
}

async function openTimes(date) {
  state.selectedDate = date;
  state.selectedTime = null;

  if (state.sourceMode === "live") {
    const payload = await apiGet("/mini-app/availability/slots", {
      masterId: state.selectedMasterId,
      serviceId: state.selectedServiceId,
      date,
    });
    state.timeItems = (payload.items || []).map((item) => ({
      time: item.time,
      isAvailable: Boolean(item.isAvailable),
    }));
  } else {
    state.timeItems = (mockData.availability.slotsByDate[date] || []).map((time) => ({
      time,
      isAvailable: true,
    }));
  }

  goTo("times");
}

function openContact(time) {
  state.selectedTime = time;
  goTo("contact");
}

async function buildDraft() {
  const master = state.masters.find((item) => item.id === state.selectedMasterId);
  const service = state.services.find((item) => item.id === state.selectedServiceId);
  if (!master || !service) throw new Error("Не удалось собрать запись. Попробуйте заново.");

  if (state.sourceMode === "live") {
    const payload = await apiPost("/mini-app/bookings/draft", {
      masterId: state.selectedMasterId,
      serviceId: state.selectedServiceId,
      date: state.selectedDate,
      time: state.selectedTime,
      clientPhone: state.phone,
      clientName: state.name,
      comment: state.comment,
      telegramUserId: tg?.initDataUnsafe?.user?.id ?? null,
      source: "mini_app",
      managerId: query.get("managerId") ? Number(query.get("managerId")) : null,
    });
    state.draftId = payload.draftId;
    state.summary = normalizeSummary(payload.summary);
    return;
  }

  state.draftId = "preview-draft";
  state.summary = {
    master: { id: master.id, name: master.name },
    service: { id: service.id, name: service.name, price: service.price, currency: service.currency },
    date: state.selectedDate,
    time: state.selectedTime,
    clientName: state.name,
    clientPhone: state.phone,
    comment: state.comment,
  };
}

async function confirmBooking() {
  try {
    let payload;
    if (state.sourceMode === "live") {
      payload = await apiPost("/mini-app/bookings/confirm", {
        draftId: state.draftId,
        telegramUserId: tg?.initDataUnsafe?.user?.id ?? null,
      });
    } else {
      payload = {
        bookingId: `preview-${Date.now()}`,
        success: {
          title: "Запись подтверждена",
          text: "Это preview-режим Mini App. Когда подключим live API base, запись будет уходить в бот по-настоящему.",
        },
      };
    }

    state.summary = {
      ...state.summary,
      bookingId: payload.bookingId,
      successTitle: decodeText(payload.success?.title || "Запись подтверждена"),
      successText: decodeText(payload.success?.text || "Спасибо. Если что-то изменится, мы заранее предупредим вас."),
    };
    goTo("success");
  } catch (error) {
    state.error = error.message;
    render();
  }
}

function normalizeSummary(summary) {
  return {
    master: {
      id: summary.master.id,
      name: decodeText(summary.master.name),
    },
    service: {
      id: summary.service.id,
      name: decodeText(summary.service.name),
      price: summary.service.price,
      currency: decodeText(summary.service.currency || "USD"),
    },
    date: summary.date,
    time: summary.time,
    clientName: decodeText(summary.clientName),
    clientPhone: decodeText(summary.clientPhone),
    comment: summary.comment ? decodeText(summary.comment) : "",
  };
}

function validateContactForm() {
  const phone = state.phone.trim();
  const name = state.name.trim();

  if (phone.replace(/[^\d+]/g, "").length < 7) {
    throw new Error("Номер выглядит неполным. Проверьте и попробуйте ещё раз.");
  }
  if (name.length < 2) {
    throw new Error("Имя выглядит слишком коротким. Введите минимум 2 символа.");
  }
}

function attachEventHandlers() {
  document.querySelectorAll("[data-action]").forEach((node) => {
    node.addEventListener("click", handleAction);
  });

  const phoneInput = document.querySelector("[data-field='phone']");
  if (phoneInput) {
    phoneInput.addEventListener("input", (event) => {
      state.phone = event.target.value;
    });
  }

  const nameInput = document.querySelector("[data-field='name']");
  if (nameInput) {
    nameInput.addEventListener("input", (event) => {
      state.name = event.target.value;
    });
  }

  const commentInput = document.querySelector("[data-field='comment']");
  if (commentInput) {
    commentInput.addEventListener("input", (event) => {
      state.comment = event.target.value;
    });
  }

  const contactForm = document.querySelector("[data-form='contact']");
  if (contactForm) {
    contactForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      try {
        validateContactForm();
        await buildDraft();
        goTo("confirm");
      } catch (error) {
        state.error = error.message;
        render();
      }
    });
  }
}

async function handleAction(event) {
  const action = event.currentTarget.dataset.action;

  try {
    if (action === "start_booking") {
      await startBookingFlow();
      return;
    }
    if (action === "open_price") {
      await ensureAllServicesLoaded();
      goTo("price");
      return;
    }
    if (action === "open_portfolio") {
      goTo("portfolio");
      return;
    }
    if (action === "open_masters") {
      goTo("masters");
      return;
    }
    if (action === "back") {
      goBack();
      return;
    }
    if (action === "home") {
      state.history = [];
      state.screen = "home";
      render();
      return;
    }
    if (action === "choose_master") {
      await openServices(Number(event.currentTarget.dataset.id));
      return;
    }
    if (action === "choose_service") {
      await openDates(Number(event.currentTarget.dataset.id));
      return;
    }
    if (action === "choose_date") {
      await openTimes(event.currentTarget.dataset.id);
      return;
    }
    if (action === "choose_time") {
      openContact(event.currentTarget.dataset.id);
      return;
    }
    if (action === "confirm_booking") {
      await confirmBooking();
      return;
    }
    if (action === "book_again") {
      state.history = [];
      await startBookingFlow();
    }
  } catch (error) {
    state.error = error.message;
    render();
  }
}

function render() {
  updateStatusPill();
  const app = document.getElementById("app");
  app.innerHTML = renderView();
  attachEventHandlers();
}

function renderView() {
  if (state.screen === "loading") {
    return `
      <div class="loading-state">
        <div class="loading-state__spark"></div>
        <h1 class="loading-state__title">Собираем Mini App</h1>
        <p class="loading-state__text">Подключаем данные, чтобы показать красивый клиентский путь записи.</p>
      </div>
    `;
  }

  const errorBlock = state.error
    ? `
      <section class="notice-card notice-card--error">
        <h2 class="notice-card__title">Нужно ещё одно действие</h2>
        <p class="helper-text">${escapeHtml(state.error)}</p>
      </section>
    `
    : "";

  const modeNotice = state.sourceMode === "mock"
    ? `
      <section class="notice-card">
        <h2 class="notice-card__title">Preview-режим</h2>
        <p class="helper-text">
          Каркас Mini App уже живой, но публичный API base пока не подключён к этому домену.
          Поэтому экран работает как демонстрационный flow, а не как production booking.
        </p>
      </section>
    `
    : "";

  return `
    <div class="view">
      ${errorBlock}
      ${modeNotice}
      ${renderScreenContent()}
    </div>
  `;
}

function renderScreenContent() {
  switch (state.screen) {
    case "home":
      return renderHome();
    case "masters":
      return renderMasters();
    case "services":
      return renderServices();
    case "dates":
      return renderDates();
    case "times":
      return renderTimes();
    case "contact":
      return renderContact();
    case "confirm":
      return renderConfirm();
    case "success":
      return renderSuccess();
    case "price":
      return renderPrice();
    case "portfolio":
      return renderPortfolio();
    default:
      return renderHome();
  }
}
function renderHome() {
  const secondary = (state.home.secondaryCtas || [])
    .map((item) => `<button class="chip-button" data-action="${item.action}">${escapeHtml(item.label)}</button>`)
    .join("");

  return `
    <section class="hero-card">
      <div class="hero-card__visual">
        <span class="hero-card__eyebrow">Клиентский путь внутри Telegram</span>
        <h1 class="hero-card__title">${escapeHtml(state.home.headline)}</h1>
        <p class="hero-card__text">${escapeHtml(state.home.subline)}</p>
      </div>
      <div class="hero-card__body">
        <div class="cta-row">
          <button class="button button--wide" data-action="${state.home.primaryCta.action}">
            ${escapeHtml(state.home.primaryCta.label)}
          </button>
        </div>
        <div class="chip-row">${secondary}</div>
        <span class="trust-pill">${escapeHtml(state.home.trustText)}</span>
      </div>
    </section>

    <section class="fact-strip">
      <article class="fact-card">
        <span class="fact-card__label">Формат</span>
        <strong class="fact-card__value">${state.home.mode === "team" ? "Team" : "Solo"}</strong>
      </article>
      <article class="fact-card">
        <span class="fact-card__label">Шагов до записи</span>
        <strong class="fact-card__value">4-6</strong>
      </article>
    </section>

    <section class="section-card">
      <div class="section-card__header">
        <h2 class="screen-title">Что уже есть внутри</h2>
        <p class="screen-subtitle">Сервис выглядит как аккуратный beauty-flow, а не как длинная переписка в боте.</p>
      </div>
      <div class="value-grid">
        <article class="value-card">
          <h3 class="value-card__title">Понятный выбор услуги</h3>
          <p class="value-card__text">Карточки с ценой и коротким объяснением, без ощущения перегруженного прайса.</p>
        </article>
        <article class="value-card">
          <h3 class="value-card__title">Быстрый выбор времени</h3>
          <p class="value-card__text">Даты и слоты показываются отдельными шагами, без хаоса в переписке.</p>
        </article>
        <article class="value-card">
          <h3 class="value-card__title">Аккуратное подтверждение</h3>
          <p class="value-card__text">Клиент видит итог записи до подтверждения, а мастер получает более чистый процесс.</p>
        </article>
      </div>
    </section>
  `;
}

function renderMasters() {
  return `
    <section class="section-card">
      <div class="section-card__header">
        <h1 class="screen-title">Выберите мастера</h1>
        <p class="screen-subtitle">Посмотрите профиль и выберите, к кому хотите записаться.</p>
      </div>
      <div class="grid-list">
        ${state.masters
          .map(
            (master) => `
              <article class="master-card">
                <div class="master-card__top">
                  <div>
                    <h2 class="master-card__name">${escapeHtml(master.name)}</h2>
                    <p class="master-card__bio">${escapeHtml(master.specialization || "Beauty-мастер")}</p>
                  </div>
                  ${master.isPrimary ? `<span class="master-card__badge">Основной</span>` : ""}
                </div>
                <p class="master-card__bio">${escapeHtml(master.shortBio || "Аккуратный сервис и спокойный клиентский путь.")}</p>
                <div class="action-row">
                  <button class="button" data-action="choose_master" data-id="${master.id}">Выбрать</button>
                  <button class="ghost-button" data-action="open_portfolio">Портфолио</button>
                </div>
              </article>
            `,
          )
          .join("")}
      </div>
      <div class="action-row">
        <button class="ghost-button" data-action="back">Назад</button>
      </div>
    </section>
  `;
}

function renderServices() {
  const master = state.masters.find((item) => item.id === state.selectedMasterId);
  return `
    <section class="section-card">
      <div class="section-card__header">
        <h1 class="screen-title">Выберите услугу</h1>
        <p class="screen-subtitle">Посмотрите доступные услуги и выберите нужную.${master ? ` Запись пойдёт к мастеру ${escapeHtml(master.name)}.` : ""}</p>
      </div>
      <div class="grid-list">
        ${state.services
          .map(
            (service) => `
              <article class="service-card">
                <div class="service-card__top">
                  <div>
                    <h2 class="service-card__name">${escapeHtml(service.name)}</h2>
                    <p class="service-card__meta">${escapeHtml(service.description || "Услуга доступна для быстрой записи через Mini App.")}</p>
                  </div>
                  <span class="service-card__badge">${service.price} ${escapeHtml(service.currency)}</span>
                </div>
                <div class="action-row">
                  <button class="button" data-action="choose_service" data-id="${service.id}">Выбрать</button>
                </div>
              </article>
            `,
          )
          .join("")}
      </div>
      <div class="action-row">
        <button class="ghost-button" data-action="back">Назад</button>
      </div>
    </section>
  `;
}

function renderDates() {
  return `
    <section class="section-card">
      <div class="section-card__header">
        <h1 class="screen-title">Выберите дату</h1>
        <p class="screen-subtitle">Покажем доступные дни для записи.</p>
      </div>
      ${
        !state.dateItems.some((item) => item.isAvailable)
          ? `<div class="empty-card"><h2 class="empty-card__title">Свободных дат пока нет</h2><p class="empty-card__text">Попробуйте выбрать другой период чуть позже.</p></div>`
          : `
            <div class="calendar-grid">
              ${state.dateItems
                .slice(0, 12)
                .map(
                  (item) => `
                    <button
                      class="date-button ${item.isAvailable ? "" : "date-button--disabled"} ${state.selectedDate === item.date ? "date-button--selected" : ""}"
                      data-action="${item.isAvailable ? "choose_date" : ""}"
                      data-id="${item.date}"
                      ${item.isAvailable ? "" : "disabled"}
                    >
                      <span class="date-button__day">${formatDay(item.date)}</span>
                      <span class="date-button__meta">${formatWeekday(item.date)} · ${item.slotsCount || 0} слота</span>
                    </button>
                  `,
                )
                .join("")}
            </div>
          `
      }
      <div class="action-row">
        <button class="ghost-button" data-action="back">Назад</button>
      </div>
    </section>
  `;
}

function renderTimes() {
  return `
    <section class="section-card">
      <div class="section-card__header">
        <h1 class="screen-title">Выберите время</h1>
        <p class="screen-subtitle">Покажем свободные слоты на ${escapeHtml(formatLongDate(state.selectedDate))}.</p>
      </div>
      ${
        !state.timeItems.length
          ? `<div class="empty-card"><h2 class="empty-card__title">На эту дату свободных слотов нет</h2><p class="empty-card__text">Выберите другой день.</p></div>`
          : `
            <div class="time-grid">
              ${state.timeItems
                .map(
                  (item) => `
                    <button
                      class="slot-button ${item.isAvailable ? "" : "slot-button--disabled"} ${state.selectedTime === item.time ? "slot-button--selected" : ""}"
                      data-action="${item.isAvailable ? "choose_time" : ""}"
                      data-id="${item.time}"
                      ${item.isAvailable ? "" : "disabled"}
                    >
                      ${escapeHtml(item.time)}
                    </button>
                  `,
                )
                .join("")}
            </div>
          `
      }
      <div class="action-row">
        <button class="ghost-button" data-action="back">Назад</button>
      </div>
    </section>
  `;
}

function renderContact() {
  return `
    <section class="section-card">
      <div class="section-card__header">
        <h1 class="screen-title">Оставьте контакт</h1>
        <p class="screen-subtitle">Он нужен, чтобы подтвердить запись и при необходимости связаться с вами по визиту.</p>
      </div>
      <form class="field-grid" data-form="contact">
        <label class="field">
          <span class="field__label">Телефон</span>
          <input class="field__input" name="phone" data-field="phone" type="tel" placeholder="+375..." value="${escapeAttribute(state.phone)}" />
        </label>
        <label class="field">
          <span class="field__label">Имя</span>
          <input class="field__input" name="name" data-field="name" type="text" placeholder="Как вас подписать?" value="${escapeAttribute(state.name)}" />
        </label>
        <label class="field">
          <span class="field__label">Комментарий</span>
          <textarea class="field__textarea" name="comment" data-field="comment" placeholder="Если хотите, можно добавить короткий комментарий для мастера.">${escapeHtml(state.comment)}</textarea>
        </label>
        <div class="action-row">
          <button class="button" type="submit">Проверить запись</button>
          <button class="ghost-button" type="button" data-action="back">Назад</button>
        </div>
      </form>
    </section>
  `;
}

function renderConfirm() {
  const summary = state.summary;
  return `
    <section class="summary-card">
      <div class="section-card__header">
        <h1 class="screen-title">Проверьте запись</h1>
        <p class="screen-subtitle">Если всё верно, подтвердите запись. Если нужно, данные можно быстро изменить.</p>
      </div>
      <div class="summary-list">
        ${renderSummaryRow("Мастер", summary.master.name)}
        ${renderSummaryRow("Услуга", `${summary.service.name} · ${summary.service.price} ${summary.service.currency}`)}
        ${renderSummaryRow("Дата", formatLongDate(summary.date))}
        ${renderSummaryRow("Время", summary.time)}
        ${renderSummaryRow("Телефон", summary.clientPhone)}
        ${renderSummaryRow("Имя", summary.clientName)}
        ${summary.comment ? renderSummaryRow("Комментарий", summary.comment) : ""}
      </div>
      <div class="action-row">
        <button class="button" data-action="confirm_booking">Подтвердить запись</button>
        <button class="ghost-button" data-action="back">Назад</button>
      </div>
    </section>
  `;
}
function renderSuccess() {
  const summary = state.summary;
  return `
    <section class="success-card">
      <div class="success-card__spark"></div>
      <h1 class="success-card__title">${escapeHtml(summary.successTitle || "Запись подтверждена")}</h1>
      <p class="success-card__text">${escapeHtml(summary.successText || "Спасибо. Если что-то изменится, мы заранее предупредим вас.")}</p>
      <div class="summary-card">
        <div class="summary-list">
          ${renderSummaryRow("Мастер", summary.master.name)}
          ${renderSummaryRow("Услуга", `${summary.service.name} · ${summary.service.price} ${summary.service.currency}`)}
          ${renderSummaryRow("Дата", formatLongDate(summary.date))}
          ${renderSummaryRow("Время", summary.time)}
        </div>
      </div>
      <div class="action-row">
        <button class="button" data-action="home">На главный экран</button>
        <button class="ghost-button" data-action="book_again">Записаться ещё раз</button>
      </div>
    </section>
  `;
}

function renderPrice() {
  const services = state.sourceMode === "live" ? state.services : Object.values(mockData.services).flat();
  return `
    <section class="section-card">
      <div class="section-card__header">
        <h1 class="screen-title">Прайс</h1>
        <p class="screen-subtitle">Аккуратная витрина услуг и цены без перегрузки.</p>
      </div>
      <div class="grid-list">
        ${services
          .map(
            (service) => `
              <article class="service-card">
                <div class="service-card__top">
                  <h2 class="service-card__name">${escapeHtml(service.name)}</h2>
                  <span class="service-card__badge">${service.price} ${escapeHtml(service.currency)}</span>
                </div>
                <p class="service-card__meta">${escapeHtml(service.description || "Услуга доступна для записи через Mini App.")}</p>
              </article>
            `,
          )
          .join("")}
      </div>
      <div class="action-row">
        <button class="button" data-action="start_booking">Записаться</button>
        <button class="ghost-button" data-action="back">Назад</button>
      </div>
    </section>
  `;
}

function renderPortfolio() {
  const items = mockData.portfolio;
  return `
    <section class="section-card">
      <div class="section-card__header">
        <h1 class="screen-title">Портфолио</h1>
        <p class="screen-subtitle">Визуальный слой доверия, который поддерживает ощущение quality-service.</p>
      </div>
      <div class="portfolio-grid">
        ${items
          .map(
            (item) => `
              <article class="portfolio-card">
                <div class="portfolio-card__visual"></div>
                <h2 class="portfolio-card__title">${escapeHtml(item.title)}</h2>
                <p class="portfolio-card__text">${escapeHtml(item.text)}</p>
              </article>
            `,
          )
          .join("")}
      </div>
      <div class="action-row">
        <button class="button" data-action="start_booking">Записаться</button>
        <button class="ghost-button" data-action="back">Назад</button>
      </div>
    </section>
  `;
}

function renderSummaryRow(label, value) {
  return `
    <div class="summary-row">
      <strong class="summary-label">${escapeHtml(label)}</strong>
      <span class="summary-value">${escapeHtml(value)}</span>
    </div>
  `;
}

function formatYearMonth(date) {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}`;
}

function formatDay(dateString) {
  return new Intl.DateTimeFormat("ru-RU", { day: "numeric", month: "long" }).format(new Date(dateString));
}

function formatWeekday(dateString) {
  return new Intl.DateTimeFormat("ru-RU", { weekday: "short" }).format(new Date(dateString));
}

function formatLongDate(dateString) {
  return new Intl.DateTimeFormat("ru-RU", {
    day: "numeric",
    month: "long",
    weekday: "long",
  }).format(new Date(dateString));
}

function toIsoDate(date) {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")}`;
}

function decodeText(value) {
  if (typeof value !== "string") return value;
  if (!/[ÐÑ]/.test(value)) return value;
  try {
    return decodeURIComponent(
      Array.from(value)
        .map((char) => `%${char.charCodeAt(0).toString(16).padStart(2, "0")}`)
        .join(""),
    );
  } catch {
    return value;
  }
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function escapeAttribute(value) {
  return escapeHtml(value).replaceAll("`", "&#96;");
}
