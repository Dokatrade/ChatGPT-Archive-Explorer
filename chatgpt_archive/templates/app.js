const state = {
  models: [],
  projects: [],
  conversations: [],
  selectedConversation: null,
  sources: [],
  imports: [],
};

const NAME_GROUP_PREFIX = "name::";
const makeNameGroupValue = (name) => `${NAME_GROUP_PREFIX}${encodeURIComponent(name)}`;
const isNameGroupValue = (value) => typeof value === "string" && value.startsWith(NAME_GROUP_PREFIX);
const parseNameGroupValue = (value) => decodeURIComponent((value || "").slice(NAME_GROUP_PREFIX.length));
const safeFilename = (name) => (name || "").replace(/[\\/:*?"<>|]+/g, "_").replace(/\s+/g, "_").trim() || "export";
const normalizeProjectName = (value) => {
  const raw = ((value ?? "") + "");
  const normalized = typeof raw.normalize === "function" ? raw.normalize("NFKC") : raw;
  return normalized.trim().replace(/\s+/g, " ").toLowerCase();
};
const groupProjectsByName = (projects) => {
  const grouped = projects.reduce((acc, p, idx) => {
    const displayNameRaw = ((p.human_name ?? p.project_id ?? "") + "").trim();
    const displayName = displayNameRaw || (p.project_id ?? "Без названия");
    const normalized = normalizeProjectName(displayName) || normalizeProjectName(p.project_id || "") || `__unnamed_${idx}`;
    const current = acc.get(normalized) || { name: displayName, projects: [], conversation_count: 0 };
    current.projects.push(p);
    current.conversation_count += Number(p.conversation_count) || 0;
    if (!current.name && displayName) current.name = displayName;
    acc.set(normalized, current);
    return acc;
  }, new Map());
  return Array.from(grouped.values()).sort((a, b) => (a.name || "").localeCompare(b.name || "", "ru"));
};

const ui = {
  chats: document.getElementById("chats"),
  messages: document.getElementById("messages"),
  search: document.getElementById("search"),
  sourceFilter: document.getElementById("source-filter"),
  projectFilter: document.getElementById("project-filter"),
  commonProjectFilter: document.getElementById("common-project-filter"),
  projectActions: document.getElementById("project-actions"),
  modelFilter: document.getElementById("model-filter"),
  chatTitle: document.getElementById("chat-title"),
  chatMeta: document.getElementById("chat-meta"),
  copyJson: document.getElementById("copy-json"),
  copyMd: document.getElementById("copy-md"),
  copyTxt: document.getElementById("copy-txt"),
  copyPath: document.getElementById("copy-path"),
  moveTarget: document.getElementById("move-target"),
  moveChat: document.getElementById("move-chat"),
  moveModal: document.getElementById("move-modal"),
  moveConfirm: document.getElementById("move-confirm"),
  moveCancel: document.getElementById("move-cancel"),
  deleteChat: document.getElementById("delete-chat"),
  addArchive: document.getElementById("add-archive"),
  resetArchive: document.getElementById("reset-archive"),
  importModal: document.getElementById("import-modal"),
  importFile: document.getElementById("import-file"),
  importAccount: document.getElementById("import-account"),
  importIncremental: document.getElementById("import-incremental"),
  importConfirm: document.getElementById("import-confirm"),
  importCancel: document.getElementById("import-cancel"),
};

const formatDate = (ts) => {
  if (!ts) return "нет даты";
  const d = new Date(ts * 1000);
  return d.toLocaleString();
};

const formatDuration = (seconds) => {
  if (seconds == null || Number.isNaN(seconds) || seconds < 0) return "";
  if (seconds < 60) return `${seconds.toFixed(1)} c`;
  const mins = Math.floor(seconds / 60);
  const secs = seconds % 60;
  return `${mins} мин ${secs.toFixed(0)} c`;
};

const escapeHtml = (value) =>
  String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");

const renderInlineMarkdown = (text) => {
  let html = escapeHtml(text || "");
  html = html.replace(/`([^`]+)`/g, "<code>$1</code>");
  html = html.replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>");
  html = html.replace(/\*([^*]+)\*/g, "<em>$1</em>");
  html = html.replace(/~~([^~]+)~~/g, "<del>$1</del>");
  html = html.replace(
    /\[([^\]]+)\]\(([^)]+)\)/g,
    '<a href="$2" target="_blank" rel="noreferrer">$1</a>',
  );
  html = html.replace(
    /(^|[\s(])((https?:\/\/|www\.)[^\s<]+)/g,
    (_, prefix, url) =>
      `${prefix}<a href="${url.startsWith("http") ? url : `https://${url}`}" target="_blank" rel="noreferrer">${url}</a>`,
  );
  return html;
};

const getProjectUid = (p) => p.project_uid || p.project_id;

function markdownToHtml(md) {
  if (!md) return "";
  const lines = md.replace(/\r\n/g, "\n").split("\n");
  const blocks = [];
  let i = 0;
  const isOl = (line) => /^\s*\d+\.\s+/.test(line);
  const isUl = (line) => /^\s*[-*+]\s+/.test(line);
  const looksLikeTableDivider = (line) =>
    /^\s*\|?\s*:?-{3,}:?\s*(\|\s*:?-{3,}:?\s*)+\|?\s*$/.test(line || "");
  const parseTableRow = (row) =>
    row
      .trim()
      .replace(/^\||\|$/g, "")
      .split("|")
      .map((cell) => cell.trim());

  while (i < lines.length) {
    const line = lines[i];

    if (/^\s*```/.test(line)) {
      const lang = line.replace(/^`{3,}\s*/, "").trim();
      const codeLines = [];
      i += 1;
      while (i < lines.length && !/^\s*```/.test(lines[i])) {
        codeLines.push(lines[i]);
        i += 1;
      }
      i += 1; // skip closing ```
      blocks.push(
        `<pre class="code-block"><code class="lang-${lang}">${escapeHtml(codeLines.join("\n"))}</code></pre>`,
      );
      continue;
    }

    if (/^\s*[-*_]{3,}\s*$/.test(line)) {
      blocks.push("<hr />");
      i += 1;
      continue;
    }

    if (/^\s*>/.test(line)) {
      const quote = [];
      while (i < lines.length && /^\s*>/.test(lines[i])) {
        quote.push(lines[i].replace(/^\s*>\s?/, ""));
        i += 1;
      }
      blocks.push(`<blockquote>${markdownToHtml(quote.join("\n"))}</blockquote>`);
      continue;
    }

    if (
      lines[i + 1] &&
      line.includes("|") &&
      looksLikeTableDivider(lines[i + 1])
    ) {
      const headerCells = parseTableRow(line);
      let j = i + 2;
      const bodyRows = [];
      while (
        j < lines.length &&
        lines[j].includes("|") &&
        lines[j].trim() !== "" &&
        !/^\s*```/.test(lines[j])
      ) {
        bodyRows.push(parseTableRow(lines[j]));
        j += 1;
      }
      const thead = `<thead><tr>${headerCells
        .map((c) => `<th>${renderInlineMarkdown(c)}</th>`)
        .join("")}</tr></thead>`;
      const tbody = bodyRows
        .map(
          (row) =>
            `<tr>${row.map((c) => `<td>${renderInlineMarkdown(c)}</td>`).join("")}</tr>`,
        )
        .join("");
      blocks.push(
        `<div class="table-wrap"><table>${thead}${tbody ? `<tbody>${tbody}</tbody>` : ""}</table></div>`,
      );
      i = j;
      continue;
    }

    if (isOl(line)) {
      const items = [];
      while (i < lines.length && isOl(lines[i])) {
        const item = lines[i].replace(/^\s*\d+\.\s+/, "");
        items.push(`<li>${renderInlineMarkdown(item)}</li>`);
        i += 1;
      }
      blocks.push(`<ol>${items.join("")}</ol>`);
      continue;
    }

    if (isUl(line)) {
      const items = [];
      while (i < lines.length && isUl(lines[i])) {
        const item = lines[i].replace(/^\s*[-*+]\s+/, "");
        items.push(`<li>${renderInlineMarkdown(item)}</li>`);
        i += 1;
      }
      blocks.push(`<ul>${items.join("")}</ul>`);
      continue;
    }

    if (/^\s*#{1,6}\s+/.test(line)) {
      const level = (line.match(/^\s*(#{1,6})\s+/) || ["", "h1"])[1].length;
      const content = line.replace(/^\s*#{1,6}\s+/, "");
      blocks.push(`<h${level}>${renderInlineMarkdown(content)}</h${level}>`);
      i += 1;
      continue;
    }

    if (line.trim() === "") {
      i += 1;
      continue;
    }

    const paragraph = [];
    while (
      i < lines.length &&
      lines[i].trim() !== "" &&
      !isOl(lines[i]) &&
      !isUl(lines[i]) &&
      !/^\s*#{1,6}\s+/.test(lines[i]) &&
      !/^\s*```/.test(lines[i]) &&
      !/^\s*>/.test(lines[i]) &&
      !(lines[i + 1] && lines[i].includes("|") && looksLikeTableDivider(lines[i + 1]))
    ) {
      paragraph.push(lines[i]);
      i += 1;
    }
    const text = paragraph.join(" ").trim();
    if (text) {
      blocks.push(`<p>${renderInlineMarkdown(text)}</p>`);
    }
  }

  return blocks.join("\n");
}

function stripMarkdown(md) {
  if (!md) return "";
  let text = md;
  // Strip code fences but keep inner text
  text = text.replace(/```[\s\S]*?```/g, (m) => m.replace(/```/g, ""));
  // Strip inline code
  text = text.replace(/`([^`]+)`/g, "$1");
  // Links/images
  text = text.replace(/!\[([^\]]*)\]\(([^)]+)\)/g, "$1 ($2)");
  text = text.replace(/\[([^\]]+)\]\(([^)]+)\)/g, "$1 ($2)");
  // Emphasis/strike
  text = text.replace(/[*_]{1,3}([^*_]+)[*_]{1,3}/g, "$1");
  text = text.replace(/~~([^~]+)~~/g, "$1");
  // Lists / blockquotes / headings
  text = text.replace(/^\s*>\s?/gm, "");
  text = text.replace(/^\s*[-*+]\s+/gm, "");
  text = text.replace(/^\s*\d+\.\s+/gm, "");
  text = text.replace(/^\s*#{1,6}\s*/gm, "");
  // Horizontal rules
  text = text.replace(/^\s*[-*_]{3,}\s*$/gm, "");
  // Collapse excess blank lines and trim
  text = text.replace(/\r\n/g, "\n").replace(/\n{3,}/g, "\n\n").trim();
  return text;
}

function buildHtmlDocument(payload) {
  const { title, createdAt, updatedAt, projectId, model, messages, baseUrl } = payload;
  const assetBase = baseUrl || "";
  const messageHtml = messages
    .map((m) => {
      const attachments = (m.attachments || [])
        .filter((att) => att.path || att.local_path)
        .map((att) => {
          const src = att.path ? `${assetBase}/files/${encodeURI(att.path)}` : att.local_path;
          if (!src) return "";
          const alt = escapeHtml(att.asset_id || "image");
          return `<figure class="asset"><img src="${escapeHtml(src)}" alt="${alt}" loading="lazy"/><figcaption>${alt}</figcaption></figure>`;
        })
        .join("");
      return `
        <article class="msg ${m.role}">
          <div class="msg-head">
            <span class="pill ${m.role}">${m.role === "user" ? "Пользователь" : "Ассистент"}</span>
            ${m.timestamp ? `<span class="ts">${formatDate(m.timestamp)}</span>` : ""}
          </div>
          <div class="msg-body">${markdownToHtml(m.text || "") || '<p class="muted">Без текста</p>'}</div>
          ${attachments ? `<div class="msg-attachments">${attachments}</div>` : ""}
        </article>`;
    })
    .join("\n");

  return `<!doctype html>
<html lang="ru">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>${escapeHtml(title)}</title>
  <style>
    :root {
      --bg: #0b1224;
      --card: #0f172a;
      --muted: #9ca3af;
      --accent: #7dd3fc;
      --accent-2: #a78bfa;
      --border: #1f2937;
      --text: #e5e7eb;
      --mono: "JetBrains Mono", "Fira Code", "SFMono-Regular", Menlo, monospace;
      --sans: "Manrope", "Inter", "Segoe UI", system-ui, -apple-system, sans-serif;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background:
        radial-gradient(1200px circle at 12% 10%, rgba(125, 211, 252, 0.12), transparent 35%),
        radial-gradient(800px circle at 88% 14%, rgba(167, 139, 250, 0.1), transparent 30%),
        var(--bg);
      color: var(--text);
      font-family: var(--sans);
      min-height: 100vh;
    }
    .page { max-width: 1080px; margin: 0 auto; padding: 32px 18px 56px; }
    .hero {
      background: linear-gradient(135deg, rgba(125, 211, 252, 0.1), rgba(167, 139, 250, 0.12));
      border: 1px solid rgba(255, 255, 255, 0.08);
      box-shadow: 0 25px 60px rgba(0, 0, 0, 0.35);
      border-radius: 18px;
      padding: 20px 22px;
    }
    .hero h1 { margin: 0 0 8px 0; font-size: 28px; letter-spacing: -0.2px; }
    .hero .meta {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      color: var(--muted);
      font-size: 14px;
    }
    .pill {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      border-radius: 999px;
      padding: 6px 10px;
      border: 1px solid rgba(255, 255, 255, 0.12);
      background: rgba(255, 255, 255, 0.04);
      color: #c7d2fe;
      font-weight: 700;
      font-size: 12px;
      letter-spacing: 0.4px;
      text-transform: uppercase;
    }
    .pill.user { background: rgba(125, 211, 252, 0.12); border-color: rgba(125, 211, 252, 0.5); color: #7dd3fc; }
    .pill.assistant { background: rgba(167, 139, 250, 0.12); border-color: rgba(167, 139, 250, 0.5); color: #c4b5fd; }
    .pill.meta {
      text-transform: none;
      font-weight: 600;
      color: var(--text);
      background: rgba(255, 255, 255, 0.06);
    }
    .messages { margin-top: 18px; display: flex; flex-direction: column; gap: 12px; }
    .msg {
      background: var(--card);
      border: 1px solid rgba(255, 255, 255, 0.06);
      border-radius: 14px;
      padding: 14px 16px;
      box-shadow: 0 20px 45px rgba(0, 0, 0, 0.35);
    }
    .msg-head { display: flex; align-items: center; justify-content: space-between; gap: 10px; margin-bottom: 10px; }
    .msg .ts { color: var(--muted); font-size: 12px; }
    .msg-body { line-height: 1.65; font-size: 15px; }
    .msg-body p { margin: 8px 0; }
    .msg-body h1, .msg-body h2, .msg-body h3, .msg-body h4 { margin: 10px 0 6px 0; }
    .msg-body h1 { font-size: 24px; }
    .msg-body h2 { font-size: 20px; }
    .msg-body h3 { font-size: 18px; }
    .msg-body ul, .msg-body ol { padding-left: 22px; margin: 10px 0; }
    .msg-body li { margin: 6px 0; }
    .msg-body blockquote {
      margin: 10px 0;
      padding-left: 14px;
      border-left: 3px solid rgba(255, 255, 255, 0.18);
      color: #cbd5e1;
    }
    .table-wrap { overflow-x: auto; margin: 12px 0; }
    .msg-body table {
      width: 100%;
      min-width: 420px;
      border-collapse: collapse;
      background: #0b1221;
      border: 1px solid rgba(255, 255, 255, 0.08);
      font-size: 14px;
    }
    .msg-body th, .msg-body td {
      border: 1px solid rgba(255, 255, 255, 0.12);
      padding: 10px 12px;
      text-align: left;
      vertical-align: top;
    }
    .msg-body thead {
      background: rgba(255, 255, 255, 0.06);
      font-weight: 700;
    }
    .msg-body tbody tr:nth-child(even) {
      background: rgba(255, 255, 255, 0.03);
    }
    .msg-body code {
      background: #0b1221;
      border: 1px solid rgba(255, 255, 255, 0.08);
      border-radius: 6px;
      padding: 2px 6px;
      font-family: var(--mono);
      font-size: 13px;
    }
    .msg-body pre {
      background: #0b1221;
      border: 1px solid rgba(255, 255, 255, 0.08);
      border-radius: 12px;
      padding: 12px;
      overflow-x: auto;
      font-family: var(--mono);
      font-size: 13px;
    }
    .msg-body a { color: var(--accent); text-decoration: none; }
    .msg-body a:hover { text-decoration: underline; }
    .msg-body hr { border: none; border-top: 1px solid rgba(255, 255, 255, 0.12); margin: 14px 0; }
    .msg-attachments {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
      gap: 10px;
      margin-top: 12px;
    }
    .msg-attachments img {
      width: 100%;
      border-radius: 12px;
      border: 1px solid rgba(255, 255, 255, 0.12);
      background: #0a0f1c;
    }
    .msg-attachments figcaption {
      font-size: 12px;
      color: var(--muted);
      margin-top: 4px;
      text-align: center;
    }
    .muted { color: var(--muted); }
    @media (max-width: 780px) {
      .hero h1 { font-size: 22px; }
      .msg-body { font-size: 14px; }
      .page { padding: 20px 14px 40px; }
    }
  </style>
</head>
<body>
  <div class="page">
    <header class="hero">
      <h1>${escapeHtml(title)}</h1>
      <div class="meta">
        ${createdAt ? `<span class="pill meta">Создано: ${escapeHtml(formatDate(createdAt))}</span>` : ""}
        ${updatedAt ? `<span class="pill meta">Обновлено: ${escapeHtml(formatDate(updatedAt))}</span>` : ""}
        ${projectId ? `<span class="pill meta">Проект: ${escapeHtml(projectId)}</span>` : ""}
        ${model ? `<span class="pill meta">Модель: ${escapeHtml(model)}</span>` : ""}
      </div>
    </header>
    <section class="messages">
      ${messageHtml}
    </section>
  </div>
</body>
</html>`;
}

function renderSourceOptions(selectedId) {
  const keepSelection = selectedId ?? ui.sourceFilter.value;
  const sources = Array.from(state.projects.reduce((acc, p) => acc.add(p.source_id || "default"), new Set()));
  state.sources = sources;
  ui.sourceFilter.innerHTML = '<option value="">Все аккаунты</option>';
  sources.forEach((s) => {
    const opt = document.createElement("option");
    opt.value = s;
    opt.textContent = s;
    ui.sourceFilter.appendChild(opt);
  });
  if (keepSelection) {
    ui.sourceFilter.value = keepSelection;
  }
}

function renderProjectsOptions(selectedId) {
  const keepSelection = selectedId ?? ui.projectFilter.value;
  const sourceFilter = (ui.sourceFilter.value || "").trim();
  ui.projectFilter.innerHTML = '<option value="">Все проекты</option>';
  const projects = sourceFilter
    ? state.projects.filter((p) => (p.source_id || "default") === sourceFilter)
    : state.projects;
  projects
    .map((p) => ({
      value: getProjectUid(p),
      label: `${p.human_name || p.project_id || "Без названия"} · ${p.source_id || "default"} (${Number(p.conversation_count) || 0})`,
    }))
    .forEach(({ value, label }) => {
      const opt = document.createElement("option");
      opt.value = value;
      opt.textContent = label;
      ui.projectFilter.appendChild(opt);
    });
  if (keepSelection) {
    ui.projectFilter.value = keepSelection;
  }
  const optionValues = Array.from(ui.projectFilter.options).map((opt) => opt.value);
  if (ui.projectFilter.value && !optionValues.includes(ui.projectFilter.value)) {
    ui.projectFilter.value = "";
  }
}

function renderCommonProjectsOptions(selectedId) {
  const sourceFilter = (ui.sourceFilter.value || "").trim();
  if (sourceFilter) {
    ui.commonProjectFilter.classList.add("hidden");
    ui.commonProjectFilter.value = "";
    return;
  }
  const keepSelection = selectedId ?? ui.commonProjectFilter.value;
  ui.commonProjectFilter.classList.remove("hidden");
  ui.commonProjectFilter.innerHTML = '<option value="">Общие проекты</option>';

  const groups = groupProjectsByName(state.projects).filter((group) => {
    const sourceCount = new Set(group.projects.map((p) => p.source_id || "default")).size;
    return sourceCount > 1 && group.projects.length > 1;
  });

  if (!groups.length) {
    const opt = document.createElement("option");
    opt.value = "";
    opt.textContent = "Нет общих проектов";
    opt.disabled = true;
    ui.commonProjectFilter.appendChild(opt);
    ui.commonProjectFilter.value = "";
    return;
  }

  groups.forEach((group) => {
    const opt = document.createElement("option");
    opt.value = makeNameGroupValue(group.name);
    opt.textContent = `${group.name} · общие (${group.conversation_count})`;
    ui.commonProjectFilter.appendChild(opt);
  });

  if (keepSelection) {
    ui.commonProjectFilter.value = keepSelection;
  }
  const optionValues = Array.from(ui.commonProjectFilter.options).map((opt) => opt.value);
  if (ui.commonProjectFilter.value && !optionValues.includes(ui.commonProjectFilter.value)) {
    ui.commonProjectFilter.value = "";
  }
}

function renderMoveTargets(currentProjectId, sourceId) {
  const keep = currentProjectId ?? ui.moveTarget.value;
  const sourceFilter = sourceId || (state.selectedConversation && state.selectedConversation.source_id) || "";
  ui.moveTarget.innerHTML = '<option value="">Выбрать проект</option>';
  const projects = sourceFilter
    ? state.projects.filter((p) => (p.source_id || "default") === sourceFilter)
    : state.projects;
  projects.forEach((p) => {
    const opt = document.createElement("option");
    opt.value = getProjectUid(p);
    opt.textContent = `${p.human_name || p.project_id} (${p.source_id || "default"})`;
    ui.moveTarget.appendChild(opt);
  });
  if (keep) {
    ui.moveTarget.value = keep;
  }
}

function renderModels() {
  ui.modelFilter.innerHTML = '<option value="">Все модели</option>';
  state.models.forEach((m) => {
    const opt = document.createElement("option");
    opt.value = m;
    opt.textContent = m;
    ui.modelFilter.appendChild(opt);
  });
}

function renderConversations() {
  ui.chats.innerHTML = "";
  if (!state.conversations.length) {
    ui.chats.innerHTML = '<div class="empty">Нет чатов по текущему фильтру</div>';
    return;
  }
  state.conversations.forEach((c) => {
    const div = document.createElement("div");
    div.className = "chat";
    const metaPieces = [formatDate(c.updated_at)];
    if (c.model) metaPieces.push(c.model);
    if (c.source_id) metaPieces.push(c.source_id);
    div.innerHTML = `
      <div class="chat-title">${c.title}</div>
      <div class="chat-meta">${metaPieces.filter(Boolean).join(" • ") || " "}</div>
      <div class="chat-snippet">${(c.snippet || "").replace(/</g, "&lt;")}</div>
    `;
    div.onclick = () => loadConversation(c.conversation_id);
    ui.chats.appendChild(div);
  });
}

function renderConversationView(conversation) {
  ui.messages.innerHTML = "";
  if (!conversation) {
    ui.chatTitle.textContent = "Выберите чат";
    ui.chatMeta.textContent = "";
    ui.moveTarget.value = "";
    return;
  }
  ui.chatTitle.textContent = conversation.conversation?.title || "Без названия";
  const convMeta = conversation.conversation || {};
  const model = (convMeta.metadata || {}).model;
  const updatedAt = convMeta.updated_at;
  const sourceId = conversation.source_id || convMeta.source_id || (conversation.conversation || {}).source_id || "";
  const projectId = conversation.project_id || convMeta.project_uid || convMeta.project_id || "";
  const projectName =
    (state.projects.find((p) => getProjectUid(p) === projectId) || {}).human_name || projectId || "";
  const metaFragments = [];
  if (projectName) metaFragments.push(`<strong>${escapeHtml(projectName)}</strong>`);
  if (sourceId) metaFragments.push(`Аккаунт: ${escapeHtml(sourceId)}`);
  if (updatedAt) metaFragments.push(`Обновлено: ${escapeHtml(formatDate(updatedAt))}`);
  if (model) metaFragments.push(escapeHtml(model));
  ui.chatMeta.innerHTML = metaFragments.join(" • ");
  const webPaths = conversation.paths?.web || {};
  const selectedProjectId = isNameGroupValue(ui.projectFilter.value) ? "" : ui.projectFilter.value;
  renderMoveTargets(projectId || selectedProjectId || "", sourceId);
  ui.moveTarget.value = projectId || ui.moveTarget.value;
  ui.copyJson.onclick = async () => {
    const payload = conversation.conversation;
    if (!payload) return;
    try {
      await navigator.clipboard.writeText(JSON.stringify(payload, null, 2));
    } catch (e) {
      console.error("Clipboard copy failed", e);
    }
  };
  ui.copyMd.onclick = async () => {
    const md = conversation.markdown;
    if (!md) return;
    try {
      await navigator.clipboard.writeText(md);
    } catch (e) {
      console.error("Clipboard copy failed", e);
    }
  };
  ui.copyTxt.onclick = async () => {
    const msgs = conversation.conversation?.messages || [];
    const textBlocks = msgs
      .filter((m) => m.role === "user" || m.role === "assistant")
      .map((m) => {
        const plain = stripMarkdown(m.text || "");
        return `${m.role === "user" ? "User" : "Assistant"}: ${plain}`.trim();
      });
    const plain = textBlocks.join("\n\n");
    if (!plain) return;
    try {
      await navigator.clipboard.writeText(plain);
    } catch (e) {
      console.error("Clipboard copy failed", e);
    }
  };

  ui.copyPath.onclick = async () => {
    const path = webPaths.markdown || webPaths.html || webPaths.json || "";
    if (!path) return;
    await navigator.clipboard.writeText(path);
  };

  const messages = conversation.conversation?.messages || [];
  messages
    .filter((m) => m.role === "user" || m.role === "assistant")
    .forEach((m) => {
      const wrap = document.createElement("div");
      wrap.className = `message ${m.role}`;
      const attachments = (m.attachments || [])
        .filter((a) => a.path || a.local_path)
        .map((a) => {
          const src = a.path ? `/files/${encodeURI(a.path)}` : a.local_path;
          if (!src) return "";
          const alt = escapeHtml(a.asset_id || "image");
          return `<figure><img src="${escapeHtml(src)}" alt="${alt}" loading="lazy"/><figcaption>${alt}</figcaption></figure>`;
        })
        .join("");
      wrap.innerHTML = `
        <div class="message-head">
          <span class="pill ${m.role}">${m.role === "user" ? "Пользователь" : "Ассистент"}</span>
          ${m.timestamp ? `<span class="ts">${formatDate(m.timestamp)}</span>` : ""}
        </div>
        <div class="body">${markdownToHtml(m.text || "") || '<p class="muted">Без текста</p>'}</div>
        ${attachments ? `<div class="attachments">${attachments}</div>` : ""}
      `;
      ui.messages.appendChild(wrap);
    });
}

function renderImports() {
  ui.importFile.innerHTML = "";
  if (!state.imports.length) {
    const opt = document.createElement("option");
    opt.value = "";
    opt.textContent = "Нет доступных архивов (.zip) в папке";
    ui.importFile.appendChild(opt);
    ui.importFile.disabled = true;
    ui.importConfirm.disabled = true;
    return;
  }
  ui.importFile.disabled = false;
  ui.importConfirm.disabled = false;
  state.imports.forEach((item) => {
    const opt = document.createElement("option");
    opt.value = item.path || item.name;
    const sizeMb = item.size ? ` · ${(item.size / (1024 * 1024)).toFixed(1)} MB` : "";
    opt.textContent = `${item.name}${sizeMb}`;
    ui.importFile.appendChild(opt);
  });
}

async function loadProjects() {
  const res = await fetch("/api/projects");
  const payload = await res.json();
  state.projects = (payload || []).map((p) => ({
    ...p,
    project_uid: p.project_uid || p.project_id,
    source_id: p.source_id || "default",
  }));
  renderSourceOptions();
  renderProjectsOptions();
  renderCommonProjectsOptions();
  renderMoveTargets();
}

async function loadImports() {
  const res = await fetch("/api/imports");
  state.imports = await res.json();
  renderImports();
}

async function renameCurrentProject() {
  const projectId = ui.projectFilter.value;
  if (!projectId) {
    alert("Сначала выберите проект в фильтре.");
    return;
  }
  if (isNameGroupValue(projectId)) {
    alert("Чтобы переименовать, выберите конкретный проект в конкретном аккаунте.");
    return;
  }
  const current = state.projects.find((p) => getProjectUid(p) === projectId);
  const proposed = window.prompt("Новое название проекта", (current && current.human_name) || projectId);
  if (proposed == null) return; // cancelled
  const human_name = proposed.trim();
  if (!human_name) {
    alert("Название не может быть пустым.");
    return;
  }
  try {
    const res = await fetch("/api/project/rename", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ project_uid: projectId, human_name, source_id: current?.source_id }),
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.error || "Не удалось сохранить новое имя.");
    }
  } catch (e) {
    console.error("Rename failed", e);
    alert(e.message || "Не удалось сохранить новое имя.");
    return;
  }

  await loadProjects();
  ui.projectFilter.value = projectId;
  await loadConversations();
}

function showMoveModal() {
  ui.moveModal.classList.remove("hidden");
}

function hideMoveModal() {
  ui.moveModal.classList.add("hidden");
}

function showImportModal() {
  ui.importModal.classList.remove("hidden");
}

function hideImportModal() {
  ui.importModal.classList.add("hidden");
}

async function moveCurrentConversation() {
  const conv = state.selectedConversation;
  const conversationId = conv?.conversation_id || conv?.conversation?.conversation_id;
  const currentProject = conv?.project_id || conv?.conversation?.project_id || ui.projectFilter.value;
  if (!conversationId) {
    alert("Сначала откройте чат.");
    return;
  }
  const targetId = ui.moveTarget.value || "";
  if (!targetId) {
    alert("Сначала выберите проект назначения.");
    return;
  }
  try {
    const res = await fetch("/api/conversation/move", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ conversation_id: conversationId, target_project_id: targetId }),
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.error || "Не удалось переместить чат.");
    }
  } catch (e) {
    console.error("Move failed", e);
    alert(e.message || "Не удалось переместить чат.");
    return;
  }
  await loadProjects();
  ui.projectFilter.value = targetId;
  await loadConversations();
  await loadConversation(conversationId);
  hideMoveModal();
}

async function runImport() {
  const archivePath = ui.importFile.value;
  const account = ui.importAccount.value.trim() || "default";
  const incremental = ui.importIncremental.checked;
  if (!archivePath) {
    alert("Нет выбранного архива.");
    return;
  }
  ui.importConfirm.disabled = true;
  try {
    const res = await fetch("/api/imports", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ archive: archivePath, account, incremental }),
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.error || "Импорт не удался");
    }
    await loadProjects();
    await loadConversations();
    hideImportModal();
    alert("Импорт завершен");
  } catch (e) {
    console.error("Import failed", e);
    alert(e.message || "Импорт не удался");
  } finally {
    ui.importConfirm.disabled = false;
  }
}

async function resetArchive() {
  const sourceId = (ui.sourceFilter.value || "").trim();
  const scopeLabel = sourceId ? `данные аккаунта "${sourceId}"` : "все импортированные данные";
  if (!window.confirm(`Удалить ${scopeLabel}? (.zip файлы останутся)`)) return;
  try {
    const options = { method: "POST" };
    if (sourceId) {
      options.headers = { "Content-Type": "application/json" };
      options.body = JSON.stringify({ source_id: sourceId });
    }
    const res = await fetch("/api/reset", options);
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.error || "Сброс не удался");
    }
    state.selectedConversation = null;
    ui.messages.innerHTML = "";
    ui.chatTitle.textContent = sourceId ? `Аккаунт ${sourceId} очищен` : "Архив сброшен";
    ui.chatMeta.textContent = "";
    await loadProjects();
    await loadConversations();
    alert(sourceId ? `Данные аккаунта "${sourceId}" удалены.` : "Архив очищен. Теперь можно импортировать заново.");
  } catch (e) {
    console.error("Reset failed", e);
    alert(e.message || "Сброс не удался");
  }
}

async function deleteCurrentConversation() {
  const conv = state.selectedConversation;
  const conversationId = conv?.conversation_id || conv?.conversation?.conversation_id;
  if (!conversationId) {
    alert("Сначала откройте чат.");
    return;
  }
  if (!window.confirm("Удалить чат? Действие необратимо.")) return;
  try {
    const res = await fetch("/api/conversation/delete", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ conversation_id: conversationId }),
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.error || "Не удалось удалить чат.");
    }
  } catch (e) {
    console.error("Delete failed", e);
    alert(e.message || "Не удалось удалить чат.");
    return;
  }
  await loadProjects();
  await loadConversations();
  state.selectedConversation = null;
  renderConversationView(null);
}

function downloadTxt(projectId) {
  const effectiveProject = projectId || ui.commonProjectFilter.value || ui.projectFilter.value;
  const params = new URLSearchParams();
  if (effectiveProject) {
    if (isNameGroupValue(effectiveProject)) {
      params.set("project_name", parseNameGroupValue(effectiveProject));
    } else {
      params.set("project_id", effectiveProject);
    }
  }
  if (ui.sourceFilter.value) params.set("source_id", ui.sourceFilter.value);
  const url = params.toString() ? `/api/export/txt?${params.toString()}` : "/api/export/txt";
  const a = document.createElement("a");
  a.href = url;
  if (effectiveProject) {
    if (isNameGroupValue(effectiveProject)) {
      const name = safeFilename(parseNameGroupValue(effectiveProject));
      a.download = `project-${name}.txt`;
    } else {
      a.download = `project-${effectiveProject}.txt`;
    }
  } else if (ui.sourceFilter.value) {
    a.download = `account-${ui.sourceFilter.value}.txt`;
  } else {
    a.download = "all-projects.txt";
  }
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
}

async function handleProjectAction() {
  const action = ui.projectActions.value;
  if (!action) return;
  if (action === "rename") {
    await renameCurrentProject();
  } else if (action === "download-project") {
    const projectId = ui.projectFilter.value || ui.commonProjectFilter.value;
    if (!projectId) {
      alert("Сначала выберите проект.");
    } else {
      downloadTxt(projectId);
    }
  } else if (action === "download-all") {
    downloadTxt();
  }
  ui.projectActions.value = "";
}

async function loadModels() {
  // Варианты ограничены двумя значениями
  state.models = ["Chat", "Research"];
  renderModels();
}

async function loadConversations() {
  const params = new URLSearchParams();
  if (ui.search.value.trim()) params.set("q", ui.search.value.trim());
  if (ui.sourceFilter.value) params.set("source_id", ui.sourceFilter.value);
  const commonProject = ui.commonProjectFilter.value;
  const directProject = ui.projectFilter.value;
  if (commonProject) {
    params.set("project_name", parseNameGroupValue(commonProject));
  } else if (directProject) {
    params.set("project_id", directProject);
  }
  if (ui.modelFilter.value) params.set("model", ui.modelFilter.value);

  const res = await fetch(`/api/conversations?${params.toString()}`);
  const data = await res.json();
  state.conversations = (data || []).map((c) => ({ ...c, source_id: c.source_id || "default" }));
  renderConversations();
}

async function loadConversation(id) {
  const res = await fetch(`/api/conversation/${id}`);
  if (!res.ok) return;
  const data = await res.json();
  data.source_id = data.source_id || (data.conversation || {}).source_id || "default";
  state.selectedConversation = data;
  renderConversationView(data);
}

function attachHandlers() {
  let searchTimer = null;
  ui.search.addEventListener("input", () => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(loadConversations, 250);
  });
  ui.sourceFilter.addEventListener("change", () => {
    renderProjectsOptions();
    renderCommonProjectsOptions();
    ui.projectFilter.value = "";
    ui.commonProjectFilter.value = "";
    loadConversations();
  });
  ui.projectFilter.addEventListener("change", () => {
    if (ui.projectFilter.value) ui.commonProjectFilter.value = "";
    loadConversations();
  });
  ui.commonProjectFilter.addEventListener("change", () => {
    if (ui.commonProjectFilter.value) ui.projectFilter.value = "";
    loadConversations();
  });
  ui.modelFilter.addEventListener("change", loadConversations);
  ui.projectActions.addEventListener("change", handleProjectAction);
  ui.moveChat.addEventListener("click", () => {
    if (!state.selectedConversation) {
      alert("Сначала откройте чат.");
      return;
    }
    const convProj =
      state.selectedConversation.project_id ||
      state.selectedConversation.conversation?.project_uid ||
      state.selectedConversation.conversation?.project_id;
    const convSource =
      state.selectedConversation.source_id || state.selectedConversation.conversation?.source_id || ui.sourceFilter.value;
    const selectedProjectId = isNameGroupValue(ui.projectFilter.value) ? "" : ui.projectFilter.value;
    renderMoveTargets(convProj || selectedProjectId || "", convSource || "");
    ui.moveTarget.value = convProj || ui.moveTarget.value;
    showMoveModal();
  });
  ui.moveConfirm.addEventListener("click", moveCurrentConversation);
  ui.moveCancel.addEventListener("click", hideMoveModal);
  ui.deleteChat.addEventListener("click", deleteCurrentConversation);
  ui.addArchive.addEventListener("click", async () => {
    await loadImports();
    showImportModal();
  });
  ui.importConfirm.addEventListener("click", runImport);
  ui.importCancel.addEventListener("click", hideImportModal);
  ui.resetArchive.addEventListener("click", resetArchive);
}

async function bootstrap() {
  attachHandlers();
  await loadProjects();
  await loadImports();
  await loadModels();
  await loadConversations();
}

bootstrap();
