"""Генерирует INDEX.md: файлы с размерами, карта разделов v2.6, рецепты контекста.

Запуск после split_v26.py: python3 -I build_index.py <v2.6.md> <папка docs>
"""
import os, re, sys
from collections import defaultdict

src, docs = sys.argv[1], sys.argv[2]
ARROW = re.compile(r" \[↗\]\([^)]*\)")

def size(f):
    return os.path.getsize(os.path.join(docs, f))

def kb(n):
    return f"{n / 1024:.0f}"

files = []
for root, _, names in os.walk(docs):
    for n in names:
        rel = os.path.relpath(os.path.join(root, n), docs)
        if rel != "INDEX.md":
            files.append(rel)

def purpose(f):
    if not f.endswith(".md") or f == "tools/ratchet.toml":
        return {"tools/check.py": "проверка автоматов по таблицам",
                "tools/deps.py": "запрещённые рёбра крейтов (храповик)",
                "tools/scc.py": "самый большой цикл модулей (храповик)",
                "tools/split_v26.py": "сборка этой документации из v2.6",
                "tools/coverage.py": "проверка, что каждая строка v2.6 есть в документах",
                "tools/build_index.py": "генерация этого файла",
                "tools/build_backlog.py": "генерация backlog.md из таблицы этапов",
                "tools/ratchet.py": "храповик для CI: deps, scc, check с порогами",
                "tools/ratchet.toml": "пороги храповика (текущие числа main)"}.get(f, "")
    text = open(os.path.join(docs, f), encoding="utf-8").read()
    title = text.split("\n", 1)[0].lstrip("# ").strip()
    return title

# --- карта разделов: где лежат строки каждого раздела и подраздела v2.6
corpus = defaultdict(set)
for f in files:
    if f != "archive/spec-v2.6.md" and (f.endswith(".md") or f in ("tools/check.py", "tools/deps.py", "tools/scc.py")):
        for line in open(os.path.join(docs, f), encoding="utf-8").read().split("\n"):
            line = ARROW.sub("", line).rstrip()
            if line.strip():
                corpus[line].add(f)

rows, cur = [], None
for line in open(src, encoding="utf-8").read().split("\n"):
    if line.startswith("## ") or (line.startswith("### ") and cur and cur["split"]):
        level = 2 if line.startswith("## ") else 3
        title = line.lstrip("# ").strip()
        cur = {"title": title, "level": level, "files": defaultdict(int),
               "split": level == 2 and re.match(r"(11а|14)\.", title) is not None}
        if level == 3:
            cur["split"] = True
        rows.append(cur)
        continue
    if cur and line.strip() and not line.startswith("```") and not re.fullmatch(r"[|\s:-]+", line):
        for f in corpus.get(line.rstrip(), ()):
            cur["files"][f] += 1

def link(f):
    return f"[{f}]({f})"

out = ["""# Индекс и навигация

> Документация Swarm собрана из целевой архитектуры v2.6 (27.11.2026). Этот файл генерируется скриптом [tools/build_index.py](tools/build_index.py); правь скрипт, а не файл.

## Как пользоваться

1. Всегда читай [ARCHITECTURE.md](ARCHITECTURE.md): суть, роли, правила, модель. Это минимум контекста.
2. Возьми рецепт для своего типа задачи ниже: 1–3 файла блока. Каждый файл начинается со шапки (зачем, владеет, вход, выход, инварианты, зависит от, статус) и дальше содержит дословный текст v2.6.
3. Ссылка «раздел N» в тексте — номер раздела v2.6; стрелка ↗ рядом ведёт туда, где он теперь. Раздел, разнесённый по нескольким файлам, — в карте разделов ниже.
4. Добавляешь зависимость, таблицу или модуль — [GRAPH.md](GRAPH.md). Опираешься на механизм — метка в [STATUS.md](STATUS.md). Старое слово из кода или разговора — словарь в [core.md](implementation/core.md#словарь-старое-слово--имя-в-спецификации).
5. Следующая задача — первый неотмеченный пункт [backlog.md](backlog.md); там же контекст этапа и открытые вопросы.
6. [archive/](archive/HISTORY.md) — история версий, доноры, инвентаризация старой документации, полный текст спецификации v2.6 ([archive/spec-v2.6.md](archive/spec-v2.6.md)). Исполнителю не нужен.
7. Остальное в `docs/` вне `docs/swarm/` (vision, architecture, roadmap, decisions, spec, harnesses, reference) описывает **текущий** код до миграции. Где оно расходится с этими документами, правы эти документы; отменённые решения отмечены в `docs/decisions.md`. Контракт desktop — `docs/design/fluent-design-system.md`, он действует.

## Рецепт контекста задачи
"""]

RECIPES = [
    ("Store: таблица, схема, admission, транзакция", ["implementation/store.md", "implementation/core.md", "GRAPH.md"]),
    ("Переход задачи, детектор, разбор, бюджет, gate", ["implementation/task-controller.md", "implementation/automata.md", "implementation/config.md"]),
    ("Супервизия, захват, поколения, сверка", ["implementation/daemon.md", "implementation/store.md"]),
    ("Runtime агента, доставка сообщений", ["implementation/runtime.md", "implementation/core.md"]),
    ("Интегратор: merge, проверки, push, чистка", ["implementation/integrator.md", "implementation/plugins/checks.md", "implementation/plugins/github.md"]),
    ("Хуки и доставка в ход", ["implementation/hooks.md", "implementation/runtime.md", "+ плагин харнесса"]),
    ("Плагин харнесса X (новый или правка)", ["implementation/plugins/README.md", "implementation/plugins/<X>.md", "implementation/runtime.md"]),
    ("Маршруты, квоты, ошибки провайдера, Hold", ["implementation/routing.md", "+ плагин харнесса"]),
    ("Облачный исполнитель", ["implementation/plugins/cloud-agents.md", "implementation/integrator.md", "implementation/plugins/github.md"]),
    ("Удалённый MCP", ["implementation/remote-mcp.md", "implementation/cli-mcp.md", "implementation/security.md"]),
    ("Команда CLI или MCP, реестр", ["implementation/cli-mcp.md", "GRAPH.md"]),
    ("Секреты и права агентов", ["implementation/security.md", "implementation/integrator.md"]),
    ("Конфигурация конвейера (workflow, routes, triggers)", ["implementation/config.md", "implementation/task-controller.md"]),
    ("Эксплуатация: наблюдение, учёт, диск, обновление", ["implementation/operations.md", "implementation/daemon.md"]),
    ("Этап миграции N", ["implementation/migration.md", "STATUS.md", "+ файлы блоков этапа"]),
    ("Сквозной тест или поведение при сбое", ["implementation/scenarios.md", "+ файл блока"]),
    ("Решение по архитектуре, новый модуль или сущность", ["GRAPH.md", "implementation/decisions.md", "STATUS.md"]),
]
out.append("Всегда плюс ARCHITECTURE.md (" + kb(size("ARCHITECTURE.md")) + " КБ). Размер — сумма с ним; файлы с «+» и «<X>» не считаются.\n")
out.append("| Задача | Файлы | КБ |\n| --- | --- | --- |")
for name, fs in RECIPES:
    total = size("ARCHITECTURE.md") + sum(size(f) for f in fs if os.path.exists(os.path.join(docs, f)))
    cells = ", ".join(link(f) if os.path.exists(os.path.join(docs, f)) else f for f in fs)
    out.append(f"| {name} | {cells} | {kb(total)} |")

out.append("\n## Файлы\n\n| Файл | Что внутри | КБ |\n| --- | --- | --- |")
order = lambda f: (f.startswith("archive") * 3 + f.startswith("tools") * 2 + ("/" in f) * 1, f)
for f in sorted(files, key=order):
    out.append(f"| {link(f)} | {purpose(f)} | {kb(size(f))} |")

out.append("\n## Карта разделов\n\nКуда попал каждый раздел v2.6 (по числу перенесённых строк). Каждая непустая строка v2.6 есть в документах дословно — это проверяет [tools/coverage.py](tools/coverage.py).\n")
out.append("| Раздел v2.6 | Где теперь |\n| --- | --- |")
for r in rows:
    fs = sorted(r["files"].items(), key=lambda kv: -kv[1])
    if not fs:
        continue
    main = [f for f, n in fs if n >= max(3, fs[0][1] // 10)] or [fs[0][0]]
    rest = [f for f, n in fs if f not in main]
    cell = ", ".join(link(f) for f in main) + ((" (по нескольку строк: " + ", ".join(os.path.basename(f).removesuffix(".md") for f in rest) + ")") if rest else "")
    title = ("— " if r["level"] == 3 else "") + r["title"]
    out.append(f"| {title} | {cell} |")

open(os.path.join(docs, "INDEX.md"), "w", encoding="utf-8").write("\n".join(out) + "\n")
print("INDEX.md:", os.path.getsize(os.path.join(docs, "INDEX.md")), "байт")
