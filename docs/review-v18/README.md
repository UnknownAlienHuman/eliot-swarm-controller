# Доказательства проверки v18

`python review-v18/reproduce_sql.py` — сравнивает сохранённую v17 DDL с v18 в памяти.
`python review-v18/validate_transitions.py` — проверяет узкий SQL begin_send и последовательные модели правил.
`python review-v18/validate_package.py` — синтаксис и согласованность комплекта; Python 3.11+.

Это утилиты проверки проектных артефактов. Они не запускают Rust, vendor SDK, Windows Jobs или модели.
Сохраняемые results повторяемы по логике; версия SQLite и пути отражают локальный интерпретатор.
`source-v17/` — неизменённые исходные тексты и DDL, а не действующая версия.
