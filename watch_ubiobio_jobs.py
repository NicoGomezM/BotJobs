#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Watcher de empleos UBB (páginas 1..5)
- Detecta nuevas ofertas
- Filtra TI por keywords
- Notifica por EMAIL via SMTP
- Estado persistente: PostgreSQL si DATABASE_URL, si no JSON
- Corre cada hora en loop (apto para Railway como worker)
"""

import os
import re
import json
import time
import smtplib
import logging
import ssl
from email.mime.text import MIMEText
from email.utils import formataddr
from pathlib import Path
from typing import List, Dict, Tuple

import requests
from bs4 import BeautifulSoup

# Postgres opcional
USE_DB = False
DB = None
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except Exception:
    pass

# =========================
# Configuración
# =========================
BASE_URL = "https://rrii.ubiobio.cl/ver-empleos/"
MAX_PAGES = int(os.getenv("MAX_PAGES", "5"))

STATE_FILE = Path(os.getenv("UBBIO_STATE_FILE", str(Path("/data/") / "ubiobio_jobs_seen.json")))
STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

USER_AGENT = os.getenv("UBBIO_UA", "Mozilla/5.0 (compatible; UBBioWatcher/1.1)")

# SMTP (obligatorio para correos)
SMTP_HOST     = os.getenv("SMTP_HOST", "").strip()
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))  # 587 TLS, 465 SSL
SMTP_USER     = os.getenv("SMTP_USER", "").strip()
SMTP_PASS     = os.getenv("SMTP_PASS", "").strip()
SMTP_FROM     = os.getenv("SMTP_FROM", SMTP_USER).strip()
SMTP_FROMNAME = os.getenv("SMTP_FROMNAME", "UBB Empleos Bot").strip()
SMTP_TO       = os.getenv("SMTP_TO", "").strip()  # coma-separado si múltiples
SMTP_USE_SSL  = os.getenv("SMTP_USE_SSL", "false").lower() == "true"  # true => 465 SSL
SMTP_SUBJECT_PREFIX = os.getenv("SMTP_SUBJECT_PREFIX", "[UBB Empleos]")

# Palabras clave TI
KEYWORDS = [
    r"inform[aá]tica", r"inform[aá]tico", r"computaci[oó]n",
    r"software", r"desarrollador", r"developer", r"programador", r"programming",
    r"full[\s-]?stack", r"backend", r"frontend", r"devops", r"\bqa\b", r"test(?:er|ing)",
    r"data\s*science", r"cient[íi]fico de datos", r"analista\s*datos", r"machine\s*learning",
    r"sistemas", r"\bTI\b", r"tecnolog[íi]as de la informaci[oó]n", r"ciberseguridad",
    r"python", r"java(?:script)?", r"typescript", r"react", r"node\.?js",
    r"sql", r"postgres", r"mongodb", r"docker", r"kubernetes", r"aws", r"azure", r"gcp",
]
KEYWORDS_RE = re.compile("|".join(KEYWORDS), re.IGNORECASE)

# =========================
# Logging
# =========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# =========================
# DB Helpers
# =========================
def db_connect_if_available():
    global USE_DB, DB
    dsn = os.getenv("DATABASE_URL", "").strip()
    if not dsn or "psycopg2" not in globals():
        USE_DB = False
        return
    try:
        DB = psycopg2.connect(dsn)
        DB.autocommit = True
        with DB.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS seen_jobs(
                    link TEXT PRIMARY KEY,
                    first_seen TIMESTAMPTZ DEFAULT NOW()
                );
            """)
        USE_DB = True
        logging.info("Persistencia: PostgreSQL habilitado.")
    except Exception as e:
        logging.warning(f"No se pudo conectar a DATABASE_URL, se usará JSON. Error: {e}")
        USE_DB = False

def seen_load() -> set:
    if USE_DB and DB:
        try:
            with DB.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT link FROM seen_jobs;")
                return {row["link"] for row in cur.fetchall()}
        except Exception as e:
            logging.error(f"DB read error, fallback JSON: {e}")
    # JSON
    try:
        if STATE_FILE.exists():
            return set(json.loads(STATE_FILE.read_text(encoding="utf-8")))
    except Exception as e:
        logging.warning(f"No se pudo leer {STATE_FILE}: {e}")
    return set()

def seen_add(links: List[str]):
    if not links:
        return
    if USE_DB and DB:
        try:
            with DB.cursor() as cur:
                cur.executemany(
                    "INSERT INTO seen_jobs(link) VALUES(%s) ON CONFLICT DO NOTHING;",
                    [(l,) for l in links]
                )
            return
        except Exception as e:
            logging.error(f"DB write error, fallback JSON: {e}")
    # JSON
    try:
        s = seen_load()
        s.update(links)
        STATE_FILE.write_text(json.dumps(sorted(s), ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        logging.error(f"No se pudo guardar {STATE_FILE}: {e}")

# =========================
# Scraper
# =========================
def http_get(url: str) -> str:
    resp = requests.get(url, timeout=30, headers={"User-Agent": USER_AGENT})
    resp.raise_for_status()
    return resp.text

def page_url(i: int) -> str:
    return BASE_URL if i <= 1 else BASE_URL.rstrip("/") + f"/{i}/"

def extract_jobs_from_html(html: str) -> List[Tuple[str, str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    jobs = []
    # heurística: títulos en h3 con <a href>, con snippet en el siguiente bloque
    for h in soup.find_all("h3"):
        a = h.find("a")
        if not a:
            continue
        title = a.get_text(" ", strip=True)
        link = a.get("href") or ""
        if not link.startswith("http"):
            continue
        snippet = ""
        sib = h.find_next_sibling()
        if sib:
            snippet = sib.get_text(" ", strip=True)[:280]
        jobs.append((title, link, snippet))
    return jobs

def is_it_job_related(title: str, snippet: str) -> bool:
    return bool(KEYWORDS_RE.search(f"{title} {snippet}"))

# =========================
# Email
# =========================
def send_email(subject: str, html_body: str):
    if not (SMTP_HOST and SMTP_USER and SMTP_PASS and SMTP_TO):
        logging.warning("SMTP no configurado: setea SMTP_HOST, SMTP_USER, SMTP_PASS, SMTP_TO.")
        logging.info("Previsualización email:\n" + subject + "\n" + html_body)
        return

    msg = MIMEText(html_body, "html", "utf-8")
    msg["Subject"] = subject
    msg["From"] = formataddr((SMTP_FROMNAME, SMTP_FROM or SMTP_USER))
    msg["To"] = SMTP_TO

    if SMTP_USE_SSL:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=context, timeout=30) as server:
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(msg["From"], [x.strip() for x in SMTP_TO.split(",")], msg.as_string())
    else:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
            server.ehlo()
            server.starttls(context=ssl.create_default_context())
            server.ehlo()
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(msg["From"], [x.strip() for x in SMTP_TO.split(",")], msg.as_string())

# =========================
# Core
# =========================
def run_once() -> Dict[str, List[Dict]]:
    seen = seen_load()
    found_it: List[Dict] = []
    new_all: List[Dict] = []
    new_it:  List[Dict] = []
    newly_seen_links: List[str] = []

    for page in range(1, MAX_PAGES + 1):
        url = page_url(page)
        try:
            html = http_get(url)
        except Exception as e:
            logging.error(f"HTTP error p{page}: {e}")
            continue

        for title, link, snippet in extract_jobs_from_html(html):
            job = {"title": title, "url": link, "snippet": snippet, "page": page}
            ti_ok = is_it_job_related(title, snippet)
            if ti_ok:
                found_it.append(job)
            if link not in seen:
                new_all.append(job)
                if ti_ok:
                    new_it.append(job)
                newly_seen_links.append(link)

    if newly_seen_links:
        seen_add(newly_seen_links)

    return {"new_all": new_all, "new_it": new_it, "all_it": found_it}

def fmt_jobs_html(jobs: List[Dict], heading: str) -> str:
    if not jobs:
        return f"<h3>{heading}</h3><p><i>Sin resultados</i></p>"
    lis = "\n".join([f'<li><a href="{j["url"]}">{j["title"]}</a> (p{j["page"]})</li>' for j in jobs])
    return f"<h3>{heading}</h3><ul>{lis}</ul>"

def main_loop():
    db_connect_if_available()
    while True:
        try:
            res = run_once()
            subject = f"{SMTP_SUBJECT_PREFIX} nuevas: {len(res['new_all'])} | nuevas TI: {len(res['new_it'])}"
            body = (
                "<div>"
                + fmt_jobs_html(res["new_all"], "Nuevas ofertas (todas)")
                + fmt_jobs_html(res["new_it"], "Nuevas ofertas TI")
                + fmt_jobs_html(res["all_it"][:25], "Ofertas TI recientes (máx 25)")
                + "</div>"
            )
            send_email(subject, body)
        except Exception as e:
            logging.exception(f"Falla en iteración: {e}")
        # Espera 1 hora
        time.sleep(3600)

if __name__ == "__main__":
    main_loop()
