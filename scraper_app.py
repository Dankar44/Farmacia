"""
FarmaSearch - Desktop Scraper Manager
Aplicacion de escritorio para ejecutar y monitorizar scrapers.
Doble clic en este archivo para abrir (o: python scraper_app.py)
"""

import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import subprocess
import threading
import sys
import os
import time
from datetime import datetime

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

# Try to load DB connection
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(PROJECT_ROOT, '.env'))
    from sqlalchemy import create_engine, text
    import urllib.parse

    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "postgres")
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "postgres")
    encoded_password = urllib.parse.quote_plus(password)
    DB_URL = f"postgresql://{user}:{encoded_password}@{host}:{port}/{db_name}"
    engine = create_engine(DB_URL)
    HAS_DB = True
except Exception as e:
    HAS_DB = False
    print(f"DB not available: {e}")


# Scraper definitions with per-level scripts
SCRAPERS = [
    {"key": "DosFarma", "method": "Algolia API", "levels": {
        1: "scrapers/dosfarma/level1_api.py", 2: "scrapers/dosfarma/level2_http.py", 3: "scrapers/dosfarma/level3_playwright.py"}},
    {"key": "Atida", "method": "Algolia API", "levels": {
        1: "scrapers/atida/level1_api.py", 2: "scrapers/atida/level2_http.py", 3: "scrapers/atida/level3_playwright.py"}},
    {"key": "PromoFarma", "method": "Sitemap + Playwright", "levels": {
        2: "scrapers/promofarma/level2_http.py", 3: "scrapers/promofarma/level3_playwright.py"}},
    {"key": "FarmaciasVazquez", "method": "Doofinder API", "levels": {
        1: "scrapers/vazquez/level1_api.py", 2: "scrapers/vazquez/level2_http.py", 3: "scrapers/vazquez/level3_playwright.py"}},
    {"key": "FarmaciasDirect", "method": "Empathy API", "levels": {
        1: "scrapers/farmaciasdirect/level1_api.py", 2: "scrapers/farmaciasdirect/level2_http.py", 3: "scrapers/farmaciasdirect/level3_playwright.py"}},
    {"key": "Farmacoslada", "method": "Sitemap + aiohttp", "levels": {
        2: "scrapers/farmacoslada/level2_http.py", 3: "scrapers/farmacoslada/level3_playwright.py"}},
    {"key": "Okfarma", "method": "Sitemap + Playwright", "levels": {
        2: "scrapers/okfarma/level2_http.py", 3: "scrapers/okfarma/level3_playwright.py"}},
    {"key": "FarmaciaBarata", "method": "Sitemap + Playwright", "levels": {
        2: "scrapers/farmaciabarata/level2_http.py", 3: "scrapers/farmaciabarata/level3_playwright.py"}},
]


class ScraperApp:
    def __init__(self, root):
        self.root = root
        self.root.title("FarmaSearch - Panel de Scrapers")
        self.root.geometry("900x700")
        self.root.minsize(750, 550)

        # Colors
        self.PRIMARY = "#00a99d"
        self.PRIMARY_DARK = "#008f85"
        self.PRIMARY_LIGHT = "#e0f7f5"
        self.ACCENT = "#f57c00"
        self.BG = "#f5f6f8"
        self.BG_WHITE = "#ffffff"
        self.TEXT_DARK = "#1a1a2e"
        self.TEXT_MUTED = "#9ca3af"
        self.GREEN = "#059669"
        self.RED = "#dc2626"

        self.root.configure(bg=self.BG)

        # State
        self.active_processes = {}
        self.product_counts = {}

        # Build UI
        self.build_header()
        self.build_stats()
        self.build_scraper_list()
        self.build_log_panel()
        self.build_bottom_bar()

        # Load initial data
        self.refresh_counts()

    def build_header(self):
        header = tk.Frame(self.root, bg=self.PRIMARY, height=56)
        header.pack(fill=tk.X)
        header.pack_propagate(False)

        tk.Label(header, text="✚", font=("Segoe UI", 18, "bold"),
                 bg=self.PRIMARY, fg="white").pack(side=tk.LEFT, padx=(16, 8))
        tk.Label(header, text="FarmaSearch", font=("Segoe UI", 16, "bold"),
                 bg=self.PRIMARY, fg="white").pack(side=tk.LEFT)
        tk.Label(header, text="Panel de Scrapers", font=("Segoe UI", 10),
                 bg=self.PRIMARY, fg="#b2dfdb").pack(side=tk.LEFT, padx=(10, 0))

        # DB status
        db_text = "BD conectada" if HAS_DB else "BD no disponible"
        db_color = "#80ffb4" if HAS_DB else "#ff8a80"
        tk.Label(header, text=f"● {db_text}", font=("Segoe UI", 9),
                 bg=self.PRIMARY, fg=db_color).pack(side=tk.RIGHT, padx=16)

    def build_stats(self):
        stats_frame = tk.Frame(self.root, bg=self.BG)
        stats_frame.pack(fill=tk.X, padx=16, pady=(12, 8))

        self.stat_labels = {}
        for i, (key, label) in enumerate([("productos", "Productos"), ("precios", "Precios"), ("farmacias", "Farmacias")]):
            card = tk.Frame(stats_frame, bg=self.BG_WHITE, relief="flat", bd=0,
                           highlightbackground="#e5e7eb", highlightthickness=1)
            card.pack(side=tk.LEFT, expand=True, fill=tk.X, padx=(0 if i == 0 else 4, 0))

            val = tk.Label(card, text="...", font=("Segoe UI", 18, "bold"),
                          bg=self.BG_WHITE, fg=self.TEXT_DARK)
            val.pack(pady=(10, 0))
            tk.Label(card, text=label, font=("Segoe UI", 9),
                    bg=self.BG_WHITE, fg=self.TEXT_MUTED).pack(pady=(0, 10))
            self.stat_labels[key] = val

    def build_scraper_list(self):
        list_frame = tk.Frame(self.root, bg=self.BG)
        list_frame.pack(fill=tk.BOTH, expand=True, padx=16, pady=(0, 4))

        # Header
        hdr = tk.Frame(list_frame, bg=self.BG)
        hdr.pack(fill=tk.X, pady=(0, 6))
        tk.Label(hdr, text="Farmacias", font=("Segoe UI", 11, "bold"),
                bg=self.BG, fg=self.TEXT_DARK).pack(side=tk.LEFT)

        btn_all = tk.Button(hdr, text="▶ Ejecutar Todos", font=("Segoe UI", 9, "bold"),
                           bg=self.PRIMARY, fg="white", relief="flat", cursor="hand2",
                           command=self.run_all_scrapers, padx=12, pady=4)
        btn_all.pack(side=tk.RIGHT)

        # Scrollable list
        canvas = tk.Canvas(list_frame, bg=self.BG, highlightthickness=0)
        scrollbar = ttk.Scrollbar(list_frame, orient="vertical", command=canvas.yview)
        self.scraper_frame = tk.Frame(canvas, bg=self.BG)

        self.scraper_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=self.scraper_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # Build scraper rows
        self.scraper_widgets = {}
        for scraper in SCRAPERS:
            self.build_scraper_row(scraper)

    def build_scraper_row(self, scraper):
        key = scraper["key"]

        row = tk.Frame(self.scraper_frame, bg=self.BG_WHITE, relief="flat",
                      highlightbackground="#e5e7eb", highlightthickness=1)
        row.pack(fill=tk.X, pady=2)

        # Left: name and info
        left = tk.Frame(row, bg=self.BG_WHITE)
        left.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=12, pady=10)

        name_label = tk.Label(left, text=key, font=("Segoe UI", 10, "bold"),
                             bg=self.BG_WHITE, fg=self.TEXT_DARK)
        name_label.pack(anchor="w")

        info_label = tk.Label(left, text=f"{scraper['method']} · ... productos",
                             font=("Segoe UI", 8), bg=self.BG_WHITE, fg=self.TEXT_MUTED)
        info_label.pack(anchor="w")

        # Status label
        status = tk.Label(row, text="Pendiente", font=("Segoe UI", 8, "bold"),
                         bg="#f3f4f6", fg=self.TEXT_MUTED, padx=8, pady=2)
        status.pack(side=tk.LEFT, padx=4)

        # Right: buttons
        btn_frame = tk.Frame(row, bg=self.BG_WHITE)
        btn_frame.pack(side=tk.RIGHT, padx=8, pady=6)

        # Level buttons
        levels = scraper["levels"]
        for lvl in [1, 2, 3]:
            available = lvl in levels
            lvl_names = {1: "N1 API", 2: "N2 HTTP", 3: "N3 PW"}
            if available:
                colors = {1: (self.PRIMARY_LIGHT, self.PRIMARY_DARK),
                         2: ("#fff3e0", "#e65100"),
                         3: ("#f3f4f6", "#374151")}
                bg_c, fg_c = colors[lvl]
                script_path = levels[lvl]
                btn = tk.Button(btn_frame, text=lvl_names[lvl], font=("Segoe UI", 8),
                               bg=bg_c, fg=fg_c, relief="flat", cursor="hand2",
                               padx=6, pady=2,
                               command=lambda k=key, l=lvl, s=script_path: self.run_scraper(k, s, l))
            else:
                btn = tk.Button(btn_frame, text=f"{lvl_names[lvl]} N/D", font=("Segoe UI", 8),
                               bg="#f3f4f6", fg="#d1d5db", relief="flat", state="disabled",
                               padx=6, pady=2)
            btn.pack(side=tk.LEFT, padx=1)

        # Stop button (hidden by default)
        stop_btn = tk.Button(btn_frame, text="⏹", font=("Segoe UI", 9),
                            bg="#fef2f2", fg=self.RED, relief="flat", cursor="hand2",
                            padx=4, command=lambda k=key: self.stop_scraper(k))

        self.scraper_widgets[key] = {
            "row": row, "info": info_label, "status": status,
            "stop_btn": stop_btn, "btn_frame": btn_frame
        }

    def build_log_panel(self):
        log_frame = tk.Frame(self.root, bg=self.BG)
        log_frame.pack(fill=tk.BOTH, expand=True, padx=16, pady=(4, 0))

        hdr = tk.Frame(log_frame, bg=self.BG)
        hdr.pack(fill=tk.X, pady=(0, 4))
        tk.Label(hdr, text="Logs", font=("Segoe UI", 10, "bold"),
                bg=self.BG, fg=self.TEXT_DARK).pack(side=tk.LEFT)

        btn_clear = tk.Button(hdr, text="Limpiar", font=("Segoe UI", 8),
                             bg="#f3f4f6", fg=self.TEXT_MUTED, relief="flat",
                             cursor="hand2", padx=8, command=self.clear_logs)
        btn_clear.pack(side=tk.RIGHT)

        self.log_text = scrolledtext.ScrolledText(
            log_frame, font=("Consolas", 9), bg="#1a1a2e", fg="#a0e8a0",
            insertbackground="#a0e8a0", relief="flat", height=10, wrap=tk.WORD,
            state="disabled"
        )
        self.log_text.pack(fill=tk.BOTH, expand=True)

    def build_bottom_bar(self):
        bottom = tk.Frame(self.root, bg=self.BG_WHITE, height=36)
        bottom.pack(fill=tk.X, side=tk.BOTTOM)
        bottom.pack_propagate(False)

        self.bottom_status = tk.Label(bottom, text="Listo", font=("Segoe UI", 8),
                                     bg=self.BG_WHITE, fg=self.TEXT_MUTED)
        self.bottom_status.pack(side=tk.LEFT, padx=12)

        tk.Button(bottom, text="Actualizar datos", font=("Segoe UI", 8),
                 bg=self.PRIMARY_LIGHT, fg=self.PRIMARY_DARK, relief="flat",
                 cursor="hand2", padx=10, command=self.refresh_counts).pack(side=tk.RIGHT, padx=8, pady=4)

    # ==================== Actions ====================

    def log(self, message, tag=None):
        """Write to the log panel."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.configure(state="normal")
        line = f"[{timestamp}] {message}\n"

        if tag == "error":
            self.log_text.insert(tk.END, line, "error")
        elif tag == "success":
            self.log_text.insert(tk.END, line, "success")
        else:
            self.log_text.insert(tk.END, line)

        self.log_text.see(tk.END)
        self.log_text.configure(state="disabled")

    def set_status(self, key, text, color):
        w = self.scraper_widgets.get(key)
        if w:
            w["status"].configure(text=text, fg=color,
                                 bg=self.PRIMARY_LIGHT if color == self.GREEN else
                                   "#fff3e0" if color == self.ACCENT else
                                   "#fef2f2" if color == self.RED else "#f3f4f6")

    def refresh_counts(self):
        """Load product counts from Supabase."""
        if not HAS_DB:
            self.log("BD no disponible - no se pueden cargar conteos", "error")
            return

        def _load():
            try:
                with engine.connect() as con:
                    counts = con.execute(text("SELECT farmacia, COUNT(*) as c FROM productos GROUP BY farmacia")).fetchall()
                    total_prod = sum(r.c for r in counts)
                    total_prec = con.execute(text("SELECT COUNT(*) FROM precios")).scalar()
                    count_map = {r.farmacia: r.c for r in counts}

                self.root.after(0, lambda: self._update_stats(count_map, total_prod, total_prec))
            except Exception as e:
                self.root.after(0, lambda: self.log(f"Error cargando datos: {e}", "error"))

        threading.Thread(target=_load, daemon=True).start()

    def _update_stats(self, count_map, total_prod, total_prec):
        fmt = lambda n: f"{n:,}".replace(",", ".")
        self.stat_labels["productos"].configure(text=fmt(total_prod))
        self.stat_labels["precios"].configure(text=fmt(total_prec))
        self.stat_labels["farmacias"].configure(text=str(len(count_map)))

        for scraper in SCRAPERS:
            key = scraper["key"]
            count = count_map.get(key, 0)
            w = self.scraper_widgets.get(key)
            if w:
                w["info"].configure(text=f"{scraper['method']} · {fmt(count)} productos")

        self.bottom_status.configure(text=f"Actualizado: {datetime.now().strftime('%H:%M:%S')}")
        self.log(f"Datos cargados: {fmt(total_prod)} productos, {fmt(total_prec)} precios")

    def run_scraper(self, key, script, level):
        """Run a scraper in a background thread."""
        if key in self.active_processes:
            messagebox.showwarning("En curso", f"{key} ya esta ejecutandose")
            return

        # Ask for limit
        limit_str = tk.simpledialog.askstring(
            "Limite de productos",
            f"Ejecutar {key} - Nivel {level}\n\nProductos a scrapear (0 = todos):",
            initialvalue="100"
        )
        if limit_str is None:
            return

        limit = int(limit_str) if limit_str.isdigit() else 0
        level_names = {1: "API", 2: "HTTP", 3: "Playwright"}

        self.log(f"Iniciando {key} - Nivel {level} ({level_names.get(level, '?')}) - Limite: {limit or 'todos'}")
        self.set_status(key, "En curso...", self.ACCENT)
        self.bottom_status.configure(text=f"Ejecutando {key}...")

        # Show stop button
        w = self.scraper_widgets.get(key)
        if w:
            w["stop_btn"].pack(side=tk.LEFT, padx=(4, 0))

        def _run():
            cmd = [sys.executable, os.path.join(PROJECT_ROOT, script)]
            if limit > 0:
                cmd += ["--limit", str(limit)]
            # Level would be passed if scrapers support --level

            start = time.time()
            try:
                proc = subprocess.Popen(
                    cmd, cwd=PROJECT_ROOT,
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    bufsize=1
                )
                self.active_processes[key] = proc
                line_count = 0

                while proc.poll() is None:
                    line = proc.stdout.readline()
                    if line:
                        decoded = line.decode('utf-8', errors='replace').rstrip()
                        line_count += 1
                        # Show every 10th line or important ones
                        if line_count % 10 == 0 or any(w in decoded.lower() for w in ['error', 'guardado', 'total', 'completado', 'productos', 'scraping']):
                            self.root.after(0, lambda m=decoded: self.log(f"[{key}] {m}"))

                # Read remaining
                remaining = proc.stdout.read()
                if remaining:
                    for line in remaining.decode('utf-8', errors='replace').strip().split('\n')[-5:]:
                        self.root.after(0, lambda m=line: self.log(f"[{key}] {m}"))

                elapsed = int(time.time() - start)
                elapsed_str = f"{elapsed//60}m {elapsed%60}s" if elapsed >= 60 else f"{elapsed}s"

                if proc.returncode == 0:
                    self.root.after(0, lambda: self.set_status(key, "Completado", self.GREEN))
                    self.root.after(0, lambda: self.log(f"✓ {key} completado en {elapsed_str}", "success"))
                else:
                    self.root.after(0, lambda: self.set_status(key, "Error", self.RED))
                    self.root.after(0, lambda: self.log(f"✗ {key} fallo (code {proc.returncode}) en {elapsed_str}", "error"))

            except Exception as e:
                self.root.after(0, lambda: self.set_status(key, "Error", self.RED))
                self.root.after(0, lambda: self.log(f"✗ {key} excepcion: {e}", "error"))
            finally:
                self.active_processes.pop(key, None)
                self.root.after(0, lambda: self._hide_stop(key))
                self.root.after(0, lambda: self.bottom_status.configure(text="Listo"))
                self.root.after(1000, self.refresh_counts)

        threading.Thread(target=_run, daemon=True).start()

    def _hide_stop(self, key):
        w = self.scraper_widgets.get(key)
        if w:
            w["stop_btn"].pack_forget()

    def stop_scraper(self, key):
        proc = self.active_processes.get(key)
        if proc and proc.poll() is None:
            proc.kill()
            self.active_processes.pop(key, None)
            self.set_status(key, "Detenido", self.RED)
            self.log(f"⏹ {key} detenido manualmente", "error")
            self._hide_stop(key)

    def run_all_scrapers(self):
        """Run all scrapers sequentially."""
        if self.active_processes:
            messagebox.showwarning("En curso", "Ya hay scrapers ejecutandose")
            return

        limit_str = tk.simpledialog.askstring(
            "Ejecutar todos",
            "Productos por farmacia (0 = todos):",
            initialvalue="50"
        )
        if limit_str is None:
            return

        limit = int(limit_str) if limit_str.isdigit() else 0

        def _run_all():
            for scraper in SCRAPERS:
                if scraper["key"] not in self.active_processes:
                    levels = scraper["levels"]
                    best_level = min(levels.keys())  # Lowest number = fastest method
                    script_path = levels[best_level]
                    self.root.after(0, lambda k=scraper["key"], s=script_path, l=best_level:
                                   self.run_scraper(k, s, l))
                    # Wait for it to finish
                    time.sleep(2)
                    while scraper["key"] in self.active_processes:
                        time.sleep(1)
                    time.sleep(2)

        threading.Thread(target=_run_all, daemon=True).start()

    def clear_logs(self):
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", tk.END)
        self.log_text.configure(state="disabled")


def main():
    # Need this for simpledialog
    import tkinter.simpledialog

    root = tk.Tk()

    # Set icon if available
    try:
        root.iconbitmap(os.path.join(PROJECT_ROOT, "static", "favicon.ico"))
    except:
        pass

    # Configure tag colors for log
    app = ScraperApp(root)
    app.log_text.tag_configure("error", foreground="#ff6b6b")
    app.log_text.tag_configure("success", foreground="#80ffb4")

    app.log("FarmaSearch Scraper Manager iniciado")
    app.log(f"Directorio: {PROJECT_ROOT}")
    app.log(f"Python: {sys.executable}")
    if HAS_DB:
        app.log(f"Base de datos: Supabase conectada")
    else:
        app.log("Base de datos: No disponible - configura .env", "error")

    root.mainloop()


if __name__ == "__main__":
    main()
