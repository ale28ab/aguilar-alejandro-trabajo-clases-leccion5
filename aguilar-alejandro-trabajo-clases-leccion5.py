<<<<<<< HEAD
import argparse
import concurrent.futures
import json
import logging
import os
import random
import threading
import time
from pathlib import Path
from typing import List, Dict, Any

import requests
from tqdm import tqdm

# ------ Configuración de logging ------
LOG_DIR = Path("results")
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "summary.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("app")

# ----- Reintentos con backoff -----

def http_get_with_retries(url: str, timeout: float = 10.0, max_retries: int = 3) -> Dict[str, Any]:
    """Hace GET con timeout y reintentos exponenciales con jitter.

    Retorna dict con keys: url, ok, status_code, text (truncado), error, attempts, elapsed
    """
    attempt = 0
    backoff = 1.0
    start = time.time()
    while True:
        attempt += 1
        try:
            resp = requests.get(url, timeout=timeout)
            elapsed = time.time() - start
            return {
                "url": url,
                "ok": resp.ok,
                "status_code": resp.status_code,
                "text": resp.text[:1000],  # truncar para evitar recuerdos enormes
                "error": None,
                "attempts": attempt,
                "elapsed": elapsed,
            }
        except (requests.RequestException, Exception) as exc:
            logger.debug(f"Error en {url} (intento {attempt}): {exc}")
            if attempt >= max_retries:
                elapsed = time.time() - start
                return {
                    "url": url,
                    "ok": False,
                    "status_code": None,
                    "text": None,
                    "error": str(exc),
                    "attempts": attempt,
                    "elapsed": elapsed,
                }
            # backoff exponencial con jitter
            sleep_time = backoff + random.uniform(0, 0.5)
            logger.info(f"Reintentando {url} en {sleep_time:.1f}s (intento {attempt+1}/{max_retries})")
            time.sleep(sleep_time)
            backoff *= 2


# ------ Lectura de archivos -------

def read_file_with_metadata(path: Path) -> Dict[str, Any]:
    """Lee archivo y retorna metadata + primeras 1000 chars.
    Simula I/O-bound si el archivo es grande.
    """
    start = time.time()
    try:
        text = path.read_text(encoding="utf-8")
        elapsed = time.time() - start
        return {
            "path": str(path),
            "ok": True,
            "size": len(text),
            "preview": text[:1000],
            "error": None,
            "elapsed": elapsed,
        }
    except Exception as exc:
        elapsed = time.time() - start
        return {"path": str(path), "ok": False, "size": 0, "preview": None, "error": str(exc), "elapsed": elapsed}


# ----- Utilidades de concurrencia -----

def compute_default_max_workers(requested: int, total_tasks: int) -> int:
    """Decisión razonable para max_workers en tareas I/O-bound.

    Razonamiento:
    - Para operaciones I/O-bound conviene usar más hilos que núcleos.
    - Limitar a un valor razonable para evitar sobrecarga. Aquí: min(32, cpu_count*5, total_tasks)
    """
    cpu = os.cpu_count() or 1
    default = min(32, cpu * 5, max(1, total_tasks))
    if requested and requested > 0:
        return requested
    return default


# ---- Flujo principal -----

def main(urls: List[str], files: List[Path], max_workers: int | None):
    tasks = []
    total_tasks = len(urls) + len(files)
    workers = compute_default_max_workers(max_workers or 0, total_tasks)
    logger.info(f"Total tasks: {total_tasks} (URLs: {len(urls)} files: {len(files)})")
    logger.info(f"Usando max_workers={workers} (decisión documentada en README)")

    results = []
    lock = threading.Lock()

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as exe:
        future_to_task = {}

        # Programar descargas HTTP
        for url in urls:
            fut = exe.submit(http_get_with_retries, url, 10.0, 3)
            future_to_task[fut] = ("http", url)

        # Programar lecturas de archivos
        for f in files:
            fut = exe.submit(read_file_with_metadata, f)
            future_to_task[fut] = ("file", str(f))

        # Procesar a medida que estén listos
        for fut in tqdm(concurrent.futures.as_completed(future_to_task), total=total_tasks):
            task_type, identifier = future_to_task[fut]
            try:
                res = fut.result()
            except Exception as exc:
                logger.exception(f"Error ejecutando tarea {identifier}: {exc}")
                res = {"ok": False, "error": str(exc), "task": identifier}

            # Manejo común: registrar y mostrar resumen
            with lock:
                results.append({"task_type": task_type, "id": identifier, "result": res})

            if task_type == "http":
                status = res.get("status_code")
                ok = res.get("ok")
                logger.info(f"[HTTP] {identifier} -> ok={ok} status={status} attempts={res.get('attempts')}")
            else:
                ok = res.get("ok")
                size = res.get("size")
                logger.info(f"[FILE] {identifier} -> ok={ok} size={size} elapsed={res.get('elapsed'):.3f}s")

    # Guardar resultados
    out_path = LOG_DIR / "results.json"
    out_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"Resultados guardados en: {out_path}")


# ---- CLI y carga de inputs ----

def load_urls_from_file(path: Path) -> List[str]:
    if not path.exists():
        return []
    lines = [l.strip() for l in path.read_text(encoding="utf-8").splitlines() if l.strip()]
    return lines


def load_files_from_dir(path: Path) -> List[Path]:
    p = Path(path)
    if not p.exists():
        return []
    return [f for f in p.iterdir() if f.is_file()]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--urls-file", type=str, default="data/urls.txt", help="Archivo con URLs, una por línea")
    parser.add_argument("--files-dir", type=str, default="data/files", help="Directorio con archivos para leer")
    parser.add_argument("--workers", type=int, default=0, help="max_workers para ThreadPoolExecutor (0 => auto)")
    args = parser.parse_args()

    urls = load_urls_from_file(Path(args.urls_file))
    files = load_files_from_dir(Path(args.files_dir))

    if not urls and not files:
        logger.warning("No hay tareas (ni URLs ni archivos). Crea data/urls.txt o data/files/ con contenido de prueba.")

    main(urls, files, args.workers)
=======
import argparse
import concurrent.futures
import json
import logging
import os
import random
import threading
import time
from pathlib import Path
from typing import List, Dict, Any

import requests
from tqdm import tqdm

# ------ Configuración de logging ------
LOG_DIR = Path("results")
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "summary.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("app")

# ----- Reintentos con backoff -----

def http_get_with_retries(url: str, timeout: float = 10.0, max_retries: int = 3) -> Dict[str, Any]:
    """Hace GET con timeout y reintentos exponenciales con jitter.

    Retorna dict con keys: url, ok, status_code, text (truncado), error, attempts, elapsed
    """
    attempt = 0
    backoff = 1.0
    start = time.time()
    while True:
        attempt += 1
        try:
            resp = requests.get(url, timeout=timeout)
            elapsed = time.time() - start
            return {
                "url": url,
                "ok": resp.ok,
                "status_code": resp.status_code,
                "text": resp.text[:1000],  # truncar para evitar recuerdos enormes
                "error": None,
                "attempts": attempt,
                "elapsed": elapsed,
            }
        except (requests.RequestException, Exception) as exc:
            logger.debug(f"Error en {url} (intento {attempt}): {exc}")
            if attempt >= max_retries:
                elapsed = time.time() - start
                return {
                    "url": url,
                    "ok": False,
                    "status_code": None,
                    "text": None,
                    "error": str(exc),
                    "attempts": attempt,
                    "elapsed": elapsed,
                }
            # backoff exponencial con jitter
            sleep_time = backoff + random.uniform(0, 0.5)
            logger.info(f"Reintentando {url} en {sleep_time:.1f}s (intento {attempt+1}/{max_retries})")
            time.sleep(sleep_time)
            backoff *= 2


# ------ Lectura de archivos -------

def read_file_with_metadata(path: Path) -> Dict[str, Any]:
    """Lee archivo y retorna metadata + primeras 1000 chars.
    Simula I/O-bound si el archivo es grande.
    """
    start = time.time()
    try:
        text = path.read_text(encoding="utf-8")
        elapsed = time.time() - start
        return {
            "path": str(path),
            "ok": True,
            "size": len(text),
            "preview": text[:1000],
            "error": None,
            "elapsed": elapsed,
        }
    except Exception as exc:
        elapsed = time.time() - start
        return {"path": str(path), "ok": False, "size": 0, "preview": None, "error": str(exc), "elapsed": elapsed}


# ----- Utilidades de concurrencia -----

def compute_default_max_workers(requested: int, total_tasks: int) -> int:
    """Decisión razonable para max_workers en tareas I/O-bound.

    Razonamiento:
    - Para operaciones I/O-bound conviene usar más hilos que núcleos.
    - Limitar a un valor razonable para evitar sobrecarga. Aquí: min(32, cpu_count*5, total_tasks)
    """
    cpu = os.cpu_count() or 1
    default = min(32, cpu * 5, max(1, total_tasks))
    if requested and requested > 0:
        return requested
    return default


# ---- Flujo principal -----

def main(urls: List[str], files: List[Path], max_workers: int | None):
    tasks = []
    total_tasks = len(urls) + len(files)
    workers = compute_default_max_workers(max_workers or 0, total_tasks)
    logger.info(f"Total tasks: {total_tasks} (URLs: {len(urls)} files: {len(files)})")
    logger.info(f"Usando max_workers={workers} (decisión documentada en README)")

    results = []
    lock = threading.Lock()

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as exe:
        future_to_task = {}

        # Programar descargas HTTP
        for url in urls:
            fut = exe.submit(http_get_with_retries, url, 10.0, 3)
            future_to_task[fut] = ("http", url)

        # Programar lecturas de archivos
        for f in files:
            fut = exe.submit(read_file_with_metadata, f)
            future_to_task[fut] = ("file", str(f))

        # Procesar a medida que estén listos
        for fut in tqdm(concurrent.futures.as_completed(future_to_task), total=total_tasks):
            task_type, identifier = future_to_task[fut]
            try:
                res = fut.result()
            except Exception as exc:
                logger.exception(f"Error ejecutando tarea {identifier}: {exc}")
                res = {"ok": False, "error": str(exc), "task": identifier}

            # Manejo común: registrar y mostrar resumen
            with lock:
                results.append({"task_type": task_type, "id": identifier, "result": res})

            if task_type == "http":
                status = res.get("status_code")
                ok = res.get("ok")
                logger.info(f"[HTTP] {identifier} -> ok={ok} status={status} attempts={res.get('attempts')}")
            else:
                ok = res.get("ok")
                size = res.get("size")
                logger.info(f"[FILE] {identifier} -> ok={ok} size={size} elapsed={res.get('elapsed'):.3f}s")

    # Guardar resultados
    out_path = LOG_DIR / "results.json"
    out_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"Resultados guardados en: {out_path}")


# ---- CLI y carga de inputs ----

def load_urls_from_file(path: Path) -> List[str]:
    if not path.exists():
        return []
    lines = [l.strip() for l in path.read_text(encoding="utf-8").splitlines() if l.strip()]
    return lines


def load_files_from_dir(path: Path) -> List[Path]:
    p = Path(path)
    if not p.exists():
        return []
    return [f for f in p.iterdir() if f.is_file()]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--urls-file", type=str, default="data/urls.txt", help="Archivo con URLs, una por línea")
    parser.add_argument("--files-dir", type=str, default="data/files", help="Directorio con archivos para leer")
    parser.add_argument("--workers", type=int, default=0, help="max_workers para ThreadPoolExecutor (0 => auto)")
    args = parser.parse_args()

    urls = load_urls_from_file(Path(args.urls_file))
    files = load_files_from_dir(Path(args.files_dir))

    if not urls and not files:
        logger.warning("No hay tareas (ni URLs ni archivos). Crea data/urls.txt o data/files/ con contenido de prueba.")

    main(urls, files, args.workers)
>>>>>>> 3fb275df1802f21cf7b51da69a32e14571088004
