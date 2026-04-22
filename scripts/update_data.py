import csv
import io
import json
import os
import re
import urllib.request
import zipfile
from datetime import datetime
from datetime import timezone
from pathlib import Path


DATASET_ID = os.getenv("DATASET_ID", "sepa-precios")
CKAN_BASE_URL = os.getenv("CKAN_BASE_URL", "https://datos.produccion.gob.ar")
CKAN_SHOW_URL = f"{CKAN_BASE_URL}/api/3/action/package_show?id={DATASET_ID}"

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
SNAPSHOTS_DIR = DATA_DIR / "snapshots"

PROVINCES = {
    p.strip().upper()
    for p in os.getenv(
        "PROVINCES", "AR-C,AR-B,CIUDAD AUTONOMA DE BUENOS AIRES,BUENOS AIRES"
    ).split(",")
    if p.strip()
}
MAX_RECORDS_PER_DAY = int(os.getenv("MAX_RECORDS_PER_DAY", "120000"))
MAX_SNAPSHOTS = int(os.getenv("MAX_SNAPSHOTS", "45"))
TIMEOUT_SECONDS = int(os.getenv("HTTP_TIMEOUT", "50"))
MAX_INNER_ZIPS = int(os.getenv("MAX_INNER_ZIPS", "40"))
RESET_DATA = os.getenv("RESET_DATA", "false").lower() in {"1", "true", "yes"}
MAX_RESOURCES_PER_RUN = int(os.getenv("MAX_RESOURCES_PER_RUN", "3"))
MAX_PRODUCTS = int(os.getenv("MAX_PRODUCTS", "3000"))
MAX_STORES = int(os.getenv("MAX_STORES", "200"))

PROVINCE_EQUIVALENCE = {
    "AR-B": "BUENOS AIRES",
    "AR-C": "CIUDAD AUTONOMA DE BUENOS AIRES",
}


def fetch_json(url):
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; changuito-bot/1.0)"},
    )
    with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        return json.loads(response.read().decode("utf-8"))


def download_bytes(url):
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; changuito-bot/1.0)"},
    )
    with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        return response.read()


def normalize_key(value):
    return re.sub(r"[^a-z0-9]", "", value.lower())


def pick_column(columns, candidates):
    normalized = {normalize_key(col): col for col in columns}
    for candidate in candidates:
        if candidate in normalized:
            return normalized[candidate]
    return None


def pick_recent_resources(resources, max_count):
    csv_zip = [r for r in resources if str(r.get("format", "")).upper() == "ZIP"]
    if not csv_zip:
        raise RuntimeError("No se encontraron recursos ZIP en el dataset.")

    def parse_date(resource):
        value = resource.get("last_modified") or resource.get("created") or ""
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return datetime.min

    csv_zip.sort(key=parse_date, reverse=True)
    return csv_zip[:max_count]


def open_csv_dicts(raw_bytes, delimiter="|"):
    text = raw_bytes.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    for row in reader:
        if row:
            yield row


def iter_rows_from_zip(zip_bytes):
    with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as outer_zip:
        inner_members = [name for name in outer_zip.namelist() if name.lower().endswith(".zip")]
        if not inner_members:
            raise RuntimeError("El ZIP externo no contiene archivos ZIP internos.")

        for member_name in inner_members[:MAX_INNER_ZIPS]:
            inner_bytes = outer_zip.read(member_name)
            with zipfile.ZipFile(io.BytesIO(inner_bytes), "r") as inner_zip:
                files = {name.lower(): name for name in inner_zip.namelist()}
                if not {"comercio.csv", "sucursales.csv", "productos.csv"}.issubset(files.keys()):
                    continue

                comercio_rows = open_csv_dicts(inner_zip.read(files["comercio.csv"]))
                sucursales_rows = open_csv_dicts(inner_zip.read(files["sucursales.csv"]))
                productos_rows = open_csv_dicts(inner_zip.read(files["productos.csv"]))

                comercio_map = {}
                for row in comercio_rows:
                    key = (text(row.get("id_comercio")), text(row.get("id_bandera")))
                    if not key[0] or not key[1]:
                        continue
                    comercio_map[key] = {
                        "chain": text(row.get("comercio_bandera_nombre") or row.get("comercio_razon_social")),
                    }

                sucursal_map = {}
                for row in sucursales_rows:
                    key = (
                        text(row.get("id_comercio")),
                        text(row.get("id_bandera")),
                        text(row.get("id_sucursal")),
                    )
                    if not key[0] or not key[1] or not key[2]:
                        continue
                    sucursal_map[key] = {
                        "name": text(row.get("sucursales_nombre")),
                        "address": f"{text(row.get('sucursales_calle'))} {text(row.get('sucursales_numero'))}".strip(),
                        "province": text(row.get("sucursales_provincia")).upper(),
                        "lat": safe_float(row.get("sucursales_latitud")),
                        "lon": safe_float(row.get("sucursales_longitud")),
                    }

                for row in productos_rows:
                    id_comercio = text(row.get("id_comercio"))
                    id_bandera = text(row.get("id_bandera"))
                    id_sucursal = text(row.get("id_sucursal"))
                    if not id_comercio or not id_bandera or not id_sucursal:
                        continue

                    suc_key = (id_comercio, id_bandera, id_sucursal)
                    com_key = (id_comercio, id_bandera)
                    sucursal = sucursal_map.get(suc_key, {})
                    comercio = comercio_map.get(com_key, {})

                    candidate_id = text(row.get("id_producto"))
                    fallback_ean = text(row.get("productos_ean"))
                    product_id = candidate_id if len(candidate_id) >= 8 else fallback_ean

                    yield {
                        "product_id": product_id,
                        "product_name": text(row.get("productos_descripcion")),
                        "brand": text(row.get("productos_marca")),
                        "store_id": f"{id_comercio}-{id_bandera}-{id_sucursal}",
                        "store_name": sucursal.get("name") or f"Sucursal {id_sucursal}",
                        "chain": comercio.get("chain") or "Cadena sin nombre",
                        "address": sucursal.get("address") or "",
                        "province": sucursal.get("province") or "",
                        "lat": sucursal.get("lat"),
                        "lon": sucursal.get("lon"),
                        "price": safe_float(row.get("productos_precio_lista")),
                    }


def safe_float(value):
    if value is None:
        return None
    value = str(value).strip()
    if not value:
        return None
    if "," in value and "." in value:
        value = value.replace(".", "").replace(",", ".")
    elif "," in value:
        value = value.replace(",", ".")
    try:
        return float(value)
    except ValueError:
        return None


def text(value):
    return (value or "").strip()


def province_allowed(raw_province):
    if not PROVINCES:
        return True
    province = text(raw_province).upper()
    if province in PROVINCES:
        return True
    equivalent = PROVINCE_EQUIVALENCE.get(province)
    return bool(equivalent and equivalent in PROVINCES)


def build_outputs(rows, source_date):
    products = {}
    stores = {}
    history_accumulator = {}
    processed_rows = 0

    for row in rows:
        province = (row.get("province") or "").strip().upper()
        if not province_allowed(province):
            continue

        product_id = (row.get("product_id") or "").strip()
        product_name = (row.get("product_name") or "").strip()
        brand = (row.get("brand") or "").strip()

        store_id = (row.get("store_id") or "").strip()
        store_name = (row.get("store_name") or "").strip()
        chain = (row.get("chain") or "").strip()
        address = (row.get("address") or "").strip()

        price = row.get("price")
        lat = row.get("lat")
        lon = row.get("lon")

        if not product_id or not store_id or price is None:
            continue

        if product_id not in products and len(products) >= MAX_PRODUCTS:
            continue
        if store_id not in stores and len(stores) >= MAX_STORES:
            continue

        products[product_id] = {
            "id": product_id,
            "name": product_name or "Producto sin nombre",
            "brand": brand,
        }
        stores[store_id] = {
            "id": store_id,
            "name": store_name or "Sucursal sin nombre",
            "chain": chain or "Cadena sin nombre",
            "address": address,
            "province": province,
            "lat": lat,
            "lon": lon,
        }

        key_all = f"{product_id}::all::{source_date}"
        history_accumulator.setdefault(key_all, []).append(price)
        processed_rows += 1

        if processed_rows >= MAX_RECORDS_PER_DAY:
            break

    history_bucket = {}
    for key, prices in history_accumulator.items():
        product_id, store_id, date = key.split("::", 2)
        series_key = f"{product_id}::{store_id}"
        avg_price = round(sum(prices) / len(prices), 2)
        history_bucket.setdefault(series_key, []).append({"date": date, "price": avg_price})

    return products, stores, history_bucket


def read_existing_json(path, default):
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def merge_history(existing, current):
    merged = existing.copy()
    for key, points in current.items():
        existing_points = merged.get(key, [])
        seen = {(p["date"], p["price"]) for p in existing_points}
        for point in points:
            marker = (point["date"], point["price"])
            if marker not in seen:
                existing_points.append(point)
                seen.add(marker)
        existing_points.sort(key=lambda p: p["date"])
        merged[key] = existing_points[-MAX_SNAPSHOTS:]
    return merged


def ensure_dirs():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)


def prune_old_snapshots():
    snapshot_files = sorted(SNAPSHOTS_DIR.glob("*.json"))
    while len(snapshot_files) > MAX_SNAPSHOTS:
        snapshot_files[0].unlink(missing_ok=True)
        snapshot_files.pop(0)


def main():
    ensure_dirs()

    ckan = fetch_json(CKAN_SHOW_URL)
    if not ckan.get("success"):
        raise RuntimeError("No se pudo consultar CKAN")

    result = ckan["result"]
    resources = pick_recent_resources(result["resources"], MAX_RESOURCES_PER_RUN)

    all_products = {}
    all_stores = {}
    all_histories = {}

    for resource in resources:
        source_url = resource["url"]
        source_date = (resource.get("last_modified") or datetime.now(timezone.utc).isoformat())[:10]
        zip_bytes = download_bytes(source_url)
        rows = iter_rows_from_zip(zip_bytes)
        products, stores, new_histories = build_outputs(rows, source_date)
        all_products.update(products)
        all_stores.update(stores)
        all_histories = merge_history(all_histories, new_histories)

    products_path = DATA_DIR / "products.json"
    stores_path = DATA_DIR / "stores.json"
    histories_path = DATA_DIR / "histories.json"
    meta_path = DATA_DIR / "meta.json"

    if RESET_DATA:
        existing_products = {}
        existing_stores = {}
        existing_histories = {}
    else:
        existing_products = {p["id"]: p for p in read_existing_json(products_path, [])}
        existing_stores = {s["id"]: s for s in read_existing_json(stores_path, [])}
        existing_histories = read_existing_json(histories_path, {})

    existing_products.update(all_products)
    existing_stores.update(all_stores)
    merged_histories = merge_history(existing_histories, all_histories)

    sorted_products = sorted(existing_products.values(), key=lambda p: (p.get("name") or "", p["id"]))
    sorted_stores = sorted(existing_stores.values(), key=lambda s: (s.get("chain") or "", s["id"]))

    with products_path.open("w", encoding="utf-8") as handle:
        json.dump(sorted_products, handle, ensure_ascii=False)

    with stores_path.open("w", encoding="utf-8") as handle:
        json.dump(sorted_stores, handle, ensure_ascii=False)

    with histories_path.open("w", encoding="utf-8") as handle:
        json.dump(merged_histories, handle, ensure_ascii=False)

    snapshot_payload = {
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "resources_processed": [
            {
                "id": resource.get("id"),
                "name": resource.get("name"),
                "date": (resource.get("last_modified") or "")[:10],
                "url": resource.get("url"),
            }
            for resource in resources
        ],
        "products_added": len(all_products),
        "stores_added": len(all_stores),
        "history_keys": len(all_histories),
    }

    with (SNAPSHOTS_DIR / f"{snapshot_payload['date']}.json").open("w", encoding="utf-8") as handle:
        json.dump(snapshot_payload, handle, ensure_ascii=False, indent=2)

    meta_payload = {
        "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "dataset": result.get("title"),
        "dataset_id": DATASET_ID,
        "ckan_base_url": CKAN_BASE_URL,
        "sources": [
            {
                "resource_id": resource.get("id"),
                "resource_name": resource.get("name"),
                "resource_url": resource.get("url"),
                "source_date": (resource.get("last_modified") or "")[:10],
            }
            for resource in resources
        ],
        "filters": {
            "provinces": sorted(list(PROVINCES)),
            "max_records_per_day": MAX_RECORDS_PER_DAY,
            "max_snapshots": MAX_SNAPSHOTS,
            "max_resources_per_run": MAX_RESOURCES_PER_RUN,
            "max_products": MAX_PRODUCTS,
            "max_stores": MAX_STORES,
        },
        "totals": {
            "products": len(sorted_products),
            "stores": len(sorted_stores),
            "history_keys": len(merged_histories),
        },
    }

    with meta_path.open("w", encoding="utf-8") as handle:
        json.dump(meta_payload, handle, ensure_ascii=False, indent=2)

    prune_old_snapshots()
    print("OK: datos actualizados")


if __name__ == "__main__":
    main()
