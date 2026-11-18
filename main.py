"""ATLAS OSINT PRO v0.4.2 — passive, source-linked investigation gateway.
Developer: Suat DOĞU. Public data only. No intrusive scanning.
Run: uvicorn main:app --host 0.0.0.0 --port 10000
"""
from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import json
import logging
import os
import re
import unicodedata
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import httpx
from fastapi import FastAPI, Header, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

VERSION = "0.4.2"
ROOT = Path(__file__).resolve().parent
APP_TOKEN = os.environ.get("OSINT_APP_TOKEN", "").strip()
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "").strip()
OPENALEX_API_KEY = os.environ.get("OPENALEX_API_KEY", "").strip()
# Wikimedia asks for an identifiable application and contact URL. A 403 is not
# automatically recoverable; never proxy-hop around deliberate blocks.
WIKIMEDIA_USER_AGENT = "AtlasOSINTPro/0.4.1 (https://github.com/suatdogu4-cpu/atlas-osint-pro; passive public-data research)"
LOG = logging.getLogger("atlas-osint")
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="ATLAS OSINT PRO", version=VERSION, docs_url=None, redoc_url=None)

# Short-lived, in-memory cache: no subject data written to disk by backend.
_cache: dict[tuple[str, str], tuple[float, list[dict]]] = {}
_source_health: dict[str, dict] = {}
_rate_windows: dict[str, list[float]] = {}
RATE_LIMIT = 18  # per IP / minute, in addition to APP_TOKEN

CACHE_TTL = 240
MAX_CACHE_ENTRIES = 128
REQUEST_TIMEOUT = httpx.Timeout(14.0, connect=5.0)

KINDS = {
    "domain": "Alan adı / DNS / RDAP / CT",
    "ip": "IP / kayıt bilgileri",
    "github_user": "GitHub açık kullanıcı profili",
    "github_repos": "GitHub açık depolar",
    "bluesky": "Bluesky açık gönderiler",
    "news": "GDELT haber araması",
    "cve": "NVD güvenlik açığı",
    "bitcoin": "Bitcoin açık zincir adresi",
    "wayback": "Wayback Machine / web arşivi",
    "openalex": "OpenAlex / akademik araştırma",
    "kev": "CISA KEV / istismar edildiği bilinen güvenlik açıkları",
    "person_public": "Ad-soyad / açık kamusal kayıt adayları",
    "social_profile": "Kullanıcı tarafından belirtilen açık sosyal profil",
    "web_trace": "Alan adı dijital izi / DNS, RDAP, CT, arşiv",
    "username_public": "Kullanıcı adı / iki ayrı açık profil adayı",
    "company_public": "Şirket/kurum / kamusal kayıt adayları",
}

class SearchRequest(BaseModel):
    kind: str
    query: str = Field(min_length=1, max_length=160)


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def require_token(supplied: str | None):
    if not APP_TOKEN:
        raise HTTPException(status_code=503, detail="OSINT_APP_TOKEN tanımlanmamış; sorgular güvenlik amacıyla kapalı.")
    if (supplied or "") != APP_TOKEN:
        raise HTTPException(status_code=401, detail="Uygulama anahtarı hatalı.")


def validate(kind: str, raw: str) -> str:
    q = raw.strip()
    if kind not in KINDS:
        raise ValueError("Bilinmeyen sorgu kategorisi")
    if not q or len(q) > 160 or any(ord(c) < 32 for c in q):
        raise ValueError("Sorgu 1-160 karakter olmalıdır")
    if kind == "domain":
        try:
            q = q.lower().rstrip(".").encode("idna").decode("ascii")
        except UnicodeError as exc:
            raise ValueError("Alan adı geçersiz") from exc
        if len(q) > 253 or not re.fullmatch(r"(?=.{3,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}", q):
            raise ValueError("example.com biçiminde geçerli bir alan adı girin")
        if q.endswith((".local", ".internal", ".test", ".invalid")):
            raise ValueError("Yalnız kamusal alan adlarını araştırın")
    elif kind in ("wayback", "web_trace"):
        q = validate("domain", q)
    elif kind == "ip":
        try:
            ip = ipaddress.ip_address(q)
        except ValueError as exc:
            raise ValueError("Geçerli IPv4 veya IPv6 adresi girin") from exc
        if not ip.is_global:
            raise ValueError("Yalnız genel internete açık IP adresleri sorgulanabilir")
        q = str(ip)
    elif kind == "username_public":
        if not re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?", q):
            raise ValueError("Kullanıcı adını girin (yalnız ASCII harf, rakam ve tire; en fazla 39 karakter).")
    elif kind == "company_public":
        if not (3 <= len(q) <= 100) or not all(c.isalnum() or c in " -.,&'’()" for c in q):
            raise ValueError("Kamusal kurum/şirket adı girin (3–100 karakter).")
    elif kind == "person_public":
        if len(q) > 80 or not (2 <= len(q.split()) <= 5) or not all(
            c.isalpha() or c in " -'.’" for c in q
        ):
            raise ValueError("Yalnız ad ve soyad girin (2-5 kelime, en fazla 80 karakter).")
    elif kind == "social_profile":
        prefix, sep, handle = q.partition(":")
        prefix = prefix.lower().strip()
        handle = handle.strip().lstrip("@")
        if not sep or prefix not in ("github", "bluesky"):
            raise ValueError("github:kullanici veya bluesky:handle.bsky.social girin")
        if prefix == "github":
            validate("github_user", handle)
        elif not re.fullmatch(r"[a-zA-Z0-9](?:[a-zA-Z0-9.\-]{1,245})[a-zA-Z0-9]", handle) or "." not in handle:
            raise ValueError("Geçerli Bluesky handle girin (ör. bsky.app kullanıcı adı).")
        q = prefix + ":" + handle.lower()
    elif kind == "github_user":
        if not re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?", q):
            raise ValueError("Geçerli GitHub kullanıcı adı girin")
    elif kind == "cve":
        q = q.upper()
        if not re.fullmatch(r"CVE-\d{4}-\d{4,}", q):
            raise ValueError("CVE-2024-12345 biçiminde girin")
    elif kind == "bitcoin":
        if not re.fullmatch(r"(?:bc1[ac-hj-np-z02-9]{11,87}|[13][a-km-zA-HJ-NP-Z1-9]{25,34})", q, re.I):
            raise ValueError("Geçerli biçimde bir Bitcoin ana ağ adresi girin")
    elif kind in {"github_repos", "news", "bluesky", "openalex", "kev"} and len(q) < 2:
        raise ValueError("En az iki karakter girin")
    return q


def check_rate_limit(peer: str):
    t = time.monotonic()
    # Old buckets are evicted to avoid unbounded memory usage.
    if len(_rate_windows) > 1024:
        _rate_windows.clear()
    recent = [x for x in _rate_windows.get(peer, []) if t - x < 60]
    if len(recent) >= RATE_LIMIT:
        raise HTTPException(status_code=429, detail="Dakikalık sorgu sınırına ulaşıldı.")
    recent.append(t)
    _rate_windows[peer] = recent


def item(source, title, description, url="", fields=None):
    return {
        "source": source,
        "title": str(title)[:180],
        "description": str(description or "")[:1200],
        "url": url,
        "fields": fields or {},
        "checked_at": now(),
        "status": "ok", "evidence_level": "source_record",
    }



def attach_evidence_metadata(record: dict, kind: str, query: str) -> dict:
    """Source-record SHA-256 digest: integrity identifier, NOT proof of truth/authenticity."""
    result = dict(record)
    result["query_kind"] = kind
    result["query"] = query
    fields = ("query_kind", "query", "source", "title", "description", "url",
              "fields", "checked_at", "status")
    canonical = json.dumps({k: result.get(k) for k in fields},
                           sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)
    result["sha256"] = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return result


async def get_json(client: httpx.AsyncClient, url: str, params=None, headers=None, max_bytes=3_000_000):
    # Outbound URLs are static provider endpoints; never accept a user-supplied URL.
    resp = await client.get(url, params=params, headers=headers)
    resp.raise_for_status()
    if len(resp.content) > max_bytes:
        raise ValueError("Kaynak yanıtı izin verilen boyutu aşıyor")
    return resp.json()


async def domain_dns(client, domain):
    records = []
    succeeded = 0
    for kind in ("A", "AAAA", "MX", "NS", "TXT"):
        try:
            data = await get_json(client, "https://dns.google/resolve", {"name": domain, "type": kind})
            answers = [x.get("data", "") for x in data.get("Answer", [])][:15]
            records.append(item("Google Public DNS", kind + " kayıtları", ", ".join(answers) if answers else "Kayıt bulunamadı", "https://dns.google/", {"record_type": kind, "answers": answers}))
            succeeded += 1
        except Exception as exc:
            failure = item("Google Public DNS", kind + " · kaynak hatası", safe_error(exc))
            failure["status"] = "error"
            failure["evidence_level"] = "error"
            records.append(failure)
    if succeeded == 0:
        raise ValueError("DNS kaynağına erişilemedi (beş kayıt türü de başarısız).")
    return records


async def domain_rdap(client, domain):
    data = await get_json(client, "https://rdap.org/domain/" + domain)
    events = [{"event": x.get("eventAction"), "date": x.get("eventDate")} for x in data.get("events", [])][:10]
    return [item("RDAP", "Alan adı kayıt özeti", data.get("ldhName", domain), "https://client.rdap.org/?object=" + quote(domain) + "&type=domain", {
        "status": data.get("status", []), "nameservers": [x.get("ldhName") for x in data.get("nameservers", [])][:20], "events": events,
        "handle": data.get("handle", ""),
    })]


async def domain_ct(client, domain):
    data = await get_json(client, "https://crt.sh/", {"q": "%." + domain, "output": "json"}, max_bytes=8_000_000)
    if not isinstance(data, list):
        raise ValueError("Sertifika kaynağı beklenmeyen veri döndürdü")
    names = set()
    for cert in data[:1500]:
        for name in cert.get("name_value", "").splitlines():
            name = name.strip().lower().removeprefix("*.")
            if name == domain or name.endswith("." + domain):
                names.add(name)
    values = sorted(names)[:100]
    return [item("crt.sh / Certificate Transparency", f"Sertifikada görünen alan adları · {len(names)}+", ", ".join(values[:25]) or "Kayıt bulunamadı", "https://crt.sh/?q=%25." + quote(domain), {"names": values, "truncated": len(names) > 100})]


async def lookup_ip(client, q):
    data = await get_json(client, "https://rdap.org/ip/" + quote(q))
    names = [e.get("name") for e in data.get("entities", []) if e.get("name")][:8]
    return [item("RDAP IP", "IP tahsis bilgisi", data.get("name", q), "https://client.rdap.org/?object=" + quote(q) + "&type=ip", {
        "handle": data.get("handle"), "startAddress": data.get("startAddress"), "endAddress": data.get("endAddress"),
        "country": data.get("country"), "entities": names, "status": data.get("status", [])
    })]


def gh_headers():
    h = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
    if GITHUB_TOKEN:
        h["Authorization"] = "Bearer " + GITHUB_TOKEN
    return h


async def lookup_github_user(client, q):
    data = await get_json(client, "https://api.github.com/users/" + quote(q), headers=gh_headers())
    return [item("GitHub API", "Açık geliştirici profili", data.get("bio", "") or data.get("name", "") or q, data.get("html_url", ""), {
        "login": data.get("login"), "name": data.get("name"), "public_repos": data.get("public_repos"),
        "followers": data.get("followers"), "created_at": data.get("created_at"), "updated_at": data.get("updated_at"),
        "avatar_url": data.get("avatar_url"), "identity_match": "not_performed"
    })]


def wikidata_headers():
    return {"User-Agent": WIKIMEDIA_USER_AGENT,
            "Api-User-Agent": WIKIMEDIA_USER_AGENT,
            "Accept": "application/json"}


async def wikidata_search(client, name, lang):
    # One official Action API request; if access is denied, expose the error.
    return await get_json(client, "https://www.wikidata.org/w/api.php", {
        "action": "wbsearchentities", "search": name, "language": lang,
        "uselang": lang, "type": "item", "limit": 8,
        "format": "json", "maxlag": 5
    }, headers=wikidata_headers())


async def lookup_public_wikidata(client, fullname):
    # Candidate entries only. A matching name does NOT establish a verified person.
    data = await wikidata_search(client, fullname, "tr")
    return [item("Wikidata", "Kamusal kayıt adayı · " + x.get("label", ""),
                 x.get("description", "") or "Açık ansiklopedik kayıt adayı",
                 x.get("concepturi", "") or "https://www.wikidata.org/wiki/" + quote(x.get("id", "")),
                 {"record_id": x.get("id"), "identity_match": "unverified_candidate", "query_is_name_only": True})
            for x in data.get("search", [])[:8] if x.get("id")]


async def lookup_public_academics(client, fullname):
    params = {"search": fullname, "per-page": 8}
    if OPENALEX_API_KEY:
        params["api_key"] = OPENALEX_API_KEY
    data = await get_json(client, "https://api.openalex.org/authors", params)
    out = []
    for x in data.get("results", [])[:8]:
        institutions = [a.get("institution", {}).get("display_name", "") for a in x.get("affiliations", [])[:3]]
        out.append(item("OpenAlex Authors", "Akademik kayıt adayı · " + x.get("display_name", ""),
                        "Kayıtta bulunan yayın sayısı: " + str(x.get("works_count", 0)), x.get("id", ""),
                        {"works_count": x.get("works_count"), "institutions": [a for a in institutions if a],
                         "identity_match": "unverified_candidate", "query_is_name_only": True}))
    return out


def _norm_public_name(value):
    """Only compare explicit author display names; never infer account ownership."""
    value = unicodedata.normalize("NFKD", str(value).replace("ı", "i"))
    return " ".join("".join(ch for ch in value if ch.isalnum() or ch.isspace()).casefold().split())


async def lookup_public_crossref(client, fullname):
    """Independent publication metadata search; exact public author names only.

    Crossref is NOT a registry of people. No result is not evidence of absence.
    """
    data = await get_json(client, "https://api.crossref.org/works", {
        "query.author": fullname, "rows": 12,
        "select": "DOI,title,author,published,container-title"
    })
    target = _norm_public_name(fullname)
    results = []
    for work in data.get("message", {}).get("items", [])[:12]:
        authors = work.get("author", [])
        matched = []
        for a in authors:
            display = (a.get("given", "") + " " + a.get("family", "")).strip()
            if _norm_public_name(display) == target:
                matched.append(display)
        if not matched:
            continue
        doi = str(work.get("DOI", ""))
        title = (work.get("title") or ["Kaynaklı yayın"])[0]
        journal = (work.get("container-title") or [""])[0]
        results.append(item("Crossref", "Yayın yazarı aday kaydı · " + matched[0],
                            title, "https://doi.org/" + quote(doi, safe="/") if doi else "",
                            {"journal": journal, "doi": doi, "identity_match": "unverified_candidate",
                             "query_is_name_only": True, "source_scope": "publication_metadata"}))
        if len(results) >= 5:
            break
    return results


async def lookup_social_profile(client, query):
    provider, handle = query.split(":", 1)
    if provider == "github":
        rows = await lookup_github_user(client, handle)
        for row in rows:
            row["title"] = "Belirtilen GitHub profili · " + handle
            row["fields"]["identity_match"] = "not_performed"
        return rows
    data = await get_json(client, "https://public.api.bsky.app/xrpc/app.bsky.actor.getProfile", {"actor": handle})
    profile_handle = data.get("handle", handle)
    return [item("Bluesky AppView", "Belirtilen Bluesky profili · @" + profile_handle,
                 data.get("description") or "Kamuya açık profil bilgileri",
                 "https://bsky.app/profile/" + quote(profile_handle, safe="."), {
                     "handle": profile_handle, "display_name": data.get("displayName"),
                     "avatar_url": data.get("avatar"), "followers": data.get("followersCount"),
                     "follows": data.get("followsCount"), "posts": data.get("postsCount"),
                     "identity_match": "not_performed"
                 })]


async def username_github_public(client, handle):
    # Exact public-handle candidate only; never infer account ownership across sites.
    try:
        rows = await lookup_github_user(client, handle)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            return []
        raise
    for row in rows:
        row["title"] = "Kullanıcı adı kaydı · GitHub @" + handle
        row["fields"]["identity_match"] = "unverified_handle_candidate"
    return rows


async def username_bluesky_public(client, handle):
    bsky_handle = handle + ".bsky.social"
    try:
        rows = await lookup_social_profile(client, "bluesky:" + bsky_handle)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            return []
        raise
    for row in rows:
        row["title"] = "Kullanıcı adı kaydı · Bluesky @" + bsky_handle
        row["fields"]["identity_match"] = "unverified_handle_candidate"
    return rows


async def lookup_company_wikidata(client, name):
    data = await wikidata_search(client, name, "en")
    return [item("Wikidata", "Kurumsal kayıt adayı · " + x.get("label", ""),
                 x.get("description", "") or "Kamusal kurum kaydı adayı",
                 x.get("concepturi", "") or "https://www.wikidata.org/wiki/" + quote(x.get("id", "")),
                 {"record_id":x.get("id"), "identity_match":"unverified_company_candidate"})
            for x in data.get("search", [])[:8] if x.get("id")]


async def lookup_company_openalex(client, name):
    params = {"search": name, "per-page": 8}
    if OPENALEX_API_KEY:
        params["api_key"] = OPENALEX_API_KEY
    data = await get_json(client, "https://api.openalex.org/institutions", params)
    return [item("OpenAlex Institutions", "Araştırma kurumu adayı · " + x.get("display_name", ""),
                 "Açık akademik kurum kaydı; şirket sicili değildir.", x.get("id", ""),
                 {"country_code":x.get("country_code"), "works_count":x.get("works_count"),
                  "identity_match":"unverified_company_candidate"})
            for x in data.get("results", [])[:8]]


async def lookup_github_repos(client, q):
    data = await get_json(client, "https://api.github.com/search/repositories", {"q": q, "sort": "updated", "per_page": 12}, headers=gh_headers())
    return [item("GitHub API", x.get("full_name", "Depo"), x.get("description") or "Açıklama yok", x.get("html_url", ""), {
        "language": x.get("language"), "stars": x.get("stargazers_count"), "updated_at": x.get("updated_at")
    }) for x in data.get("items", [])[:12]]


async def lookup_bluesky(client, q):
    params = {"q": q, "limit": 15, "sort": "latest"}
    try:
        data = await get_json(client, "https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts", params)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code not in (403, 429):
            raise
        # Second official AppView host; do not bypass sign-in / access controls.
        data = await get_json(client, "https://api.bsky.app/xrpc/app.bsky.feed.searchPosts", params)
    out = []
    for p in data.get("posts", [])[:15]:
        handle = p.get("author", {}).get("handle", "")
        rkey = (p.get("uri") or "").rsplit("/", 1)[-1]
        url = f"https://bsky.app/profile/{quote(handle, safe='.')}/post/{quote(rkey)}" if handle and rkey else ""
        out.append(item("Bluesky AppView", "@" + handle, p.get("record", {}).get("text", ""), url, {
            "created_at": p.get("record", {}).get("createdAt"), "likes": p.get("likeCount"), "reposts": p.get("repostCount")
        }))
    return out


async def lookup_news(client, q):
    data = await get_json(client, "https://api.gdeltproject.org/api/v2/doc/doc", {
        "query": q, "mode": "artlist", "format": "json", "maxrecords": 15, "timespan": "7d", "sort": "datedesc"
    })
    return [item("GDELT DOC 2.0", x.get("title", "Haber"), x.get("domain", "") + " · " + x.get("seendate", ""), x.get("url", ""), {
        "language": x.get("language", ""), "seendate": x.get("seendate", ""), "sourcecountry": x.get("sourcecountry", "")
    }) for x in data.get("articles", [])[:15]]


async def lookup_cve(client, q):
    data = await get_json(client, "https://services.nvd.nist.gov/rest/json/cves/2.0", {"cveId": q})
    result = []
    for v in data.get("vulnerabilities", [])[:1]:
        cve = v.get("cve", {})
        descs = cve.get("descriptions", [])
        desc = next((x.get("value", "") for x in descs if x.get("lang") == "en"), "Açıklama yok")
        result.append(item("NIST NVD", q, desc, "https://nvd.nist.gov/vuln/detail/" + q, {
            "published": cve.get("published"), "lastModified": cve.get("lastModified"), "vulnStatus": cve.get("vulnStatus")
        }))
    return result


async def lookup_bitcoin(client, q):
    data = await get_json(client, "https://mempool.space/api/address/" + quote(q))
    chain = data.get("chain_stats", {})
    pool = data.get("mempool_stats", {})
    return [item("mempool.space", "Bitcoin adres özeti", "Açık Bitcoin zincirindeki istatistikler (özel mülkiyet/kimlik çıkarımı değildir)", "https://mempool.space/address/" + quote(q), {
        "confirmed_txs": chain.get("tx_count", 0), "unconfirmed_txs": pool.get("tx_count", 0),
        "confirmed_balance_sats": chain.get("funded_txo_sum", 0) - chain.get("spent_txo_sum", 0),
        "mempool_delta_sats": pool.get("funded_txo_sum", 0) - pool.get("spent_txo_sum", 0)
    })]




async def lookup_wayback(client, domain):
    data = await get_json(client, "https://archive.org/wayback/available", {"url": "https://" + domain})
    capture = data.get("archived_snapshots", {}).get("closest", {})
    if not capture or not capture.get("available"):
        return []
    url = capture.get("url", "")
    if not isinstance(url, str) or not url.startswith("https://web.archive.org/"):
        url = "https://web.archive.org/web/*/" + quote(domain)
    return [item("Internet Archive Wayback", "Web arşivindeki en yakın görüntü", domain,
                 url, {"timestamp": capture.get("timestamp"), "available": True})]


async def lookup_openalex(client, q):
    data = await get_json(client, "https://api.openalex.org/works", {"search": q, "per-page": 10})
    return [item("OpenAlex", x.get("display_name", "Yayın"),
                 "Açık akademik yayın kaydı", x.get("doi") or x.get("id") or "",
                 {"publication_year": x.get("publication_year"), "cited_by_count": x.get("cited_by_count"),
                  "type": x.get("type"), "open_access": x.get("open_access", {}).get("is_oa")})
            for x in data.get("results", [])[:10]]


async def kev_feed(client):
    sources = [
        "https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json",
        "https://raw.githubusercontent.com/cisagov/kev-data/develop/known_exploited_vulnerabilities.json",
    ]
    last = None
    for url in sources:
        try:
            return await get_json(client, url, max_bytes=5_000_000)
        except (httpx.HTTPError, ValueError) as exc:
            last = exc
    if last:
        raise last
    raise ValueError("CISA KEV erişilemedi")


async def lookup_kev(client, q):
    data = await kev_feed(client)
    findings = []
    for v in data.get("vulnerabilities", []):
        searchable = " ".join(str(v.get(k, "")) for k in ("cveID", "vendorProject", "product", "vulnerabilityName"))
        if q.casefold() in searchable.casefold():
            cve = v.get("cveID", "")
            findings.append(item("CISA KEV", cve + " · " + str(v.get("vendorProject", "")),
                                 v.get("shortDescription", ""),
                                 "https://www.cisa.gov/known-exploited-vulnerabilities-catalog",
                                 {"cve": cve, "product": v.get("product"), "dateAdded": v.get("dateAdded"),
                                  "dueDate": v.get("dueDate"),
                                  "knownRansomwareCampaignUse": v.get("knownRansomwareCampaignUse")}))
            if len(findings) >= 20:
                break
    return findings


async def lookup_epss(client, cve):
    data = await get_json(client, "https://api.first.org/data/v1/epss", {"cve": cve})
    rows = data.get("data", [])
    if not rows:
        return []
    x = rows[0]
    return [item("FIRST EPSS", cve + " · istismar olasılığı", "Gelecek 30 gün için model tahmini; gerçekleşmiş saldırı kanıtı değildir.",
                 "https://www.first.org/epss/", {"epss_probability": x.get("epss"),
                 "percentile": x.get("percentile"), "model_date": x.get("date")})]


def safe_error(exc: Exception) -> str:
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        host = exc.response.request.url.host or ""
        if status == 403 and host.endswith("wikidata.org"):
            return ("Wikidata HTTP 403: Wikimedia isteği reddetti. Bu, aranan kaydın "
                    "mevcut olmadığı anlamına gelmez; erişim veya sunucu politikası kısıtı olabilir.")
        if status == 403:
            return "Kaynak HTTP 403: erişim reddedildi; erişim yetkisi veya kaynak politikası kontrol edilmeli."
        if status == 429:
            return "Kaynak HTTP 429: istek kotası aşıldı; tekrar denemeden önce bekleyin."
        return f"Kaynak HTTP {status} yanıtı verdi; erişim ya da kaynak hatası olabilir."
    if isinstance(exc, (httpx.TimeoutException, asyncio.TimeoutError)):
        return "Kaynak zaman aşımına uğradı."
    if isinstance(exc, httpx.RequestError):
        return "Kaynağa ağ bağlantısı kurulamadı."
    if isinstance(exc, (ValueError, json.JSONDecodeError)):
        return str(exc)[:160]
    LOG.warning("Adapter error: %s", type(exc).__name__)
    return "Veri kaynağında beklenmeyen hata oluştu."


async def source_job(label: str, coroutine):
    start = time.monotonic()
    try:
        results = await coroutine
        status = "partial" if results and any(x.get("status") == "error" for x in results) else ("ok" if results else "empty")
        _source_health[label] = {"status": status, "last_attempt": now(),
                                 "last_success": now(), "latency_ms": round((time.monotonic()-start)*1000)}
        return results
    except Exception as exc:
        msg = safe_error(exc)
        _source_health[label] = {"status": "error", "last_attempt": now(),
                                 "last_error": msg, "latency_ms": round((time.monotonic()-start)*1000)}
        error_item = item(label, "Kaynak hatası", msg)
        error_item["status"] = "error"
        error_item["evidence_level"] = "error"
        return [error_item]


def get_tasks(client, kind, q):
    if kind == "username_public":
        return [("GitHub Username", username_github_public(client, q)),
                ("Bluesky Handle", username_bluesky_public(client, q))]
    if kind == "company_public":
        return [("Wikidata Organizations", lookup_company_wikidata(client, q)),
                ("OpenAlex Institutions", lookup_company_openalex(client, q))]
    if kind == "person_public":
        return [("Wikidata", lookup_public_wikidata(client, q)),
                ("OpenAlex Authors", lookup_public_academics(client, q)),
                ("Crossref Publications", lookup_public_crossref(client, q))]
    if kind == "social_profile":
        return [("Public social profile", lookup_social_profile(client, q))]
    if kind == "web_trace":
        return [("Google Public DNS", domain_dns(client, q)),
                ("RDAP Domain", domain_rdap(client, q)),
                ("crt.sh", domain_ct(client, q)),
                ("Wayback Machine", lookup_wayback(client, q))]
    if kind == "domain":
        return [("Google Public DNS", domain_dns(client, q)),
                ("RDAP Domain", domain_rdap(client, q)),
                ("crt.sh", domain_ct(client, q))]
    if kind == "cve":
        return [("NIST NVD", lookup_cve(client, q)),
                ("FIRST EPSS", lookup_epss(client, q)),
                ("CISA KEV", lookup_kev(client, q))]
    mapping = {
        "ip": ("RDAP IP", lookup_ip), "github_user": ("GitHub User", lookup_github_user),
        "github_repos": ("GitHub Search", lookup_github_repos),
        "bluesky": ("Bluesky AppView", lookup_bluesky),
        "news": ("GDELT DOC", lookup_news), "bitcoin": ("mempool.space", lookup_bitcoin),
        "wayback": ("Wayback Machine", lookup_wayback),
        "openalex": ("OpenAlex", lookup_openalex), "kev": ("CISA KEV", lookup_kev),
    }
    name, fn = mapping[kind]
    return [(name, fn(client, q))]


def make_client():
    return httpx.AsyncClient(timeout=REQUEST_TIMEOUT, follow_redirects=True,
                             headers={"User-Agent": WIKIMEDIA_USER_AGENT},
                             limits=httpx.Limits(max_connections=10))


async def investigate(kind: str, query: str):
    q = validate(kind, query)
    key = (kind, q.casefold())
    hit = _cache.get(key)
    if hit and hit[0] > time.monotonic():
        return hit[1], True
    async with make_client() as client:
        tasks = [source_job(name, task) for name, task in get_tasks(client, kind, q)]
        response = await asyncio.gather(*tasks)
        out = [attach_evidence_metadata(row, kind, q) for group in response for row in group]
    # Do not cache failures; the next request must be able to retry.
    if not any(x.get("status") == "error" for x in out):
        if len(_cache) >= MAX_CACHE_ENTRIES:
            _cache.clear()
        _cache[key] = (time.monotonic() + CACHE_TTL, out)
    return out, False


@app.get("/")
async def home():
    return FileResponse(ROOT / "index.html")


@app.get("/api/health")
async def health():
    return {"status": "ok", "version": VERSION, "protected": bool(APP_TOKEN),
            "ready": bool(APP_TOKEN), "sources": len(KINDS), "time_utc": now(),
            "features": ["evidence_sha256", "relationship_graph", "timeline", "source_comparison", "case_export", "public_name_candidates", "explicit_social_profile", "domain_footprint", "local_photo_metadata", "username_candidates", "organization_candidates", "local_header_analysis"]}


@app.get("/api/sources")
async def sources(x_osint_key: str | None = Header(default=None)):
    require_token(x_osint_key)
    return {"sources": [{"id": k, "name": v, "type": "REST (on-demand)"} for k, v in KINDS.items()],
            "live": ["mempool.space WebSocket (browser direct)"], "source_health": _source_health}


@app.post("/api/investigate")
async def investigate_rest(payload: SearchRequest, x_osint_key: str | None = Header(default=None), request: Request = None):
    require_token(x_osint_key)
    check_rate_limit(request.client.host if request and request.client else "unknown")
    try:
        items, cached = await investigate(payload.kind, payload.query)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"kind": payload.kind, "query": payload.query.strip(), "results": items, "cached": cached, "checked_at": now()}


@app.get("/api/source-health")
async def source_health(x_osint_key: str | None = Header(default=None)):
    require_token(x_osint_key)
    return {"observations": _source_health, "note": "Son istek gözlemleridir; doğrulanmış kesintisiz canlı sağlık testi değildir."}


@app.websocket("/ws/investigate")
async def investigate_ws(ws: WebSocket):
    await ws.accept()
    try:
        request = await asyncio.wait_for(ws.receive_json(), timeout=12)
        if not APP_TOKEN:
            await ws.send_json({"type": "error", "message": "OSINT_APP_TOKEN ayarlanmamış. Güvenlik için sorgu kapalı."})
            await ws.close(code=1008)
            return
        if request.get("token") != APP_TOKEN:
            await ws.send_json({"type": "error", "message": "Uygulama anahtarı hatalı."})
            await ws.close(code=1008)
            return
        try:
            check_rate_limit(ws.client.host if ws.client else "unknown")
        except HTTPException:
            await ws.send_json({"type": "error", "message": "Sorgu limiti: dakikada en fazla 18 istek."})
            await ws.close(code=1008)
            return
        kind, query = request.get("kind", ""), request.get("query", "")
        q = validate(kind, query)
        await ws.send_json({"type": "started", "at": now(), "kind": kind})
        key = (kind, q.casefold())
        hit = _cache.get(key)
        if hit and hit[0] > time.monotonic():
            results, cached = hit[1], True
            for i, x in enumerate(results):
                await ws.send_json({"type": "result", "item": x, "index": i})
        else:
            cached, results = False, []
            async with make_client() as client:
                tasks = [asyncio.create_task(source_job(name, coro)) for name, coro in get_tasks(client, kind, q)]
                for completed in asyncio.as_completed(tasks):
                    batch = await completed
                    for x in batch:
                        observed = attach_evidence_metadata(x, kind, q)
                        results.append(observed)
                        await ws.send_json({"type": "result", "item": observed, "index": len(results)-1})
            if not any(x.get("status") == "error" for x in results):
                if len(_cache) >= MAX_CACHE_ENTRIES:
                    _cache.clear()
                _cache[key] = (time.monotonic() + CACHE_TTL, results)
        errors = sum(x.get("status") == "error" for x in results)
        await ws.send_json({"type": "done", "count": len(results)-errors,
                            "errors": errors, "cached": cached, "at": now()})
        await ws.close()
    except (ValueError, TypeError) as exc:
        await ws.send_json({"type": "error", "message": str(exc)[:180]})
        await ws.close(code=1003)
    except (WebSocketDisconnect, RuntimeError):
        return
    except Exception:
        LOG.exception("Unhandled WS request error")
        try:
            await ws.send_json({"type": "error", "message": "Beklenmeyen bağlantı hatası."})
            await ws.close(code=1011)
        except Exception:
            pass
