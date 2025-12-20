from flask import Flask, request, jsonify
from flask_cors import CORS
from opensearchpy import OpenSearch, helpers
from bs4 import BeautifulSoup
from geopy.geocoders import Nominatim
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
import spacy
from collections import Counter
import time
import dateparser


app = Flask(__name__)
CORS(app)


# ==================== NLP Setup ====================
# Load spaCy model for entity extraction (locations, etc.)
try:
    nlp = spacy.load("en_core_web_sm")
    NLP_AVAILABLE = True
except:
    print("Warning: spaCy model not available. Location extraction will be limited.")
    NLP_AVAILABLE = False

# Geocoder used to convert location names into lat/lon
geolocator = Nominatim(user_agent="smart_document_retrieval")


# ==================== OpenSearch Setup ====================
# Read OpenSearch connection settings from environment variables
OPENSEARCH_HOST = os.getenv("OPENSEARCH_HOST", "localhost")
OPENSEARCH_PORT = int(os.getenv("OPENSEARCH_PORT", 9200))

# Create OpenSearch client connection
client = OpenSearch(
    hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
    http_auth=None,
    use_ssl=False,
    verify_certs=False,
    ssl_show_warn=False
)

# OpenSearch index name
INDEX_NAME = "smart_documents"


# ==================== Location Extraction ====================
# Extract all unique location entities from text
def extract_all_locations(text):
    if not text or not NLP_AVAILABLE:
        return []

    doc = nlp(text[:8000])
    raw = [ent.text.strip() for ent in doc.ents if ent.label_ in ["GPE", "LOC", "FAC"]]

    cleaned = []
    seen = set()
    for loc in raw:
        norm = re.sub(r"\s+", " ", loc).strip()
        key = norm.lower()
        if key and key not in seen:
            seen.add(key)
            cleaned.append(norm)

    return cleaned


# Extract the most frequent location mentioned in text (main location)
def extract_most_frequent_location(text):
    if not text or not NLP_AVAILABLE:
        return None

    doc = nlp(text[:8000])
    raw = [ent.text.strip() for ent in doc.ents if ent.label_ in ["GPE", "LOC", "FAC"]]
    if not raw:
        return None

    norm = [re.sub(r"\s+", " ", x).lower().strip() for x in raw]
    counts = Counter(norm)
    top_norm = counts.most_common(1)[0][0]

    for loc in raw:
        if re.sub(r"\s+", " ", loc).lower().strip() == top_norm:
            return re.sub(r"\s+", " ", loc).strip()

    return None


# Cache geocoding results to avoid repeated API calls
GEOCODE_CACHE = {}

# Convert location names into latitude/longitude points
def geocode_locations(location_names, limit=10):
    points = []
    for name in location_names[:limit]:
        key = str(name).strip().lower()
        if not key:
            continue

        if key in GEOCODE_CACHE:
            cached = GEOCODE_CACHE[key]
            if cached:
                points.append(cached)
            continue

        try:
            loc = geolocator.geocode(name, timeout=5)
            time.sleep(0.15)
            if loc:
                point = {"lat": loc.latitude, "lon": loc.longitude}
                GEOCODE_CACHE[key] = point
                points.append(point)
            else:
                GEOCODE_CACHE[key] = None
        except:
            GEOCODE_CACHE[key] = None

    return points


# Ensure the document has georeferences and optionally geo coordinates
def ensure_document_has_location(document, do_geocode=True, geo_limit=1):
    text = ((document.get("content") or "") + " " + (document.get("title") or "")).strip()

    existing = document.get("georeferences") or []
    if isinstance(existing, str):
        existing = [existing]

    if existing:
        document["georeferences"] = list(dict.fromkeys([str(x).strip() for x in existing if str(x).strip()]))
        document["location_source"] = document.get("location_source", "from-places")
    else:
        all_locs = extract_all_locations(text) if text else []
        if all_locs:
            document["georeferences"] = all_locs
            document["location_source"] = "auto-extracted"
        else:
            document["georeferences"] = []
            document["location_source"] = "default"

    if do_geocode and document["georeferences"]:
        document["geo_points"] = geocode_locations(document["georeferences"], limit=geo_limit)

        main_loc = extract_most_frequent_location(text)
        if main_loc:
            main_point = geocode_locations([main_loc], limit=1)
            if main_point:
                document["geopoint"] = main_point[0]

    return document


# ==================== Date Handling (FIXED) ====================
# Map month names to numbers for parsing dates like "March 10, 2020"
MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12
}

# Normalize different date formats into "YYYY-MM-DDTHH:MM:SS"
def normalize_date_value(value):
    """
    Convert any date string (ISO with Z, milliseconds, timezone offsets)
    to: YYYY-MM-DDTHH:MM:SS
    """
    if not value:
        return None

    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%S")

    if isinstance(value, str):
        s = value.strip()

        # ISO: 2025-12-20T06:38:53.990Z -> 2025-12-20T06:38:53
        m = re.match(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(\.\d+)?(Z)?$", s)
        if m:
            return m.group(1)

        # ISO with offset: 2025-12-20T06:38:53+00:00 or 2025-12-20T06:38:53.123+00:00
        m2 = re.match(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(\.\d+)?([+-]\d{2}:\d{2})$", s)
        if m2:
            return m2.group(1)

        dt = dateparser.parse(s)
        if dt:
            return dt.strftime("%Y-%m-%dT%H:%M:%S")

    return None


# Extract possible full dates from raw text (not year-only)
def parse_date_candidates_from_text(text, limit=10):
    if not text:
        return []

    cands = []

    # ISO: 1987-02-26
    for m in re.findall(r"\b(\d{4})-(\d{2})-(\d{2})\b", text):
        y, mo, d = map(int, m)
        try:
            cands.append(datetime(y, mo, d))
        except:
            pass

    # US: 2/26/1987 or 02/26/1987
    for m in re.findall(r"\b(\d{1,2})/(\d{1,2})/(\d{2,4})\b", text):
        mo, d, y = m
        y = int(y)
        if y < 100:
            y += 1900 if y >= 50 else 2000
        try:
            cands.append(datetime(int(y), int(mo), int(d)))
        except:
            pass

    # Month DD, YYYY
    for m in re.findall(
        r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})\b",
        text,
        re.IGNORECASE
    ):
        mon, day, year = m
        try:
            cands.append(datetime(int(year), MONTHS[mon.lower()], int(day)))
        except:
            pass

    # IMPORTANT: DO NOT create "date" from year-only. Year-only stays in temporal_expressions.

    uniq = []
    seen = set()
    for d in cands:
        key = d.date().isoformat()
        if key not in seen:
            seen.add(key)
            uniq.append(d)

    return uniq[:limit]


# Ensure the document has a normalized "date" field, or extracted_dates list
def ensure_document_has_date(document):
    if document.get("date"):
        fixed = normalize_date_value(document.get("date"))
        if fixed:
            document["date"] = fixed
        else:
            document.pop("date", None)

        if "extracted_dates" not in document:
            document["extracted_dates"] = []
        return document

    text = ((document.get("title") or "") + " " + (document.get("content") or "")).strip()
    cands = parse_date_candidates_from_text(text, limit=10)

    if cands:
        chosen = max(cands)
        document["date"] = chosen.strftime("%Y-%m-%dT%H:%M:%S")
        document["extracted_dates"] = [d.strftime("%Y-%m-%dT%H:%M:%S") for d in cands]
    else:
        document["extracted_dates"] = []
        # leave date missing

    return document


# ==================== Helpers ====================
# Remove duplicate results based on a specific key (default: title)
def remove_duplicates(results, key="title"):
    seen = set()
    unique_results = []
    for item in results:
        identifier = item.get(key, "") if isinstance(item, dict) else getattr(item, key, "")
        identifier = str(identifier).lower().strip()
        if identifier and identifier not in seen:
            seen.add(identifier)
            unique_results.append(item)
    return unique_results


# ==================== SGML Parsing ====================
# Parse Reuters date format into normalized date string
def parse_reuters_date(date_str):
    try:
        date_str = re.sub(r"\.\d+$", "", date_str)
        dt = datetime.strptime(date_str, "%d-%b-%Y %H:%M:%S")
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except:
        return None


# Extract temporal expressions (years and date-like patterns) from text
def extract_temporal_expressions(text):
    temporal_patterns = [
        r"\b\d{4}\b",
        r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}\b",
        r"\b\d{1,2}/\d{1,2}/\d{2,4}\b",
        r"\b\d{4}-\d{2}-\d{2}\b",
    ]

    expressions = []
    for pattern in temporal_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        expressions.extend(matches)

    return list(set(expressions))[:10]


# Parse one Reuters .sgm file and extract documents
def parse_sgm_file(file_path):
    documents = []
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()

        soup = BeautifulSoup(content, "html.parser")
        reuters_docs = soup.find_all("reuters")

        for doc in reuters_docs:
            try:
                title_tag = doc.find("title")
                body_tag = doc.find("body")
                date_tag = doc.find("date")
                places_tag = doc.find("places")

                title = title_tag.get_text(strip=True) if title_tag else ""
                body = body_tag.get_text(strip=True) if body_tag else ""
                date_str = date_tag.get_text(strip=True) if date_tag else ""

                places = []
                if places_tag:
                    place_tags = places_tag.find_all("d")
                    places = [p.get_text(strip=True) for p in place_tags]

                doc_date = parse_reuters_date(date_str) if date_str else None

                document = {
                    "title": title,
                    "content": body,
                    "date": doc_date,
                    "georeferences": places if places else [],
                    "temporal_expressions": extract_temporal_expressions(body),
                    "source_file": os.path.basename(file_path)
                }

                document = ensure_document_has_date(document)
                document = ensure_document_has_location(document, do_geocode=False)

                if title or body:
                    documents.append(document)

            except Exception as e:
                print(f"Error parsing document: {str(e)}")
                continue

        return documents

    except Exception as e:
        print(f"Error reading file {file_path}: {str(e)}")
        return []


# ==================== APIs ====================
# Index a single document into OpenSearch
@app.route("/api/index/document", methods=["POST"])
def index_document():
    try:
        document = request.json or {}

        document = ensure_document_has_date(document)
        document = ensure_document_has_location(document, do_geocode=True, geo_limit=1)

        response = client.index(
            index=INDEX_NAME,
            body=document,
            refresh="wait_for"
        )

        doc_id = response["_id"]
        verify = client.get(index=INDEX_NAME, id=doc_id)

        return jsonify({
            "success": True,
            "message": "Document indexed successfully and persisted",
            "document_id": doc_id,
            "georeferences": document.get("georeferences", []),
            "geo_points": document.get("geo_points", []),
            "geopoint": document.get("geopoint"),
            "date": document.get("date"),
            "extracted_dates": document.get("extracted_dates", []),
            "location_source": document.get("location_source", "manual"),
            "verified": verify["found"]
        }), 201

    except Exception as e:
        import traceback
        return jsonify({
            "success": False,
            "error": str(e),
            "trace": traceback.format_exc()
        }), 500


# Bulk index many documents into OpenSearch
@app.route("/api/index/bulk", methods=["POST"])
def bulk_index():
    try:
        data = request.json or {}
        documents = data.get("documents", [])

        for doc in documents:
            ensure_document_has_date(doc)
            ensure_document_has_location(doc, do_geocode=True, geo_limit=1)

        actions = [{"_index": INDEX_NAME, "_source": doc} for doc in documents]

        success, failed = helpers.bulk(
            client,
            actions,
            raise_on_error=False,
            refresh="wait_for"
        )

        count = client.count(index=INDEX_NAME)

        return jsonify({
            "success": True,
            "indexed": success,
            "failed": failed,
            "total": len(documents),
            "current_index_count": count["count"]
        }), 201

    except Exception as e:
        import traceback
        return jsonify({
            "success": False,
            "error": str(e),
            "trace": traceback.format_exc()
        }), 500


# Get one document by ID
@app.route("/api/document/<doc_id>", methods=["GET"])
def get_document(doc_id):
    try:
        doc = client.get(index=INDEX_NAME, id=doc_id)
        return jsonify({
            "success": True,
            "found": doc["found"],
            "document": doc["_source"]
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e),
            "found": False
        }), 404


# Check index count and health
@app.route("/api/index/verify", methods=["GET"])
def verify_index():
    try:
        count = client.count(index=INDEX_NAME)
        health = client.cluster.health(index=INDEX_NAME)

        return jsonify({
            "success": True,
            "document_count": count["count"],
            "index_health": health["status"],
            "shards": {
                "total": health["active_shards"],
                "primary": health["active_primary_shards"]
            },
            "message": "Index is stable" if count["count"] > 0 else "Index is empty"
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# Autocomplete search endpoint (title-based suggestions)
@app.route("/api/search/autocomplete", methods=["GET"])
def autocomplete():
    try:
        query = request.args.get("q", "")
        size = int(request.args.get("size", 10))

        if len(query) < 3:
            return jsonify({"success": True, "results": []})

        search_query = {
            "size": size * 3,
            "query": {
                "bool": {
                    "should": [
                        {"match": {"title": {"query": query, "fuzziness": "AUTO", "boost": 2}}},
                        {"match_phrase_prefix": {"title": {"query": query, "boost": 3}}}
                    ],
                    "minimum_should_match": 1
                }
            },
            "_source": ["title", "content", "georeferences", "location_source", "date"]
        }

        response = client.search(index=INDEX_NAME, body=search_query)

        results = []
        for hit in response["hits"]["hits"]:
            source = hit["_source"]
            results.append({
                "id": hit["_id"],
                "title": source.get("title", ""),
                "content": source.get("content", ""),
                "georeferences": source.get("georeferences", []),
                "location_source": source.get("location_source", "unknown"),
                "score": hit["_score"]
            })

        results = remove_duplicates(results, key="title")
        return jsonify({"success": True, "results": results[:size]})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# Convert a temporal input string into an OpenSearch date filter or temporal_expressions match
def parse_temporal_to_date_filter(t):
    if not t:
        return None

    t = t.strip()

    try:
        d = datetime.strptime(t, "%Y-%m-%d")
        start = d.strftime("%Y-%m-%dT00:00:00")
        end = (d + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00")
        return {"range": {"date": {"gte": start, "lt": end}}}
    except:
        pass

    try:
        d = datetime.strptime(t, "%m/%d/%Y")
        start = d.strftime("%Y-%m-%dT00:00:00")
        end = (d + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00")
        return {"range": {"date": {"gte": start, "lt": end}}}
    except:
        pass

    # Year input: (range on date) OR (match temporal_expressions)
    if re.fullmatch(r"\d{4}", t):
        y = int(t)
        start = f"{y}-01-01T00:00:00"
        end = f"{y+1}-01-01T00:00:00"
        return {
            "bool": {
                "should": [
                    {"range": {"date": {"gte": start, "lt": end}}},
                    {"match": {"temporal_expressions": {"query": t, "boost": 2.0}}}
                ],
                "minimum_should_match": 1
            }
        }

    return {"match": {"temporal_expressions": {"query": t, "boost": 1.5}}}


@app.route("/api/search/smart", methods=["POST"])
def smart_search():
    try:
        data = request.json or {}
        query_text = (data.get("query") or "").strip()
        temporal = (data.get("temporal_expression") or "").strip()
        geo = (data.get("georeference") or "").strip()
        size = int(data.get("size", 10))

        must_clauses = []
        should_clauses = []
        filter_clauses = []

        # 1) Text query (title has higher priority)
        if query_text:
            must_clauses.append({
                "bool": {
                    "should": [
                        # exact title match (needs title.keyword in mapping)
                        {"term": {"title.keyword": {"value": query_text, "boost": 50}}},
                        {"match_phrase": {"title": {"query": query_text, "boost": 20}}},
                        {"match": {"title": {"query": query_text, "boost": 6, "fuzziness": "AUTO"}}},
                        {"match": {"content": {"query": query_text, "boost": 1}}}
                    ],
                    "minimum_should_match": 1
                }
            })

        # 2) Temporal boost (not required filter)
        if temporal:
            tf = parse_temporal_to_date_filter(temporal)
            if tf:
                if "range" in tf:
                    should_clauses.append({
                        "constant_score": {"filter": tf, "boost": 3}
                    })
                else:
                    should_clauses.append(tf)

        # 3) Location boost (strong boost but not required)
        if geo:
            # exact match on keyword (strongest)
            should_clauses.append({"term": {"georeferences.keyword": {"value": geo, "boost": 60}}})

            # fallback match (case / formatting differences)
            should_clauses.append({"match": {"georeferences": {"query": geo, "boost": 15}}})

        search_query = {
            "size": size * 3,
            "query": {
                "bool": {
                    "must": must_clauses,
                    "filter": filter_clauses,
                    "should": should_clauses,
                    # keep location optional
                    "minimum_should_match": 0
                }
            },
            "sort": [
                "_score",
                {"date": {"order": "desc", "missing": "_last"}}
            ]
        }

        response = client.search(index=INDEX_NAME, body=search_query)

        results = []
        for hit in response["hits"]["hits"]:
            result = hit["_source"]
            result["id"] = hit["_id"]
            result["score"] = hit["_score"]
            if not result.get("georeferences"):
                result["georeferences"] = []
            results.append(result)

        results = remove_duplicates(results, key="title")

        return jsonify({
            "success": True,
            "results": results[:size],
            "total": len(results)
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# Return the most common georeferences in the index
@app.route("/api/analytics/top-georeferences", methods=["GET"])
def top_georeferences():
    try:
        size = int(request.args.get("size", 10))

        query = {
            "size": 0,
            "aggs": {
                "top_georeferences": {
                    "terms": {
                        "field": "georeferences.keyword",
                        "size": size * 2
                    }
                }
            }
        }

        response = client.search(index=INDEX_NAME, body=query)

        buckets = response.get("aggregations", {}).get("top_georeferences", {}).get("buckets", [])
        filtered = [b for b in buckets if b.get("key")]

        results = [{"georeference": b["key"], "count": b["doc_count"]} for b in filtered[:size]]
        return jsonify({"success": True, "results": results})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# Return documents distribution over time (date histogram)
@app.route("/api/analytics/time-distribution", methods=["GET"])
def time_distribution():
    try:
        interval = request.args.get("interval", "1d")

        query = {
            "size": 0,
            "aggs": {
                "documents_over_time": {
                    "date_histogram": {
                        "field": "date",
                        "calendar_interval": interval,
                        "format": "yyyy-MM-dd"
                    }
                }
            }
        }

        response = client.search(index=INDEX_NAME, body=query)

        buckets = response.get("aggregations", {}).get("documents_over_time", {}).get("buckets", [])
        results = [{"date": b["key_as_string"], "count": b["doc_count"]} for b in buckets]

        return jsonify({"success": True, "results": results})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# Combined analytics endpoint used for dashboard stats
@app.route("/api/analytics/dashboard", methods=["GET"])
def dashboard():
    try:
        total_docs = client.count(index=INDEX_NAME)["count"]

        unique_geo_query = {
            "size": 0,
            "aggs": {
                "unique_locations": {
                    "cardinality": {
                        "field": "georeferences.keyword"
                    }
                }
            }
        }
        unique_geo_response = client.search(index=INDEX_NAME, body=unique_geo_query)
        unique_locations_count = unique_geo_response.get("aggregations", {}).get("unique_locations", {}).get("value", 0)

        top_geo_query = {
            "size": 0,
            "aggs": {
                "top_georeferences": {
                    "terms": {
                        "field": "georeferences.keyword",
                        "size": 10
                    }
                }
            }
        }
        geo_response = client.search(index=INDEX_NAME, body=top_geo_query)
        buckets = geo_response.get("aggregations", {}).get("top_georeferences", {}).get("buckets", [])
        top_geo = [{"georeference": b["key"], "count": b["doc_count"]} for b in buckets if b.get("key")]

        time_query = {
            "size": 0,
            "aggs": {
                "documents_over_time": {
                    "date_histogram": {
                        "field": "date",
                        "calendar_interval": "1d",
                        "format": "yyyy-MM-dd"
                    }
                }
            }
        }
        time_response = client.search(index=INDEX_NAME, body=time_query)
        tb = time_response.get("aggregations", {}).get("documents_over_time", {}).get("buckets", [])
        time_dist = [{"date": b["key_as_string"], "count": b["doc_count"]} for b in tb]

        return jsonify({
            "success": True,
            "total_documents": total_docs,
            "unique_locations": unique_locations_count,
            "top_georeferences": top_geo,
            "documents_over_time": time_dist
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# Basic health check endpoint for OpenSearch + NLP availability
@app.route("/api/health", methods=["GET"])
def health_check():
    try:
        info = client.info()
        return jsonify({
            "success": True,
            "opensearch": info,
            "nlp_available": NLP_AVAILABLE
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# Return index statistics (document count and size)
@app.route("/api/index/stats", methods=["GET"])
def index_stats():
    try:
        if not client.indices.exists(index=INDEX_NAME):
            return jsonify({"success": False, "error": f"Index '{INDEX_NAME}' does not exist"}), 404

        stats = client.indices.stats(index=INDEX_NAME)
        count = client.count(index=INDEX_NAME)

        index_stats_data = stats.get("indices", {}).get(INDEX_NAME, {})
        total_stats = index_stats_data.get("total", {})
        store_stats = total_stats.get("store", {})

        return jsonify({
            "success": True,
            "document_count": count["count"],
            "index_size": store_stats.get("size_in_bytes", 0),
            "index_name": INDEX_NAME,
            "status": "ready" if count["count"] > 0 else "empty"
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# Index all Reuters .sgm files from local "database" folder
@app.route('/api/index/reuters', methods=['POST'])
def index_reuters_files():
    try:
        reuters_dir = Path("database")

        if not reuters_dir.exists():
            return jsonify({"success": False, "error": f"Directory '{reuters_dir}' not found"}), 404

        sgm_files = list(reuters_dir.rglob("*.sgm")) + list(reuters_dir.rglob("*.SGM"))
        sgm_files = list(dict.fromkeys(sgm_files))

        if not sgm_files:
            return jsonify({
                "success": False,
                "error": "No .sgm/.SGM files found",
                "searched_in": str(reuters_dir.resolve())
            }), 404

        batch_size = 1000
        total_indexed = 0
        total_failed = 0
        total_docs_found = 0
        debug_first_files = []

        for fp in sgm_files:
            docs = parse_sgm_file(fp)

            if len(debug_first_files) < 5:
                debug_first_files.append({
                    "file": fp.name,
                    "docs_parsed": len(docs)
                })

            if not docs:
                continue

            total_docs_found += len(docs)

            for i in range(0, len(docs), batch_size):
                chunk = docs[i:i+batch_size]
                actions = [{"_index": INDEX_NAME, "_source": d} for d in chunk]

                success, failed = helpers.bulk(
                    client,
                    actions,
                    raise_on_error=False,
                    refresh=False
                )

                total_indexed += success
                total_failed += (len(failed) if isinstance(failed, list) else int(failed))

        client.indices.refresh(index=INDEX_NAME)
        count = client.count(index=INDEX_NAME)

        return jsonify({
            "success": True,
            "searched_in": str(reuters_dir.resolve()),
            "files_found": len(sgm_files),
            "debug_first_files": debug_first_files,
            "documents_found": total_docs_found,
            "documents_indexed": total_indexed,
            "failed": total_failed,
            "total_in_index": count["count"]
        })

    except Exception as e:
        import traceback
        return jsonify({"success": False, "error": str(e), "trace": traceback.format_exc()}), 500


if __name__ == "__main__":
    print("=" * 60)
    print("Smart Document Retrieval System")
    print("=" * 60)
    print(f"OpenSearch: {OPENSEARCH_HOST}:{OPENSEARCH_PORT}")
    print(f"Index: {INDEX_NAME}")
    print(f"NLP Available: {NLP_AVAILABLE}")
    print(f"Auto-location extraction: {'Enabled' if NLP_AVAILABLE else 'Disabled'}")
    print("=" * 60)
    app.run(debug=True, host="0.0.0.0", port=5000)
