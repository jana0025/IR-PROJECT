from flask import Flask, request, jsonify
from flask_cors import CORS
from opensearchpy import OpenSearch, helpers
from bs4 import BeautifulSoup
import os
import re
from datetime import datetime
from pathlib import Path
import spacy

app = Flask(__name__)
CORS(app)

# تحميل نموذج spaCy لاستخراج المواقع
try:
    nlp = spacy.load("en_core_web_sm")
    NLP_AVAILABLE = True
except:
    print("Warning: spaCy model not available. Location extraction will be limited.")
    NLP_AVAILABLE = False

# إعداد OpenSearch
OPENSEARCH_HOST = os.getenv('OPENSEARCH_HOST', 'localhost')
OPENSEARCH_PORT = int(os.getenv('OPENSEARCH_PORT', 9200))

client = OpenSearch(
    hosts=[{'host': OPENSEARCH_HOST, 'port': OPENSEARCH_PORT}],
    http_auth=None,
    use_ssl=False,
    verify_certs=False,
    ssl_show_warn=False
)

INDEX_NAME = "smart_documents"

# ============= دالة استخراج المواقع من النص =============

def extract_locations_from_text(text):
    """
    استخراج المواقع الجغرافية من النص باستخدام NLP
    """
    if not text or not NLP_AVAILABLE:
        return []
    
    try:
        doc = nlp(text[:5000])  # أول 5000 حرف للسرعة
        locations = []
        
        for ent in doc.ents:
            if ent.label_ in ["GPE", "LOC"]:  # GPE: countries/cities, LOC: locations
                location_name = ent.text.strip()
                if location_name and location_name not in locations:
                    locations.append(location_name)
        
        return locations[:5]  # أول 5 مواقع
    except Exception as e:
        print(f"Error extracting locations: {e}")
        return []


def ensure_document_has_location(document):
    """
    التأكد من أن المستند عنده location
    إذا ما عنده، استخرج من الـ content
    """
    # إذا موجود georeferences وفيه بيانات، خلاص تمام
    if document.get('georeferences') and len(document['georeferences']) > 0:
        return document
    
    # إذا مش موجود، استخرج من المحتوى
    content = document.get('content', '') or document.get('title', '')
    
    if content:
        extracted_locations = extract_locations_from_text(content)
        if extracted_locations:
            document['georeferences'] = extracted_locations
            document['location_source'] = 'auto-extracted'  # علامة أنه مستخرج تلقائياً
        else:
            # إذا ما لقى، حط Unknown
            document['georeferences'] = ['Unknown']
            document['location_source'] = 'default'
    else:
        document['georeferences'] = ['Unknown']
        document['location_source'] = 'default'
    
    return document


# ============= دالة إزالة التكرارات =============

def remove_duplicates(results, key="title"):
    """
    إزالة التكرارات من النتائج
    """
    seen = set()
    unique_results = []
    
    for item in results:
        if isinstance(item, dict):
            identifier = item.get(key, "")
        else:
            identifier = getattr(item, key, "")
        
        identifier = str(identifier).lower().strip()
        
        if identifier and identifier not in seen:
            seen.add(identifier)
            unique_results.append(item)
    
    return unique_results


# ============= دوال SGML =============

def parse_sgm_file(file_path):
    """
    قراءة وتحليل ملف SGML
    """
    documents = []
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        soup = BeautifulSoup(content, 'html.parser')
        reuters_docs = soup.find_all('reuters')
        
        for doc in reuters_docs:
            try:
                title_tag = doc.find('title')
                body_tag = doc.find('body')
                date_tag = doc.find('date')
                places_tag = doc.find('places')
                
                title = title_tag.get_text(strip=True) if title_tag else ""
                body = body_tag.get_text(strip=True) if body_tag else ""
                date_str = date_tag.get_text(strip=True) if date_tag else ""
                
                # استخراج الأماكن من الـ SGML
                places = []
                if places_tag:
                    place_tags = places_tag.find_all('d')
                    places = [p.get_text(strip=True) for p in place_tags]
                
                doc_date = parse_reuters_date(date_str)
                
                document = {
                    "title": title,
                    "content": body,
                    "date": doc_date,
                    "georeferences": places if places else [],
                    "temporal_expressions": extract_temporal_expressions(body),
                    "source_file": os.path.basename(file_path)
                }
                
                # التأكد من وجود location
                document = ensure_document_has_location(document)
                
                if title or body:
                    documents.append(document)
                    
            except Exception as e:
                print(f"Error parsing document: {str(e)}")
                continue
        
        return documents
        
    except Exception as e:
        print(f"Error reading file {file_path}: {str(e)}")
        return []


def parse_reuters_date(date_str):
    """
    تحويل تاريخ Reuters
    """
    try:
        date_str = re.sub(r'\.\d+$', '', date_str)
        dt = datetime.strptime(date_str, "%d-%b-%Y %H:%M:%S")
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except:
        return None


def extract_temporal_expressions(text):
    """
    استخراج التعابير الزمنية
    """
    temporal_patterns = [
        r'\b\d{4}\b',
        r'\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}\b',
        r'\b\d{1,2}/\d{1,2}/\d{2,4}\b',
    ]
    
    expressions = []
    for pattern in temporal_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        expressions.extend(matches)
    
    return list(set(expressions))[:10]


# ============= APIs =============
@app.route('/api/index/document', methods=['POST'])
def index_document():
    """
    فهرسة مستند واحد مع دعم Location اختياري - Fixed Version
    """
    try:
        document = request.json
        
        # التأكد من وجود location
        document = ensure_document_has_location(document)
        
        # الحل: استخدم refresh='wait_for' بدل True
        response = client.index(
            index=INDEX_NAME,
            body=document,
            refresh='wait_for'  # ✅ هذا الحل الصحيح
        )
        
        # تأكد إن المستند اتخزن
        doc_id = response["_id"]
        
        # تحقق من المستند (اختياري)
        verify = client.get(index=INDEX_NAME, id=doc_id)
        
        return jsonify({
            "success": True,
            "message": "Document indexed successfully and persisted",
            "document_id": doc_id,
            "location": document.get('georeferences', []),
            "location_source": document.get('location_source', 'manual'),
            "verified": verify['found']
        }), 201
        
    except Exception as e:
        import traceback
        return jsonify({
            "success": False,
            "error": str(e),
            "trace": traceback.format_exc()
        }), 500


@app.route('/api/index/bulk', methods=['POST'])
def bulk_index():
    """
    فهرسة مجموعة مستندات - Fixed Version
    """
    try:
        data = request.json
        documents = data.get("documents", [])
        
        # التأكد من locations لكل مستند
        for doc in documents:
            ensure_document_has_location(doc)
        
        actions = [
            {
                "_index": INDEX_NAME,
                "_source": doc
            }
            for doc in documents
        ]
        
        # ✅ استخدم refresh='wait_for' مع bulk
        success, failed = helpers.bulk(
            client, 
            actions, 
            raise_on_error=False,
            refresh='wait_for'  # الحل هنا
        )
        
        # تحقق من العدد الفعلي
        count = client.count(index=INDEX_NAME)
        
        return jsonify({
            "success": True,
            "indexed": success,
            "failed": failed,
            "total": len(documents),
            "current_index_count": count['count']
        }), 201
        
    except Exception as e:
        import traceback
        return jsonify({
            "success": False,
            "error": str(e),
            "trace": traceback.format_exc()
        }), 500


# ✅ وظيفة جديدة للتحقق من المستند
@app.route('/api/document/<doc_id>', methods=['GET'])
def get_document(doc_id):
    """
    جلب مستند محدد للتحقق من وجوده
    """
    try:
        doc = client.get(index=INDEX_NAME, id=doc_id)
        
        return jsonify({
            "success": True,
            "found": doc['found'],
            "document": doc['_source']
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e),
            "found": False
        }), 404


# ✅ وظيفة للتحقق من استقرار البيانات
@app.route('/api/index/verify', methods=['GET'])
def verify_index():
    """
    التحقق من حالة الفهرس والبيانات
    """
    try:
        # عدد المستندات
        count = client.count(index=INDEX_NAME)
        
        # إحصائيات الفهرس
        stats = client.indices.stats(index=INDEX_NAME)
        
        # حالة الفهرس
        health = client.cluster.health(index=INDEX_NAME)
        
        return jsonify({
            "success": True,
            "document_count": count['count'],
            "index_health": health['status'],
            "shards": {
                "total": health['active_shards'],
                "primary": health['active_primary_shards']
            },
            "message": "Index is stable" if count['count'] > 0 else "Index is empty"
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/search/autocomplete', methods=['GET'])
def autocomplete():
    """
    البحث التلقائي
    """
    try:
        query = request.args.get('q', '')
        size = int(request.args.get('size', 10))
        
        if len(query) < 3:
            return jsonify({
                "success": True,
                "results": []
            })
        
        search_query = {
            "size": size * 3,
            "query": {
                "bool": {
                    "should": [
                        {
                            "match": {
                                "title": {
                                    "query": query,
                                    "fuzziness": "AUTO",
                                    "boost": 2
                                }
                            }
                        },
                        {
                            "match_phrase_prefix": {
                                "title": {
                                    "query": query,
                                    "boost": 3
                                }
                            }
                        }
                    ]
                }
            },
            "_source": ["title", "content", "georeferences", "location_source"]
        }
        
        response = client.search(index=INDEX_NAME, body=search_query)
        
        results = []
        for hit in response["hits"]["hits"]:
            source = hit["_source"]
            results.append({
                "id": hit["_id"],
                "title": source.get("title", ""),
                "content": source.get("content", ""),
                "georeferences": source.get("georeferences", ["Unknown"]),
                "location_source": source.get("location_source", "unknown"),
                "score": hit["_score"]
            })
        
        results = remove_duplicates(results, key="title")
        
        return jsonify({
            "success": True,
            "results": results[:size]
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route('/api/search/smart', methods=['POST'])
def smart_search():
    """
    البحث الذكي
    """
    try:
        data = request.json
        query_text = data.get('query', '')
        temporal = data.get('temporal_expression')
        geo = data.get('georeference')
        size = data.get('size', 10)
        
        should_clauses = []
        
        if query_text:
            should_clauses.extend([
                {
                    "match": {
                        "title": {
                            "query": query_text,
                            "boost": 3,
                            "fuzziness": "AUTO"
                        }
                    }
                },
                {
                    "match": {
                        "content": {
                            "query": query_text,
                            "boost": 1
                        }
                    }
                }
            ])
        
        if temporal:
            should_clauses.append({
                "match": {
                    "temporal_expressions": {
                        "query": temporal,
                        "boost": 1.5
                    }
                }
            })
        
        if geo:
            should_clauses.append({
                "match": {
                    "georeferences": {
                        "query": geo,
                        "boost": 1.5
                    }
                }
            })
        
        search_query = {
            "size": size * 3,
            "query": {
                "bool": {
                    "should": should_clauses,
                    "minimum_should_match": 1
                }
            },
            "sort": [
                "_score",
                {
                    "date": {
                        "order": "desc",
                        "missing": "_last"
                    }
                }
            ]
        }
        
        response = client.search(index=INDEX_NAME, body=search_query)
        
        results = []
        for hit in response["hits"]["hits"]:
            result = hit["_source"]
            result["id"] = hit["_id"]
            result["score"] = hit["_score"]
            
            # التأكد من وجود georeferences
            if not result.get('georeferences'):
                result['georeferences'] = ['Unknown']
            
            results.append(result)
        
        results = remove_duplicates(results, key="title")
        
        return jsonify({
            "success": True,
            "results": results[:size],
            "total": len(results)
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route('/api/analytics/top-georeferences', methods=['GET'])
def top_georeferences():
    """
    أكثر المواقع تكراراً
    """
    try:
        size = int(request.args.get('size', 10))
        
        query = {
            "size": 0,
            "aggs": {
                "top_georeferences": {
                    "terms": {
                        "field": "georeferences.keyword",
                        "size": size * 2  # ضعف العدد للتأكد
                    }
                }
            }
        }
        
        response = client.search(index=INDEX_NAME, body=query)
        
        results = []
        if "aggregations" in response:
            buckets = response["aggregations"]["top_georeferences"]["buckets"]
            # فلترة Unknown إذا كان فيه مواقع تانية
            filtered_buckets = [b for b in buckets if b["key"] != "Unknown"]
            
            # إذا ما فيه غير Unknown، خليه
            if not filtered_buckets:
                filtered_buckets = buckets
            
            results = [
                {
                    "georeference": bucket["key"],
                    "count": bucket["doc_count"]
                }
                for bucket in filtered_buckets[:size]
            ]
        
        return jsonify({
            "success": True,
            "results": results
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route('/api/analytics/time-distribution', methods=['GET'])
def time_distribution():
    """
    توزيع المستندات عبر الزمن
    """
    try:
        interval = request.args.get('interval', '1d')
        
        query = {
            "size": 0,
            "aggs": {
                "documents_over_time": {
                    "date_histogram": {
                        "field": "date",
                        "calendar_interval": interval,
                        "format": "yyyy-MM-dd",
                        "min_doc_count": 0
                    }
                }
            }
        }
        
        response = client.search(index=INDEX_NAME, body=query)
        
        results = []
        if "aggregations" in response:
            buckets = response["aggregations"]["documents_over_time"]["buckets"]
            results = [
                {
                    "date": bucket["key_as_string"],
                    "count": bucket["doc_count"]
                }
                for bucket in buckets
            ]
        
        return jsonify({
            "success": True,
            "results": results
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route('/api/analytics/dashboard', methods=['GET'])
def dashboard():
    """
    لوحة التحكم
    """
    try:
        count_response = client.count(index=INDEX_NAME)
        total_docs = count_response["count"]
        
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
        
        top_geo = []
        if "aggregations" in geo_response:
            buckets = geo_response["aggregations"]["top_georeferences"]["buckets"]
            top_geo = [
                {"georeference": b["key"], "count": b["doc_count"]}
                for b in buckets
            ]
        
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
        
        time_dist = []
        if "aggregations" in time_response:
            buckets = time_response["aggregations"]["documents_over_time"]["buckets"]
            time_dist = [
                {"date": b["key_as_string"], "count": b["doc_count"]}
                for b in buckets
            ]
        
        return jsonify({
            "success": True,
            "total_documents": total_docs,
            "top_georeferences": top_geo,
            "documents_over_time": time_dist
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route('/api/health', methods=['GET'])
def health_check():
    """
    فحص الصحة
    """
    try:
        info = client.info()
        return jsonify({
            "success": True,
            "opensearch": info,
            "nlp_available": NLP_AVAILABLE
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route('/api/index/stats', methods=['GET'])
def index_stats():
    """
    إحصائيات الفهرس
    """
    try:
        if not client.indices.exists(index=INDEX_NAME):
            return jsonify({
                "success": False,
                "error": f"Index '{INDEX_NAME}' does not exist"
            }), 404
        
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
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


if __name__ == '__main__':
    print("="*60)
    print("Smart Document Retrieval System - Enhanced Location Support")
    print("="*60)
    print(f"OpenSearch: {OPENSEARCH_HOST}:{OPENSEARCH_PORT}")
    print(f"Index: {INDEX_NAME}")
    print(f"NLP Available: {NLP_AVAILABLE}")
    print(f"Auto-location extraction: {'Enabled' if NLP_AVAILABLE else 'Disabled'}")
    print("="*60)
    app.run(debug=True, host='0.0.0.0', port=5000)