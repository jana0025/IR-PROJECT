from flask import Flask, request, jsonify
from flask_cors import CORS
from opensearchpy import OpenSearch, helpers
from bs4 import BeautifulSoup
import os
import re
from datetime import datetime
from pathlib import Path

# استيراد الأدوات المخصصة
try:
    from entity_extractor import EntityExtractor
    from document_indexer import DocumentIndexer
    from query_engine import SmartQueryEngine
    USE_CUSTOM_TOOLS = True
except ImportError as e:
    print(f"Warning: Could not import custom tools: {e}")
    print("Falling back to direct OpenSearch queries")
    USE_CUSTOM_TOOLS = False
USE_CUSTOM_TOOLS = False
app = Flask(__name__)
CORS(app)

# إعداد OpenSearch - HTTP بدون Authentication
OPENSEARCH_HOST = os.getenv('OPENSEARCH_HOST', 'localhost')
OPENSEARCH_PORT = int(os.getenv('OPENSEARCH_PORT', 9200))

# اتصال بسيط بدون authentication
client = OpenSearch(
    hosts=[{'host': OPENSEARCH_HOST, 'port': OPENSEARCH_PORT}],
    http_auth=None,  # بدون authentication
    use_ssl=False,   # بدون SSL
    verify_certs=False,
    ssl_show_warn=False
)

INDEX_NAME = "smart_documents"

# إنشاء الأدوات المخصصة إذا كانت متوفرة
if USE_CUSTOM_TOOLS:
    extractor = EntityExtractor()
    indexer = DocumentIndexer(client, INDEX_NAME, extractor)
    query_engine = SmartQueryEngine(client, INDEX_NAME, extractor)


# ============= دوال مساعدة لقراءة SGML =============

def parse_sgm_file(file_path):
    """
    قراءة وتحليل ملف SGML واستخراج المستندات
    """
    documents = []
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        # استخدام BeautifulSoup لتحليل SGML
        soup = BeautifulSoup(content, 'html.parser')
        
        # العثور على جميع مستندات REUTERS
        reuters_docs = soup.find_all('reuters')
        
        for doc in reuters_docs:
            try:
                # استخراج المعلومات الأساسية
                title_tag = doc.find('title')
                body_tag = doc.find('body')
                date_tag = doc.find('date')
                places_tag = doc.find('places')
                
                title = title_tag.get_text(strip=True) if title_tag else ""
                body = body_tag.get_text(strip=True) if body_tag else ""
                date_str = date_tag.get_text(strip=True) if date_tag else ""
                
                # استخراج الأماكن (georeferences)
                places = []
                if places_tag:
                    place_tags = places_tag.find_all('d')
                    places = [p.get_text(strip=True) for p in place_tags]
                
                # تحويل التاريخ
                doc_date = parse_reuters_date(date_str)
                
                # بناء المستند
                document = {
                    "title": title,
                    "content": body,
                    "date": doc_date,
                    "georeferences": places,
                    "temporal_expressions": extract_temporal_expressions(body),
                    "source_file": os.path.basename(file_path)
                }
                
                # إضافة فقط إذا كان هناك محتوى
                if title or body:
                    documents.append(document)
                    
            except Exception as e:
                print(f"Error parsing document in {file_path}: {str(e)}")
                continue
        
        return documents
        
    except Exception as e:
        print(f"Error reading file {file_path}: {str(e)}")
        return []


def parse_reuters_date(date_str):
    """
    تحويل تاريخ Reuters إلى صيغة ISO
    مثال: "26-FEB-1987 15:01:01.79"
    """
    try:
        # إزالة الأجزاء الزائدة من الثواني
        date_str = re.sub(r'\.\d+$', '', date_str)
        
        # محاولة التحويل
        dt = datetime.strptime(date_str, "%d-%b-%Y %H:%M:%S")
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except:
        # إذا فشل التحويل، إرجاع None
        return None


def extract_temporal_expressions(text):
    """
    استخراج التعابير الزمنية من النص
    """
    temporal_patterns = [
        r'\b\d{4}\b',  # سنوات
        r'\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}\b',
        r'\b\d{1,2}/\d{1,2}/\d{2,4}\b',  # تواريخ بصيغة mm/dd/yyyy
    ]
    
    expressions = []
    for pattern in temporal_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        expressions.extend(matches)
    
    return list(set(expressions))[:10]  # أول 10 تعابير فريدة


# ============= APIs للفهرسة =============

@app.route('/api/index/document', methods=['POST'])
def index_document():
    """
    فهرسة مستند واحد
    """
    try:
        document = request.json
        
        if USE_CUSTOM_TOOLS:
            response = indexer.index_document(document)
        else:
            response = client.index(
                index=INDEX_NAME,
                body=document,
                refresh=True
            )
        
        return jsonify({
            "success": True,
            "message": "Document indexed successfully",
            "document_id": response["_id"]
        }), 201
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route('/api/index/bulk', methods=['POST'])
def bulk_index():
    """
    فهرسة مجموعة من المستندات
    """
    try:
        data = request.json
        documents = data.get("documents", [])
        
        if USE_CUSTOM_TOOLS:
            result = indexer.bulk_index_documents(documents)
            return jsonify({
                "success": True,
                "indexed": result["success"],
                "failed": result["failed"],
                "total": result["total"]
            }), 201
        else:
            actions = [
                {
                    "_index": INDEX_NAME,
                    "_source": doc
                }
                for doc in documents
            ]
            
            success, failed = helpers.bulk(client, actions, raise_on_error=False)
            
            return jsonify({
                "success": True,
                "indexed": success,
                "failed": failed,
                "total": len(documents)
            }), 201
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route('/api/index/from-folder', methods=['POST'])
def index_from_folder():
    """
    قراءة وفهرسة جميع ملفات .sgm من مجلد database
    """
    try:
        if request.is_json:
            data = request.json
        else:
            data = {}
        
        folder_path = data.get("folder_path", "database")
        
        print(f"\n{'='*60}")
        print(f"Starting indexing from folder: {folder_path}")
        print(f"{'='*60}")
        
        if not os.path.exists(folder_path):
            return jsonify({
                "success": False,
                "error": f"Folder '{folder_path}' not found"
            }), 404
        
        if not os.path.isdir(folder_path):
            return jsonify({
                "success": False,
                "error": f"'{folder_path}' is not a directory"
            }), 400
        
        all_documents = []
        processed_files = []
        errors = []
        
        print(f"Scanning folder: {folder_path}")
        sgm_files = list(Path(folder_path).glob("*.sgm"))
        
        if not sgm_files:
            return jsonify({
                "success": False,
                "error": f"No .sgm files found in '{folder_path}' folder"
            }), 404
        
        print(f"Found {len(sgm_files)} .sgm files")
        
        for file_path in sgm_files:
            try:
                print(f"Processing: {file_path.name}")
                documents = parse_sgm_file(str(file_path))
                all_documents.extend(documents)
                processed_files.append(file_path.name)
                print(f"✓ {file_path.name}: Extracted {len(documents)} documents")
            except Exception as e:
                error_msg = f"{file_path.name}: {str(e)}"
                errors.append(error_msg)
                print(f"✗ {error_msg}")
        
        if all_documents:
            print(f"\nIndexing {len(all_documents)} documents...")
            
            if USE_CUSTOM_TOOLS:
                result = indexer.bulk_index_documents(all_documents)
                print(f"✓ Indexing completed: {result['success']} succeeded, {result['failed']} failed")
                
                return jsonify({
                    "success": True,
                    "message": f"Successfully indexed documents from {folder_path} folder",
                    "processed_files": len(processed_files),
                    "total_documents": len(all_documents),
                    "indexed": result["success"],
                    "failed": result["failed"],
                    "files": processed_files,
                    "errors": errors
                }), 201
            else:
                actions = [
                    {
                        "_index": INDEX_NAME,
                        "_source": doc
                    }
                    for doc in all_documents
                ]
                
                success, failed = helpers.bulk(client, actions, raise_on_error=False)
                print(f"✓ Indexing completed: {success} succeeded, {failed} failed")
                
                return jsonify({
                    "success": True,
                    "message": f"Successfully indexed documents from {folder_path} folder",
                    "processed_files": len(processed_files),
                    "total_documents": len(all_documents),
                    "indexed": success,
                    "failed": failed,
                    "files": processed_files,
                    "errors": errors
                }), 201
        else:
            return jsonify({
                "success": False,
                "error": "No documents extracted from .sgm files",
                "errors": errors
            }), 400
        
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"\n{'='*60}")
        print(f"ERROR occurred:")
        print(error_trace)
        print(f"{'='*60}\n")
        return jsonify({
            "success": False,
            "error": f"Unexpected error: {str(e)}",
            "trace": error_trace
        }), 500


# ============= APIs للبحث =============

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
                "results": [],
                "message": "Query too short (minimum 3 characters)"
            })
        
        if USE_CUSTOM_TOOLS:
            results = query_engine.autocomplete_search(query, size)
        else:
            search_query = {
                "size": size,
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
                "_source": ["title"]
            }
            
            response = client.search(index=INDEX_NAME, body=search_query)
            
            results = []
            for hit in response["hits"]["hits"]:
                results.append({
                    "id": hit["_id"],
                    "title": hit["_source"].get("title", ""),
                    "score": hit["_score"]
                })
        
        return jsonify({
            "success": True,
            "results": results
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route('/api/search/smart', methods=['POST'])
def smart_search():
    """
    البحث الذكي مع المعايير المكانية والزمانية
    """
    try:
        data = request.json
        query_text = data.get('query', '')
        temporal = data.get('temporal_expression')
        geo = data.get('georeference')
        size = data.get('size', 10)
        
        if USE_CUSTOM_TOOLS:
            results = query_engine.smart_search(query_text, temporal, geo, size)
            total = len(results)
        else:
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
                "size": size,
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
                results.append(result)
            
            total = response["hits"]["total"]["value"]
        
        return jsonify({
            "success": True,
            "results": results,
            "total": total
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


# ============= APIs للتحليلات =============

@app.route('/api/analytics/top-georeferences', methods=['GET'])
def top_georeferences():
    """
    الحصول على أكثر المواقع تكراراً
    """
    try:
        size = int(request.args.get('size', 10))
        
        if USE_CUSTOM_TOOLS:
            results = query_engine.get_top_georeferences(size)
        else:
            query = {
                "size": 0,
                "aggs": {
                    "top_georeferences": {
                        "terms": {
                            "field": "georeferences.keyword",
                            "size": size
                        }
                    }
                }
            }
            
            response = client.search(index=INDEX_NAME, body=query)
            
            results = []
            if "aggregations" in response:
                buckets = response["aggregations"]["top_georeferences"]["buckets"]
                results = [
                    {
                        "georeference": bucket["key"],
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


@app.route('/api/analytics/time-distribution', methods=['GET'])
def time_distribution():
    """
    توزيع المستندات عبر الزمن
    """
    try:
        interval = request.args.get('interval', '1d')
        
        if USE_CUSTOM_TOOLS:
            results = query_engine.get_documents_distribution_over_time(interval)
        else:
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
    لوحة تحكم شاملة بجميع التحليلات
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


# ============= APIs للإدارة =============

@app.route('/api/health', methods=['GET'])
def health_check():
    """
    فحص حالة النظام
    """
    try:
        info = client.info()
        return jsonify({
            "success": True,
            "opensearch": info
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
                "error": f"Index '{INDEX_NAME}' does not exist",
                "message": "Please run /api/index/from-folder first"
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
        import traceback
        print(f"\n{'='*60}")
        print(f"❌ Error in /api/index/stats:")
        print(f"Error: {str(e)}")
        print(f"{'='*60}")
        traceback.print_exc()
        
        return jsonify({
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }), 500


if __name__ == '__main__':
    print("="*60)
    print("Smart Document Retrieval System - Flask API")
    print("="*60)
    print(f"Reading .sgm files from: database folder")
    print(f"OpenSearch: {OPENSEARCH_HOST}:{OPENSEARCH_PORT}")
    print(f"Index: {INDEX_NAME}")
    print(f"Authentication: DISABLED")
    print(f"Custom tools: {USE_CUSTOM_TOOLS}")
    print("="*60)
    app.run(debug=True, host='0.0.0.0', port=5000)