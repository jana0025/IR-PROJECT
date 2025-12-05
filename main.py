from flask import Flask, request, jsonify
from flask_cors import CORS
from opensearchpy import OpenSearch
import os
from datetime import datetime


from entity_extractor import EntityExtractor
from document_indexer import DocumentIndexer
from query_engine import SmartQueryEngine

app = Flask(__name__)
CORS(app)

# إعداد OpenSearch
OPENSEARCH_HOST = os.getenv('OPENSEARCH_HOST', 'localhost')
OPENSEARCH_PORT = int(os.getenv('OPENSEARCH_PORT', 9200))
OPENSEARCH_USER = os.getenv('OPENSEARCH_USER', 'admin')
OPENSEARCH_PASSWORD = os.getenv('OPENSEARCH_PASSWORD', 'YourPassword123!')

client = OpenSearch(
    hosts=[{'host': OPENSEARCH_HOST, 'port': OPENSEARCH_PORT}],
    http_auth=(OPENSEARCH_USER, OPENSEARCH_PASSWORD),
    use_ssl=True,
    verify_certs=False,
    ssl_show_warn=False
)


extractor = EntityExtractor()
indexer = DocumentIndexer(client, "smart_documents", extractor)
query_engine = SmartQueryEngine(client, "smart_documents", extractor)

INDEX_NAME = "smart_documents"


# ============= APIs للفهرسة =============

@app.route('/api/index/document', methods=['POST'])
def index_document():
    """
    فهرسة مستند واحد
    Body: JSON document
    """
    try:
        document = request.json
        
        response = indexer.index_document(document)
        
        # للتجربة بدون الأدوات المخصصة:
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
    Body: {"documents": [...]}
    """
    try:
        data = request.json
        documents = data.get("documents", [])
        
        response = indexer.bulk_index_documents(documents)
        
        # للتجربة بدون الأدوات المخصصة:
        from opensearchpy import helpers
        
        actions = [
            {
                "_index": INDEX_NAME,
                "_source": doc
            }
            for doc in documents
        ]
        
        success, failed = helpers.bulk(client, actions)
        
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


# ============= APIs للبحث =============

@app.route('/api/search/autocomplete', methods=['GET'])
def autocomplete():
    """
    البحث التلقائي
    Parameters: ?q=search_term&size=10
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
        
        results = query_engine.autocomplete_search(query, size)
        
        # استعلام مباشر:
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
                "title": hit["_source"]["title"],
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
    Body: {
        "query": "text query",
        "temporal_expression": "2024",
        "georeference": "New York",
        "size": 10
    }
    """
    try:
        data = request.json
        query_text = data.get('query', '')
        temporal = data.get('temporal_expression')
        geo = data.get('georeference')
        size = data.get('size', 10)
        
        results = query_engine.smart_search(query_text, temporal, geo, size)
        
        # بناء الاستعلام
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
        
        return jsonify({
            "success": True,
            "results": results,
            "total": response["hits"]["total"]["value"]
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
    Parameters: ?size=10
    """
    try:
        size = int(request.args.get('size', 10))
        
        results = query_engine.get_top_georeferences(size)
        
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
    Parameters: ?interval=1d (1d, 1w, 1M)
    """
    try:
        interval = request.args.get('interval', '1d')
        
        results = query_engine.get_documents_distribution_over_time(interval)
        
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
        analytics = query_engine.advanced_analytics()
        
        # الحصول على إجمالي المستندات
        count_response = client.count(index=INDEX_NAME)
        total_docs = count_response["count"]
        
        # الحصول على أكثر المواقع
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
        
        # توزيع المستندات
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
        stats = client.indices.stats(index=INDEX_NAME)
        count = client.count(index=INDEX_NAME)
        
        return jsonify({
            "success": True,
            "document_count": count["count"],
            "index_size": stats["indices"][INDEX_NAME]["total"]["store"]["size_in_bytes"]
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)