from opensearchpy import OpenSearch
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import dateparser

class SmartQueryEngine:
    def __init__(self, client: OpenSearch, index_name: str, extractor):
        self.client = client
        self.index_name = index_name
        self.extractor = extractor
    
    def autocomplete_search(self, prefix: str, size: int = 10) -> List[Dict]:
        """
        البحث التلقائي للعناوين (Autocomplete)
        يبدأ العمل بعد كتابة 3 أحرف
        """
        if len(prefix) < 3:
            return []
        
        query = {
            "size": size,
            "query": {
                "bool": {
                    "should": [
                        {
                            "match": {
                                "title": {
                                    "query": prefix,
                                    "fuzziness": "AUTO",
                                    "prefix_length": 2,
                                    "boost": 2
                                }
                            }
                        },
                        {
                            "match_phrase_prefix": {
                                "title": {
                                    "query": prefix,
                                    "boost": 3
                                }
                            }
                        }
                    ]
                }
            },
            "_source": ["title"]
        }
        
        response = self.client.search(index=self.index_name, body=query)
        
        results = []
        for hit in response["hits"]["hits"]:
            results.append({
                "id": hit["_id"],
                "title": hit["_source"]["title"],
                "score": hit["_score"]
            })
        
        return results
    
    def smart_search(self, 
                     query_text: str,
                     temporal_expression: Optional[str] = None,
                     georeference: Optional[str] = None,
                     size: int = 10) -> List[Dict]:
        """
        البحث الذكي مع الأخذ بالاعتبار:
        1. النص (مع تفضيل العنوان)
        2. التعبير الزمني
        3. الموقع الجغرافي
        4. الحداثة (Recency)
        5. الموقع (Localization)
        """
        must_clauses = []
        should_clauses = []
        filter_clauses = []
        
        # البحث النصي
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
        
        # البحث الزمني
        if temporal_expression:
            parsed_date = dateparser.parse(temporal_expression)
            if parsed_date:
                should_clauses.append({
                    "range": {
                        "date": {
                            "gte": parsed_date.isoformat(),
                            "boost": 1.5
                        }
                    }
                })
                should_clauses.append({
                    "match": {
                        "temporal_expressions": {
                            "query": temporal_expression,
                            "boost": 1.2
                        }
                    }
                })
        
        # البحث الجغرافي
        if georeference:
            should_clauses.append({
                "match": {
                    "georeferences": {
                        "query": georeference,
                        "boost": 1.5
                    }
                }
            })
            
            # محاولة الحصول على الإحداثيات
            try:
                from geopy.geocoders import Nominatim
                geolocator = Nominatim(user_agent="smart_query")
                location = geolocator.geocode(georeference, timeout=5)
                
                if location:
                    should_clauses.append({
                        "geo_distance": {
                            "distance": "100km",
                            "geopoint": {
                                "lat": location.latitude,
                                "lon": location.longitude
                            }
                        }
                    })
            except Exception as e:
                print(f"Geocoding error: {e}")
        
        # بناء الاستعلام النهائي
        query = {
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
        
        response = self.client.search(index=self.index_name, body=query)
        
        results = []
        for hit in response["hits"]["hits"]:
            result = hit["_source"]
            result["id"] = hit["_id"]
            result["score"] = hit["_score"]
            results.append(result)
        
        return results
    
    def get_top_georeferences(self, size: int = 10) -> List[Dict]:
        """
        الحصول على أكثر 10 مواقع جغرافية تكراراً
        """
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
        
        response = self.client.search(index=self.index_name, body=query)
        
        results = []
        if "aggregations" in response:
            buckets = response["aggregations"]["top_georeferences"]["buckets"]
            for bucket in buckets:
                results.append({
                    "georeference": bucket["key"],
                    "count": bucket["doc_count"]
                })
        
        return results
    
    def get_documents_distribution_over_time(self, interval: str = "1d") -> List[Dict]:
        """
        الحصول على توزيع المستندات عبر الزمن
        interval: 1d (يوم واحد), 1w (أسبوع), 1M (شهر)
        """
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
        
        response = self.client.search(index=self.index_name, body=query)
        
        results = []
        if "aggregations" in response:
            buckets = response["aggregations"]["documents_over_time"]["buckets"]
            for bucket in buckets:
                results.append({
                    "date": bucket["key_as_string"],
                    "count": bucket["doc_count"]
                })
        
        return results
    
    def advanced_analytics(self) -> Dict:
        """
        تحليلات متقدمة شاملة
        """
        return {
            "top_georeferences": self.get_top_georeferences(10),
            "documents_over_time": self.get_documents_distribution_over_time("1d"),
            "total_documents": self.get_total_documents()
        }
    
    def get_total_documents(self) -> int:
        """
        الحصول على إجمالي عدد المستندات
        """
        response = self.client.count(index=self.index_name)
        return response["count"]


# مثال على الاستخدام
if __name__ == "__main__":
    from entity_extractor import EntityExtractor
    
    client = OpenSearch(
        hosts=[{'host': 'localhost', 'port': 9200}],
        http_auth=('admin', 'YourPassword123!'),
        use_ssl=True,
        verify_certs=False,
        ssl_show_warn=False
    )
    
    extractor = EntityExtractor()
    query_engine = SmartQueryEngine(client, "smart_documents", extractor)
    
    # Autocomplete
    print("=== Autocomplete Results ===")
    results = query_engine.autocomplete_search("cli")
    for r in results:
        print(f"- {r['title']} (score: {r['score']})")
    
    # Smart Search
    print("\n=== Smart Search Results ===")
    results = query_engine.smart_search(
        query_text="climate research",
        temporal_expression="2024",
        georeference="New York"
    )
    for r in results:
        print(f"- {r['title']} (score: {r['score']})")
    
    # Top Georeferences
    print("\n=== Top Georeferences ===")
    results = query_engine.get_top_georeferences()
    for r in results:
        print(f"- {r['georeference']}: {r['count']} documents")
    
    # Documents Distribution
    print("\n=== Documents Over Time ===")
    results = query_engine.get_documents_distribution_over_time()
    for r in results:
        print(f"- {r['date']}: {r['count']} documents")