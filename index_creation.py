from opensearchpy import OpenSearch

# إعداد الاتصال بـ OpenSearch
client = OpenSearch(
    hosts=[{'host': 'localhost', 'port': 9200}],
    http_auth=('admin', 'YourPassword123!'),
    use_ssl=True,
    verify_certs=False,
    ssl_show_warn=False
)

# تعريف Index Settings و Mappings
index_name = "smart_documents"

index_body = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 1,
        "analysis": {
            "filter": {
                "edge_ngram_filter": {
                    "type": "edge_ngram",
                    "min_gram": 3,
                    "max_gram": 20
                },
                "stop_filter": {
                    "type": "stop",
                    "stopwords": "_english_"
                },
                "length_filter": {
                    "type": "length",
                    "min": 3
                },
                "stemmer_filter": {
                    "type": "stemmer",
                    "language": "english"
                }
            },
            "analyzer": {
                "autocomplete_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "edge_ngram_filter"
                    ]
                },
                "autocomplete_search_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase"]
                },
                "content_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "stop_filter",
                        "length_filter",
                        "stemmer_filter"
                    ],
                    "char_filter": ["html_strip"]
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "title": {
                "type": "text",
                "analyzer": "autocomplete_analyzer",
                "search_analyzer": "autocomplete_search_analyzer",
                "fields": {
                    "keyword": {
                        "type": "keyword"
                    }
                }
            },
            "content": {
                "type": "text",
                "analyzer": "content_analyzer",
                "fields": {
                    "keyword": {
                        "type": "keyword"
                    }
                }
            },
            "authors": {
                "type": "nested",
                "properties": {
                    "first_name": {
                        "type": "text",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                    },
                    "last_name": {
                        "type": "text",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                    },
                    "email": {
                        "type": "keyword"
                    }
                }
            },
            "date": {
                "type": "date",
                "format": "yyyy-MM-dd'T'HH:mm:ss||yyyy-MM-dd||epoch_millis"
            },
            "geopoint": {
                "type": "geo_point"
            },
            "temporal_expressions": {
                "type": "text",
                "fields": {
                    "keyword": {"type": "keyword"}
                }
            },
            "georeferences": {
                "type": "text",
                "fields": {
                    "keyword": {"type": "keyword"}
                }
            },
            "extracted_dates": {
                "type": "date",
                "format": "yyyy-MM-dd'T'HH:mm:ss||yyyy-MM-dd||epoch_millis"
            },
            "extracted_locations": {
                "type": "geo_point"
            }
        }
    }
}

# إنشاء الـ Index
if client.indices.exists(index=index_name):
    client.indices.delete(index=index_name)
    print(f"Deleted existing index: {index_name}")

response = client.indices.create(index=index_name, body=index_body)
print(f"Index created: {response}")