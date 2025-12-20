from opensearchpy import OpenSearch

# opensearch connection 
client = OpenSearch(
    hosts=[{'host': 'localhost', 'port': 9200}],
    use_ssl=False
)


index_name = "smart_documents"

index_body = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "analysis": {
            "filter": {
                "edge_ngram_filter": { #for autocomplete
                    "type": "edge_ngram",
                    "min_gram": 3,
                    "max_gram": 20
                },
                "stop_filter": {# delete stop words
                    "type": "stop",
                    "stopwords": "_english_"
                },
                "length_filter": {# to delete words that have length less than 3 
                    "type": "length",
                    "min": 3
                },
                "stemmer_filter": { # to stem words
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
                    "char_filter": ["html_strip"]# to delete html tags from the code 
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
            "geopoint": { # هون عملناها ك رقم مش ك نص عشان لما المستخدم يطلب مكان معين بالبحث اذا ما لقينا اكزاكت ماتش نجبله اقرب نص من ناحية المكان عن طريق مقارنة الارقام
                # طيب اي مكان باخد اذا كان عنا اكتر من مكان بالنص الواحد؟ باخد اكتر مكان بتكرر بالنص 
                "type": "geo_point"
            },
            "temporal_expressions": {
                "type": "text",
                "fields": {
                    "keyword": {"type": "keyword"}
                }
            },
            "georeferences": { # الاماكن المذكورة بالنص و الفائدة منها عشان نعرف الاماكن الاكثر تكرار على مستوى الاندكس كامل
                "type": "text",
                "fields": {
                    "keyword": {"type": "keyword"}
                }
            },
            "extracted_dates": {
                "type": "date",
                "format": "yyyy-MM-dd'T'HH:mm:ss||yyyy-MM-dd||epoch_millis"
            },
            "geo_points": {
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