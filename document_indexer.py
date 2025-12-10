from opensearchpy import OpenSearch
from typing import List, Dict, Optional
from datetime import datetime
import dateparser  # ⬅️ إضافة الاستيراد المفقود
import json
import os

class DocumentIndexer:
    def __init__(self, client: OpenSearch, index_name: str, extractor):
        self.client = client
        self.index_name = index_name
        self.extractor = extractor
    
    def prepare_document(self, doc: Dict) -> Dict:
        """
        تحضير المستند للفهرسة مع استخراج الكيانات
        """
        # استخراج المحتوى
        content = doc.get("content", "")
        title = doc.get("title", "")
        
        # استخراج الكيانات من المحتوى والعنوان
        full_text = f"{title} {content}"
        entities = self.extractor.extract_all_entities(full_text)
        
        # تحضير التاريخ
        doc_date = doc.get("date")
        if isinstance(doc_date, str):
            doc_date = dateparser.parse(doc_date)
        
        final_date = self.extractor.approximate_date(doc_date, entities["parsed_dates"])
        
        # تحضير الموقع
        doc_location = doc.get("geopoint")
        final_location = self.extractor.approximate_location(doc_location, entities["coordinates"])
        
        # بناء المستند النهائي
        prepared_doc = {
            "title": title,
            "content": content,
            "authors": doc.get("authors", []),
            "temporal_expressions": entities["temporal_expressions"],
            "georeferences": entities["georeferences"]
        }
        
        # إضافة التاريخ إذا كان موجوداً
        if final_date:
            prepared_doc["date"] = final_date.isoformat()
            prepared_doc["extracted_dates"] = [d.isoformat() for d in entities["parsed_dates"]]
        
        # إضافة الموقع إذا كان موجوداً
        if final_location:
            prepared_doc["geopoint"] = final_location
        
        # إضافة المواقع المستخرجة
        if entities["coordinates"]:
            prepared_doc["extracted_locations"] = [
                {"lat": coord["lat"], "lon": coord["lon"]} 
                for coord in entities["coordinates"]
            ]
        
        return prepared_doc
    
    def index_document(self, doc: Dict, doc_id: Optional[str] = None) -> Dict:
        """
        فهرسة مستند واحد
        """
        prepared_doc = self.prepare_document(doc)
        
        response = self.client.index(
            index=self.index_name,
            body=prepared_doc,
            id=doc_id,
            refresh=True
        )
        
        return response
    
    def bulk_index_documents(self, documents: List[Dict]) -> Dict:
        """
        فهرسة مجموعة من المستندات دفعة واحدة
        """
        from opensearchpy import helpers
        
        actions = []
        for i, doc in enumerate(documents):
            prepared_doc = self.prepare_document(doc)
            action = {
                "_index": self.index_name,
                "_id": doc.get("id", i),
                "_source": prepared_doc
            }
            actions.append(action)
        
        success, failed = helpers.bulk(self.client, actions, raise_on_error=False)
        
        return {
            "success": success,
            "failed": failed,
            "total": len(documents)
        }
    
    def index_from_json_file(self, file_path: str) -> Dict:
        """
        فهرسة المستندات من ملف JSON
        """
        with open(file_path, 'r', encoding='utf-8') as f:
            documents = json.load(f)
        
        if not isinstance(documents, list):
            documents = [documents]
        
        return self.bulk_index_documents(documents)
    
    def index_from_directory(self, directory_path: str, file_extension: str = ".json") -> Dict:
        """
        فهرسة جميع الملفات في مجلد
        """
        all_documents = []
        
        for filename in os.listdir(directory_path):
            if filename.endswith(file_extension):
                file_path = os.path.join(directory_path, filename)
                with open(file_path, 'r', encoding='utf-8') as f:
                    doc = json.load(f)
                    all_documents.append(doc)
        
        return self.bulk_index_documents(all_documents)


# مثال على الاستخدام
if __name__ == "__main__":
    # إعداد الاتصال
    client = OpenSearch(
        hosts=[{'host': 'localhost', 'port': 9200}],
        http_auth=('admin', 'YourPassword123!'),
        use_ssl=True,
        verify_certs=False,
        ssl_show_warn=False
    )
    
    # إنشاء المفهرس
    from entity_extractor import EntityExtractor
    extractor = EntityExtractor()
    indexer = DocumentIndexer(client, "smart_documents", extractor)
    
    # مثال على مستند
    sample_doc = {
        "title": "Climate Change Research in Major Cities",
        "content": """
            On January 15, 2024, researchers in New York City published findings about 
            climate change. The study was conducted in Paris and London between 2020 and 2023.
            The results show significant increases across all monitored locations.
        """,
        "authors": [
            {
                "first_name": "John",
                "last_name": "Doe",
                "email": "john.doe@example.com"
            },
            {
                "first_name": "Jane",
                "last_name": "Smith",
                "email": "jane.smith@example.com"
            }
        ],
        "date": "2024-01-15T10:00:00"
    }
    
    # فهرسة المستند
    response = indexer.index_document(sample_doc, doc_id="doc1")
    print("Indexing response:", response)