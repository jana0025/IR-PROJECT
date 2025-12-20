from opensearchpy import OpenSearch
from typing import List, Dict, Optional
from datetime import datetime
import dateparser
import json
import os


class DocumentIndexer:
    def __init__(self, client: OpenSearch, index_name: str, extractor):
        # Store OpenSearch client, index name, and entity extractor
        self.client = client
        self.index_name = index_name
        self.extractor = extractor

    def prepare_document(self, doc: Dict) -> Dict:
        """
        Prepare the document for indexing by extracting entities
        """
        # Read title and content
        content = doc.get("content", "")
        title = doc.get("title", "")

        # Combine title + content to extract entities from both
        full_text = f"{title} {content}"
        entities = self.extractor.extract_all_entities(full_text)

        # Parse doc date if it is a string
        doc_date = doc.get("date")
        if isinstance(doc_date, str):
            doc_date = dateparser.parse(doc_date)

        # Pick the best date if the original date is missing
        final_date = self.extractor.approximate_date(doc_date, entities["parsed_dates"])

        # Pick the best location if the original location is missing
        doc_location = doc.get("geopoint")
        final_location = self.extractor.approximate_location(doc_location, entities["coordinates"])

        # Build the final document structure
        prepared_doc = {
            "title": title,
            "content": content,
            "authors": doc.get("authors", []),
            "temporal_expressions": entities["temporal_expressions"],
            "georeferences": entities["georeferences"]
        }

        # Add date fields if a date exists
        if final_date:
            prepared_doc["date"] = final_date.isoformat()
            prepared_doc["extracted_dates"] = [d.isoformat() for d in entities["parsed_dates"]]

        # Add geopoint if a location exists
        if final_location:
            prepared_doc["geopoint"] = final_location

        # Add extracted location points if available
        if entities["coordinates"]:
            prepared_doc["extracted_locations"] = [
                {"lat": coord["lat"], "lon": coord["lon"]}
                for coord in entities["coordinates"]
            ]

        return prepared_doc

    def index_document(self, doc: Dict, doc_id: Optional[str] = None) -> Dict:
        """
        Index a single document
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
        Index a list of documents in bulk
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
        Index documents from a JSON file
        """
        with open(file_path, 'r', encoding='utf-8') as f:
            documents = json.load(f)

        if not isinstance(documents, list):
            documents = [documents]

        return self.bulk_index_documents(documents)

    def index_from_directory(self, directory_path: str, file_extension: str = ".json") -> Dict:
        """
        Index all JSON files inside a directory
        """
        all_documents = []

        for filename in os.listdir(directory_path):
            if filename.endswith(file_extension):
                file_path = os.path.join(directory_path, filename)
                with open(file_path, 'r', encoding='utf-8') as f:
                    doc = json.load(f)
                    all_documents.append(doc)

        return self.bulk_index_documents(all_documents)



