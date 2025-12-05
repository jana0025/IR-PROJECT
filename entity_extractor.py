import spacy
from geopy.geocoders import Nominatim
import dateparser
from datetime import datetime
import re
from typing import List, Dict, Tuple, Optional

class EntityExtractor:
    def __init__(self):
        # تحميل نموذج spaCy
        self.nlp = spacy.load("en_core_web_sm")
        
        # إعداد Geocoder
        self.geolocator = Nominatim(user_agent="smart_document_retrieval")
        
    def extract_temporal_expressions(self, text: str) -> Tuple[List[str], List[datetime]]:
        """
        استخراج التعبيرات الزمنية من النص
        Returns: (list of temporal expressions, list of parsed dates)
        """
        doc = self.nlp(text)
        temporal_expressions = []
        parsed_dates = []
        
        # استخراج التواريخ باستخدام spaCy
        for ent in doc.ents:
            if ent.label_ == "DATE":
                temporal_expressions.append(ent.text)
                # محاولة تحويل النص إلى تاريخ
                parsed_date = dateparser.parse(ent.text)
                if parsed_date:
                    parsed_dates.append(parsed_date)
        
        # البحث عن أنماط التواريخ الإضافية باستخدام regex
        date_patterns = [
            r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}',  # DD/MM/YYYY or MM/DD/YYYY
            r'\d{4}[/-]\d{1,2}[/-]\d{1,2}',    # YYYY-MM-DD
            r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2},? \d{4}\b',
            r'\b\d{1,2} (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{4}\b'
        ]
        
        for pattern in date_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                if match not in temporal_expressions:
                    temporal_expressions.append(match)
                    parsed_date = dateparser.parse(match)
                    if parsed_date:
                        parsed_dates.append(parsed_date)
        
        return temporal_expressions, parsed_dates
    
    def extract_georeferences(self, text: str) -> Tuple[List[str], List[Dict]]:
        """
        استخراج المواقع الجغرافية من النص
        Returns: (list of location names, list of coordinates)
        """
        doc = self.nlp(text)
        georeferences = []
        coordinates = []
        
        # استخراج الكيانات الجغرافية
        for ent in doc.ents:
            if ent.label_ in ["GPE", "LOC", "FAC"]:  # GPE: countries/cities, LOC: locations, FAC: facilities
                location_name = ent.text
                if location_name not in georeferences:
                    georeferences.append(location_name)
                    
                    # محاولة الحصول على الإحداثيات
                    try:
                        location = self.geolocator.geocode(location_name, timeout=5)
                        if location:
                            coordinates.append({
                                "name": location_name,
                                "lat": location.latitude,
                                "lon": location.longitude
                            })
                    except Exception as e:
                        print(f"Error geocoding {location_name}: {e}")
        
        return georeferences, coordinates
    
    def extract_all_entities(self, text: str) -> Dict:
        """
        استخراج جميع الكيانات من النص
        """
        temporal_expressions, parsed_dates = self.extract_temporal_expressions(text)
        georeferences, coordinates = self.extract_georeferences(text)
        
        return {
            "temporal_expressions": temporal_expressions,
            "parsed_dates": parsed_dates,
            "georeferences": georeferences,
            "coordinates": coordinates
        }
    
    def approximate_date(self, doc_date: Optional[datetime], parsed_dates: List[datetime]) -> Optional[datetime]:
        """
        تقريب التاريخ إذا لم يكن موجوداً
        """
        if doc_date:
            return doc_date
        if parsed_dates:
            # استخدام أحدث تاريخ مستخرج
            return max(parsed_dates)
        return None
    
    def approximate_location(self, doc_location: Optional[Dict], coordinates: List[Dict]) -> Optional[Dict]:
        """
        تقريب الموقع إذا لم يكن موجوداً
        """
        if doc_location:
            return doc_location
        if coordinates:
            # استخدام أول موقع مستخرج
            return {"lat": coordinates[0]["lat"], "lon": coordinates[0]["lon"]}
        return None


# مثال على الاستخدام
if __name__ == "__main__":
    extractor = EntityExtractor()
    
    sample_text = """
    On January 15, 2024, researchers in New York City published findings about 
    climate change. The study was conducted in Paris and London between 2020 and 2023.
    """
    
    entities = extractor.extract_all_entities(sample_text)
    print("Temporal Expressions:", entities["temporal_expressions"])
    print("Parsed Dates:", entities["parsed_dates"])
    print("Georeferences:", entities["georeferences"])
    print("Coordinates:", entities["coordinates"])